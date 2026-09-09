package sampling

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

// EmitFunc receives a decided trace's spans plus the decision, and reports
// whether the resulting write (if any) is durably confirmed. Sample
// decisions have SamplingWeight already applied to every span before this is
// called; Drop decisions are passed for observability/metrics purposes only.
//
// The returned error gates the offset watermark: finishTrace (and therefore
// resolving this trace's watermark entry) only runs on nil. A non-nil error
// leaves the partition's commit floor held at this trace's offset -- Kafka
// never advances past it, so redelivery retries the entire decide-and-write
// sequence from scratch. That retry is safe specifically because
// probabilistic sampling is deterministic by trace_id: a redelivered trace
// gets the identical verdict, not a second, inconsistent one.
type EmitFunc func(spans []storage.SpanRow, decision Decision) error

// LateAttachFunc receives one span that arrived after its trace's decision
// had already fired and sampled. SamplingWeight is already applied. Same
// durability-gating contract as EmitFunc: the watermark for this span's
// offset only resolves once the returned error is nil.
type LateAttachFunc func(span storage.SpanRow) error

// Assembler ties the Buffer, the hot-reloadable PolicyChain, and the
// per-partition OffsetWatermark together, and drives the fixed-wait +
// root-closure-early-exit completion heuristic.
type Assembler struct {
	buffer *Buffer
	wm     *OffsetWatermark
	chain  atomic.Pointer[PolicyChain]
	m      *observability.Metrics
	log    *slog.Logger

	emit       EmitFunc
	lateAttach LateAttachFunc

	// partitionOf remembers which partition each in-flight trace was first
	// seen on, purely to resolve the watermark correctly once that trace is
	// later forced, popped, or early-exited -- kept here rather than in
	// Buffer so Buffer stays free of any Kafka-specific concept.
	mu          sync.Mutex
	partitionOf map[[16]byte]int32

	// forceDecide is bound once in NewAssembler, not allocated per Ingest
	// call -- see the constructor's comment.
	forceDecide func([]storage.SpanRow) Decision
}

// NewAssembler builds an assembler. emit is called for every decided trace
// (sampled or dropped); lateAttach is called for every late span belonging
// to an already-sampled trace.
func NewAssembler(buffer *Buffer, chain *PolicyChain, m *observability.Metrics, log *slog.Logger, emit EmitFunc, lateAttach LateAttachFunc) *Assembler {
	a := &Assembler{
		buffer:      buffer,
		wm:          NewOffsetWatermark(),
		m:           m,
		log:         log,
		emit:        emit,
		lateAttach:  lateAttach,
		partitionOf: make(map[[16]byte]int32),
	}
	a.chain.Store(chain)
	// Bound once at construction, not per call. MEASURED: forceDecideFn()
	// was previously called fresh inside every Ingest(), allocating a new
	// closure per span regardless of whether eviction ever triggers --
	// confirmed as one of BenchmarkAssemblerIngest's 3 allocs/op. The
	// closure itself is stateless (only closes over `a`), so one instance
	// serves every call for this assembler's lifetime.
	a.forceDecide = a.doForceDecide
	return a
}

// SetChain hot-swaps the active policy chain. Safe to call concurrently with
// Ingest/decision processing.
func (a *Assembler) SetChain(chain *PolicyChain) { a.chain.Store(chain) }

// SafeCommitOffset exposes the per-partition watermark for the Kafka
// consumer to gate commits on. hasFloor=false means nothing on this
// partition is held back by this mechanism.
func (a *Assembler) SafeCommitOffset(partition int32) (offset int64, hasFloor bool) {
	return a.wm.SafeOffset(partition)
}

// Ingest processes one decoded span. partition/offset identify where it came
// from on the Kafka spans topic, for offset-watermark tracking.
func (a *Assembler) Ingest(s storage.SpanRow, partition int32, offset int64) {
	var id [16]byte
	copy(id[:], s.TraceID)

	a.mu.Lock()
	if _, tracked := a.partitionOf[id]; !tracked {
		a.partitionOf[id] = partition
	}
	a.mu.Unlock()

	a.wm.Track(partition, offset, id)

	result, weight := a.buffer.Ingest(s, a.forceDecide)

	switch result {
	case IngestLateAttached:
		attached := s
		attached.SamplingWeight = weight
		if err := a.lateAttach(attached); err != nil {
			a.log.Error("late-attach write failed, withholding this offset for redelivery",
				slog.Int64("offset", offset), slog.String("error", err.Error()))
			return // watermark stays held; Kafka redelivers, retry is safe (deterministic decisions)
		}
		a.finishTrace(id)
	case IngestLateDropped:
		// Nothing is written for a drop, so there is nothing to durably
		// confirm -- safe to resolve immediately.
		a.finishTrace(id)
	case IngestBuffered:
		if spans, ok := a.buffer.TryEarlyExit(id); ok {
			a.decideAndEmit(id, spans)
		}
	}
}

// doForceDecide is what Buffer invokes (via the a.forceDecide field bound
// once in NewAssembler) when capacity eviction under EvictionForcedDecision
// needs a decision on the oldest trace right now.
func (a *Assembler) doForceDecide(spans []storage.SpanRow) Decision {
	d := a.decide(spans)
	err := a.applyWeightAndEmit(spans, d)
	if len(spans) > 0 {
		var id [16]byte
		copy(id[:], spans[0].TraceID)
		if err != nil {
			a.log.Error("forced-decision write failed, withholding offset for redelivery",
				slog.String("error", err.Error()))
		} else {
			a.finishTrace(id)
		}
	}
	return d
}

// RunSweep drives the fixed-wait completion heuristic: every tick, decide
// every trace whose DecisionWait has elapsed.
func (a *Assembler) RunSweep(stop <-chan struct{}, tick time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			for _, t := range a.buffer.PopReady(now) {
				a.decideAndEmit(t.traceID, t.spans)
			}
		}
	}
}

// decideAndEmit runs the policy chain, applies the resulting weight, emits,
// records the decision for late-span classification, and cleans up
// assembler-side bookkeeping. Used by both the ordinary sweep and the
// root-closure early exit -- the two ordinary (non-forced) completion paths.
//
// On a write failure, decideAndEmit deliberately records NEITHER the
// decision NOR resolves the watermark. The trace has already left the
// buffer by this point, so a redelivered span for it (once Kafka retries,
// since the offset stays held) finds nothing in-flight and nothing decided
// -- it restarts fresh assembly from scratch, which is exactly the "retry
// the whole sequence" guarantee documented on EmitFunc, and is only safe
// because probabilistic sampling is deterministic by trace_id.
func (a *Assembler) decideAndEmit(id [16]byte, spans []storage.SpanRow) {
	d := a.decide(spans)
	if err := a.applyWeightAndEmit(spans, d); err != nil {
		a.log.Error("decision write failed, withholding offset for redelivery",
			slog.String("error", err.Error()))
		return
	}
	a.buffer.RecordDecision(id, d)
	a.finishTrace(id)
}

func (a *Assembler) applyWeightAndEmit(spans []storage.SpanRow, d Decision) error {
	if d.Verdict == VerdictSample {
		weight := d.Weight()
		for i := range spans {
			spans[i].SamplingWeight = weight
		}
	}
	return a.emit(spans, d)
}

func (a *Assembler) decide(spans []storage.SpanRow) Decision {
	var id [16]byte
	if len(spans) > 0 {
		copy(id[:], spans[0].TraceID)
	}

	start := time.Now()
	if len(spans) > 0 {
		start = spans[0].Timestamp
	}

	view := TraceView{
		TraceID:  id,
		Spans:    spans,
		Tree:     BuildTree(spans),
		Duration: TraceDuration(spans),
	}
	d := a.chain.Load().Decide(view)

	a.m.DecisionsTotal.WithLabelValues(d.Verdict.String(), d.PolicyName).Inc()
	a.m.DecisionLatency.Observe(time.Since(start).Seconds())
	if d.Verdict == VerdictSample {
		a.m.SamplingWeight.Observe(d.Weight())
	}
	return d
}

// HandleDataLoss releases the commit floor for any watermark entries on
// partition pointing into a range Kafka has proven permanently unreachable
// (see pipeline.DataLossFunc, wired as the consumer's onDataLoss callback).
// This is the precise fix for a real, reproduced bug (docs/DECISIONS.md
// Phase 4 Sec 8): a trace whose decide-and-write failed leaves its
// watermark entry held, waiting for Kafka to redeliver the same message for
// a retry -- but if the broker itself loses the underlying data (observed:
// a Redpanda restart mid-session, every partition reset to offset 0), that
// redelivery can never happen, and the floor would otherwise stay stuck
// forever. Kafka's own ErrDataLoss tells us exactly which offsets are gone,
// so this fires immediately and only for offsets proven lost -- it cannot
// release a floor under a trace that is still genuinely being processed.
func (a *Assembler) HandleDataLoss(partition int32, resetTo, consumedTo int64) {
	cleared := a.wm.ClearBelow(partition, consumedTo)
	if len(cleared) == 0 {
		return
	}

	a.mu.Lock()
	for _, id := range cleared {
		delete(a.partitionOf, id)
	}
	a.mu.Unlock()

	a.m.OffsetWatermarkDataLossTotal.Add(float64(len(cleared)))
	a.log.Error("kafka reported unrecoverable data loss; released watermark entries pointing into the lost range -- these traces' remaining spans are gone and were never written",
		slog.Int("partition", int(partition)),
		slog.Int64("reset_to", resetTo),
		slog.Int64("consumed_to", consumedTo),
		slog.Int("watermark_entries_cleared", len(cleared)))
}

// RunWatermarkWatchdog is the safety net for the OTHER way a watermark
// entry can get stuck: a write that fails permanently for a reason
// unrelated to broker data loss (a "poison" row ClickHouse always rejects)
// never triggers ErrDataLoss at all, since the data is still sitting in
// Kafka, technically retriable -- it just never succeeds. Buffer guarantees
// every trace resolves within DecisionWait (or immediately on capacity
// eviction), so an entry still held after maxAge (which must be
// comfortably larger than DecisionWait) can only be write-failed limbo,
// never genuinely still-buffered work. Buffer.Tracks is still checked
// before giving up, defending specifically against the one dangerous
// case this reasoning depends on holding: a redelivered retry keeps its
// ORIGINAL trackedAt (Track is first-offset-wins), so an active retry
// that happens to be old would otherwise look identical to an abandoned
// one by age alone.
func (a *Assembler) RunWatermarkWatchdog(stop <-chan struct{}, tick, maxAge time.Duration) {
	ticker := time.NewTicker(tick)
	defer ticker.Stop()

	for {
		select {
		case <-stop:
			return
		case now := <-ticker.C:
			a.checkWatermarkOnce(now, maxAge)
		}
	}
}

// checkWatermarkOnce is RunWatermarkWatchdog's per-tick logic, pulled out
// so a test can drive it directly against an explicit `now` rather than
// waiting on a real ticker.
func (a *Assembler) checkWatermarkOnce(now time.Time, maxAge time.Duration) {
	for _, e := range a.wm.StaleCandidates(maxAge, now) {
		if a.buffer.Tracks(e.TraceID) {
			continue // still legitimately known; leave it for normal completion
		}
		a.wm.Resolve(e.Partition, e.TraceID)
		a.mu.Lock()
		delete(a.partitionOf, e.TraceID)
		a.mu.Unlock()

		a.m.OffsetWatermarkExpiredTotal.Inc()
		a.log.Error("offset watermark entry expired: held far longer than DecisionWait and no longer tracked anywhere; giving up and releasing its commit floor",
			slog.Int("partition", int(e.Partition)),
			slog.Int64("offset", e.Offset),
			slog.Duration("held_for", e.Age))
	}
}

// finishTrace releases assembler-side bookkeeping for a trace that just
// decided (normally, early-exited, or was forced), resolving the offset
// watermark on its partition.
func (a *Assembler) finishTrace(id [16]byte) {
	a.mu.Lock()
	partition, ok := a.partitionOf[id]
	delete(a.partitionOf, id)
	a.mu.Unlock()

	if ok {
		a.wm.Resolve(partition, id)
	}
}
