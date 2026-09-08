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
