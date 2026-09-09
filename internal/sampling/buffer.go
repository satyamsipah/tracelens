package sampling

import (
	"container/heap"
	"sync"
	"time"

	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

// EvictionPolicy is what happens to the oldest in-flight trace when the
// buffer's hard cap is reached.
type EvictionPolicy string

const (
	// EvictionForcedDecision runs the normal decision pipeline on the oldest
	// trace right now, using whatever spans arrived so far, and flushes it.
	// Nothing that entered the buffer is ever discarded outright.
	EvictionForcedDecision EvictionPolicy = "forced_decision"

	// EvictionDiscard drops the oldest trace's buffered spans outright,
	// counted. A genuine, permanent loss of data that already arrived.
	EvictionDiscard EvictionPolicy = "discard"
)

// BufferConfig bounds the in-flight trace buffer.
type BufferConfig struct {
	MaxTraces int
	MaxBytes  int64

	Eviction EvictionPolicy

	// DecisionWait is the fixed-wait completion heuristic's window from a
	// trace's first-seen span.
	DecisionWait time.Duration

	// DecidedCacheSize/TTL bound the separate late-span lookup cache. Kept
	// deliberately small relative to MaxTraces: it only needs to answer "was
	// this trace recently decided", not hold full span data.
	DecidedCacheSize int
	DecidedCacheTTL  time.Duration
}

// bufferedTrace is one in-flight trace's accumulated state.
type bufferedTrace struct {
	traceID   [16]byte
	spans     []storage.SpanRow
	firstSeen time.Time
	sizeBytes int64
	heapIndex int // maintained by container/heap; do not set directly
}

// decidedEntry is what the late-span cache remembers about a trace whose
// decision already fired.
type decidedEntry struct {
	sampled   bool
	weight    float64
	decidedAt time.Time
}

// Buffer holds every trace currently being assembled, bounded by count and
// total bytes, plus a small decided-cache for classifying late spans.
type Buffer struct {
	mu sync.Mutex

	cfg BufferConfig
	m   *observability.Metrics

	inflight map[[16]byte]*bufferedTrace
	// byAge is a min-heap over inflight, ordered by firstSeen. It serves
	// BOTH the completion sweep ("which traces have now exceeded
	// DecisionWait") and capacity eviction ("which trace is oldest") --
	// deliberately one structure for both, since "oldest" is the same
	// question either way, only the trigger differs (time vs. capacity).
	byAge ageHeap

	totalBytes int64

	decided      map[[16]byte]decidedEntry
	decidedOrder []decidedKey // FIFO for bounded eviction by count/age
}

type decidedKey struct {
	id [16]byte
	at time.Time
}

// NewBuffer builds an empty buffer.
func NewBuffer(cfg BufferConfig, m *observability.Metrics) *Buffer {
	if cfg.Eviction == "" {
		cfg.Eviction = EvictionForcedDecision
	}
	b := &Buffer{
		cfg:      cfg,
		m:        m,
		inflight: make(map[[16]byte]*bufferedTrace),
		decided:  make(map[[16]byte]decidedEntry),
	}
	heap.Init(&b.byAge)
	return b
}

// IngestResult reports what happened to one ingested span.
type IngestResult int

const (
	// IngestBuffered means the span joined (or started) an in-flight trace.
	IngestBuffered IngestResult = iota
	// IngestLateAttached means the span's trace already decided Sample; the
	// span should be written directly to storage with the trace's weight.
	IngestLateAttached
	// IngestLateDropped means the span's trace already decided Drop.
	IngestLateDropped
)

// spanSize is a rough per-span byte estimate for the buffer's byte cap: the
// fixed-size fields plus the two attribute maps' approximate encoded size.
// It does not need to be exact -- only good enough that the byte cap tracks
// reality within a small constant factor, which is what "hard cap" needs.
func spanSize(s *storage.SpanRow) int64 {
	size := int64(len(s.TraceID) + len(s.SpanID) + len(s.ParentSpanID) +
		len(s.ServiceName) + len(s.SpanName) + len(s.SpanKind) + len(s.StatusCode) + len(s.StatusMessage) + 64)
	for k, v := range s.ResourceAttributes {
		size += int64(len(k) + len(v))
	}
	for k, v := range s.SpanAttributes {
		size += int64(len(k) + len(v))
	}
	return size
}

// Ingest adds one span to its trace's buffer, or classifies it as late.
//
// forceDecide is nil unless capacity eviction under EvictionForcedDecision
// requires deciding another trace to make room -- the caller supplies the
// decision function (buffer has no policy-chain dependency of its own, to
// keep it testable in isolation) and Ingest invokes it on whichever trace
// was evicted, exactly as it would for a normal completion.
func (b *Buffer) Ingest(s storage.SpanRow, forceDecide func(spans []storage.SpanRow) Decision) (IngestResult, float64) {
	var id [16]byte
	copy(id[:], s.TraceID)

	b.mu.Lock()

	if entry, ok := b.decided[id]; ok {
		b.mu.Unlock()
		if entry.sampled {
			b.m.LateSpansAttached.Inc()
			return IngestLateAttached, entry.weight
		}
		b.m.LateSpansDropped.Inc()
		return IngestLateDropped, 0
	}

	t, exists := b.inflight[id]
	if !exists {
		t = &bufferedTrace{traceID: id, firstSeen: time.Now()}
		b.inflight[id] = t
		heap.Push(&b.byAge, t)
	}

	size := spanSize(&s)
	t.spans = append(t.spans, s)
	t.sizeBytes += size
	b.totalBytes += size

	b.updateGauges()

	// Capacity check happens AFTER admitting the span that triggered it: the
	// span that pushed the buffer over the line is itself already part of
	// whichever trace gets evicted/forced, so there is nowhere else for it
	// to go, and it must not be silently discarded from being counted.
	//
	// The oldest trace (byAge[0]) is the target regardless of whether it
	// happens to be the trace this very call just touched -- that only
	// happens when a single trace's own bytes exceed MaxBytes on its own
	// (buffer otherwise has room), in which case there is no other victim to
	// pick and it must still be forced to free room.
	needsEviction := len(b.inflight) > b.cfg.MaxTraces || b.totalBytes > b.cfg.MaxBytes
	var evictedID [16]byte
	shouldEvict := false
	if needsEviction && b.byAge.Len() > 0 {
		evictedID = b.byAge[0].traceID
		shouldEvict = true
	}
	b.mu.Unlock()

	if shouldEvict {
		b.resolve(evictedID, forceDecide)
	}
	return IngestBuffered, 0
}

func (b *Buffer) updateGauges() {
	b.m.InflightTraces.Set(float64(len(b.inflight)))
	b.m.InflightBytes.Set(float64(b.totalBytes))
}

// resolve forces a decision on traceID -- via forceDecide under
// EvictionForcedDecision, or a plain discard under EvictionDiscard -- removes
// it from the in-flight set, and records it in the decided-cache.
func (b *Buffer) resolve(traceID [16]byte, forceDecide func([]storage.SpanRow) Decision) {
	b.mu.Lock()
	t, ok := b.inflight[traceID]
	if !ok {
		b.mu.Unlock()
		return
	}
	delete(b.inflight, traceID)
	heap.Remove(&b.byAge, t.heapIndex)
	b.totalBytes -= t.sizeBytes
	b.updateGauges()
	b.mu.Unlock()

	if b.cfg.Eviction == EvictionDiscard {
		b.m.EvictedTracesTotal.Inc()
		return
	}

	b.m.ForcedDecisionsTotal.Inc()
	decision := forceDecide(t.spans)
	b.recordDecision(traceID, decision)
}

// PopReady removes and returns every trace whose DecisionWait has elapsed,
// for the completion sweep to decide normally (not a forced/capacity
// decision -- this is the ordinary, expected completion path).
func (b *Buffer) PopReady(now time.Time) []*bufferedTrace {
	b.mu.Lock()
	defer b.mu.Unlock()

	var ready []*bufferedTrace
	for b.byAge.Len() > 0 && now.Sub(b.byAge[0].firstSeen) >= b.cfg.DecisionWait {
		t := heap.Pop(&b.byAge).(*bufferedTrace)
		delete(b.inflight, t.traceID)
		b.totalBytes -= t.sizeBytes
		ready = append(ready, t)
	}
	b.updateGauges()
	return ready
}

// TryEarlyExit checks a single trace against the root-closure early-exit
// condition: the root span is present and every buffered span already fits
// inside [root.start, root.end]. Returns the trace's spans and true if so,
// removing it from the buffer -- an early, ordinary completion, not forced.
func (b *Buffer) TryEarlyExit(traceID [16]byte) ([]storage.SpanRow, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	t, ok := b.inflight[traceID]
	if !ok {
		return nil, false
	}
	if !rootClosureComplete(t.spans) {
		return nil, false
	}

	delete(b.inflight, t.traceID)
	heap.Remove(&b.byAge, t.heapIndex)
	b.totalBytes -= t.sizeBytes
	b.updateGauges()
	return t.spans, true
}

// rootClosureComplete implements the early-exit condition: a root span
// (parent_span_id all-zero) is present, and every span's interval falls
// inside [root.start, root.end].
func rootClosureComplete(spans []storage.SpanRow) bool {
	var root *storage.SpanRow
	for i := range spans {
		var parentID [8]byte
		copy(parentID[:], spans[i].ParentSpanID)
		if parentID == zeroSpanID {
			root = &spans[i]
			break
		}
	}
	if root == nil {
		return false
	}
	rootEnd := root.Timestamp.Add(time.Duration(root.DurationNS))

	for i := range spans {
		end := spans[i].Timestamp.Add(time.Duration(spans[i].DurationNS))
		if spans[i].Timestamp.Before(root.Timestamp) || end.After(rootEnd) {
			return false
		}
	}
	return true
}

// recordDecision stores a trace's outcome in the bounded decided-cache, for
// classifying late spans.
func (b *Buffer) recordDecision(traceID [16]byte, d Decision) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.decided[traceID] = decidedEntry{
		sampled:   d.Verdict == VerdictSample,
		weight:    d.Weight(),
		decidedAt: time.Now(),
	}
	b.decidedOrder = append(b.decidedOrder, decidedKey{id: traceID, at: time.Now()})
	b.evictDecidedLocked()
}

// evictDecidedLocked drops the oldest decided-cache entries past its
// count/age bound. Caller must hold b.mu.
func (b *Buffer) evictDecidedLocked() {
	now := time.Now()
	for len(b.decidedOrder) > 0 {
		oldest := b.decidedOrder[0]
		expired := b.cfg.DecidedCacheTTL > 0 && now.Sub(oldest.at) > b.cfg.DecidedCacheTTL
		overCap := b.cfg.DecidedCacheSize > 0 && len(b.decidedOrder) > b.cfg.DecidedCacheSize
		if !expired && !overCap {
			break
		}
		delete(b.decided, oldest.id)
		b.decidedOrder = b.decidedOrder[1:]
	}
}

// RecordDecision is the exported form of recordDecision, used by the normal
// (non-forced) completion path once the assembler has run the policy chain.
func (b *Buffer) RecordDecision(traceID [16]byte, d Decision) { b.recordDecision(traceID, d) }

// Stats exposes current buffer occupancy, for tests and diagnostics.
func (b *Buffer) Stats() (traces int, bytes int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.inflight), b.totalBytes
}

// Tracks reports whether id is currently known to the buffer -- either
// still in-flight or present in the decided cache. Used by the offset-
// watermark watchdog to confirm a stale-looking held offset isn't actually
// a trace still being legitimately processed (or one that decided and is
// only waiting out its decided-cache TTL) before concluding it is
// genuinely orphaned and safe to give up on.
func (b *Buffer) Tracks(id [16]byte) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.inflight[id]; ok {
		return true
	}
	_, ok := b.decided[id]
	return ok
}

// ageHeap is a container/heap.Interface over *bufferedTrace, ordered by
// firstSeen ascending -- oldest at index 0.
type ageHeap []*bufferedTrace

func (h ageHeap) Len() int           { return len(h) }
func (h ageHeap) Less(i, j int) bool { return h[i].firstSeen.Before(h[j].firstSeen) }
func (h ageHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}
func (h *ageHeap) Push(x any) {
	t := x.(*bufferedTrace)
	t.heapIndex = len(*h)
	*h = append(*h, t)
}
func (h *ageHeap) Pop() any {
	old := *h
	n := len(old)
	t := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return t
}
