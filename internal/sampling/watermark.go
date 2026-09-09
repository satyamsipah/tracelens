package sampling

import (
	"container/heap"
	"fmt"
	"sync"
	"time"
)

// OffsetWatermark tracks, per Kafka partition, the lowest offset among
// currently-undecided traces, so a consumer can commit past everything else
// on that partition while withholding exactly the range still at risk.
//
// Why this exists: phase 1 could commit an offset the instant its batch was
// durably written, because decode -> write was immediate. Now a span can sit
// in the trace buffer for the full decision window before its trace
// resolves. Committing an offset as soon as a span is BUFFERED (rather than
// once its trace DECIDES) would mean a crash mid-window loses everything
// still in memory, and Kafka would never redeliver it since the offset
// already advanced -- exactly the silent loss principle 1 forbids.
//
// Each trace touches exactly one partition (spans are Kafka-partitioned by
// trace_id, phase 1), so there is no cross-partition bookkeeping to do: one
// min-heap per partition, keyed by the offset a trace was FIRST seen at, is
// enough. A trace only needs tracking once, at its first-seen offset -- later
// spans of the same trace at higher offsets never lower the watermark further.
type OffsetWatermark struct {
	mu         sync.Mutex
	partitions map[int32]*partitionWatermark
}

type partitionWatermark struct {
	h       offsetHeap
	byTrace map[[16]byte]*offsetEntry
}

type offsetEntry struct {
	offset    int64
	traceID   [16]byte
	heapIndex int

	// trackedAt is wall-clock time at Track, used only by the watchdog
	// (StaleCandidates) to bound how long an entry may be held -- unrelated
	// to offset ordering, so it does not participate in the heap at all.
	trackedAt time.Time
}

// NewOffsetWatermark builds an empty tracker.
func NewOffsetWatermark() *OffsetWatermark {
	return &OffsetWatermark{partitions: make(map[int32]*partitionWatermark)}
}

// Track records that traceID was first seen on partition at offset. A no-op
// if this trace is already tracked on this partition (first offset wins).
func (w *OffsetWatermark) Track(partition int32, offset int64, traceID [16]byte) {
	w.mu.Lock()
	defer w.mu.Unlock()

	pw, ok := w.partitions[partition]
	if !ok {
		pw = &partitionWatermark{byTrace: make(map[[16]byte]*offsetEntry)}
		w.partitions[partition] = pw
	}
	if _, exists := pw.byTrace[traceID]; exists {
		return
	}

	e := &offsetEntry{offset: offset, traceID: traceID, trackedAt: time.Now()}
	pw.byTrace[traceID] = e
	heap.Push(&pw.h, e)
}

// Resolve marks traceID's decision final, removing it from tracking on
// partition. Safe to call for a trace never tracked (a no-op) -- normal
// completion always resolves; the caller does not need to know in advance
// whether Track ever ran for this exact trace/partition pair.
func (w *OffsetWatermark) Resolve(partition int32, traceID [16]byte) {
	w.mu.Lock()
	defer w.mu.Unlock()

	pw, ok := w.partitions[partition]
	if !ok {
		return
	}
	e, ok := pw.byTrace[traceID]
	if !ok {
		return
	}
	delete(pw.byTrace, traceID)
	heap.Remove(&pw.h, e.heapIndex)
}

// SafeOffset returns the highest offset safe to commit on partition: one
// less than the minimum still-tracked offset. hasFloor is false when nothing
// is tracked, meaning there is no restriction from this mechanism -- the
// caller may commit up to whatever the highest offset it separately knows
// about is.
func (w *OffsetWatermark) SafeOffset(partition int32) (offset int64, hasFloor bool) {
	w.mu.Lock()
	defer w.mu.Unlock()

	pw, ok := w.partitions[partition]
	if !ok || pw.h.Len() == 0 {
		return 0, false
	}
	return pw.h[0].offset - 1, true
}

// ClearBelow removes every entry on partition whose tracked offset is less
// than threshold, returning the cleared trace ids (so the caller can also
// drop its own bookkeeping for them, e.g. Assembler.partitionOf). Used when
// the Kafka client itself has proven those offsets unreachable
// (kgo.ErrDataLoss -- see pipeline.DataLossFunc): the normal rule ("only
// Resolve once a trace actually decides") does not apply here, because
// there is no future in which those specific offsets are ever redelivered
// for a decision to happen at all. Without this, a watermark entry pointing
// below threshold would hold its partition's commit floor forever -- an
// unbounded, silent stall, which is worse than the proven, already-happened
// data loss this merely stops from compounding into a stuck consumer too.
func (w *OffsetWatermark) ClearBelow(partition int32, threshold int64) [][16]byte {
	w.mu.Lock()
	defer w.mu.Unlock()

	pw, ok := w.partitions[partition]
	if !ok {
		return nil
	}
	var cleared []*offsetEntry
	for _, e := range pw.h {
		if e.offset < threshold {
			cleared = append(cleared, e)
		}
	}
	ids := make([][16]byte, len(cleared))
	for i, e := range cleared {
		ids[i] = e.traceID
		delete(pw.byTrace, e.traceID)
		heap.Remove(&pw.h, e.heapIndex)
	}
	return ids
}

// StaleEntry is one watermark entry that has been held longer than a
// caller-supplied age bound -- a CANDIDATE for giving up on, not yet
// confirmed safe to. Buffer guarantees every trace resolves within
// DecisionWait (or immediately on capacity eviction), so an entry this old
// can only be a trace whose decide-and-write failed and was never
// redelivered -- but the caller must still confirm the trace is not
// otherwise tracked (Buffer.Tracks) before actually resolving it, since a
// entry's age alone does not distinguish "abandoned" from "an active retry
// that happens to share the original, still-correct floor offset" (Track
// is first-offset-wins, so a redelivered retry does not reset trackedAt).
type StaleEntry struct {
	Partition int32
	Offset    int64
	TraceID   [16]byte
	Age       time.Duration
}

// StaleCandidates returns every entry across all partitions held longer
// than maxAge as of now.
func (w *OffsetWatermark) StaleCandidates(maxAge time.Duration, now time.Time) []StaleEntry {
	w.mu.Lock()
	defer w.mu.Unlock()

	var out []StaleEntry
	for partition, pw := range w.partitions {
		for _, e := range pw.h {
			if age := now.Sub(e.trackedAt); age >= maxAge {
				out = append(out, StaleEntry{Partition: partition, Offset: e.offset, TraceID: e.traceID, Age: age})
			}
		}
	}
	return out
}

// Pending reports how many traces are currently tracked on partition, for
// diagnostics and tests.
func (w *OffsetWatermark) Pending(partition int32) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	pw, ok := w.partitions[partition]
	if !ok {
		return 0
	}
	return len(pw.byTrace)
}

// offsetHeap is a container/heap.Interface over *offsetEntry, ordered by
// offset ascending.
type offsetHeap []*offsetEntry

func (h offsetHeap) Len() int           { return len(h) }
func (h offsetHeap) Less(i, j int) bool { return h[i].offset < h[j].offset }
func (h offsetHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].heapIndex = i
	h[j].heapIndex = j
}
func (h *offsetHeap) Push(x any) {
	// heap.Interface forces `any`; see ageHeap.Push in buffer.go.
	e, ok := x.(*offsetEntry)
	if !ok {
		panic(fmt.Sprintf("offsetHeap.Push: got %T, want *offsetEntry", x))
	}
	e.heapIndex = len(*h)
	*h = append(*h, e)
}
func (h *offsetHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return e
}
