package sampling

import (
	"container/heap"
	"sync"
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

	e := &offsetEntry{offset: offset, traceID: traceID}
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
	e := x.(*offsetEntry)
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
