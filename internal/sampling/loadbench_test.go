package sampling

import (
	"encoding/binary"
	"runtime"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

func spanForTraceN(n uint32, spanID byte, startMS, durMS int, status string) storage.SpanRow {
	s := span(spanID, 0, startMS, durMS, status)
	tid := make([]byte, 16)
	binary.BigEndian.PutUint32(tid, n)
	s.TraceID = tid
	return s
}

// TestBufferMemoryPerInFlightTrace measures bytes-per-trace two ways: the
// Buffer's own accounted size (spanSize -- the exact unit
// TRACELENS_BUFFER_MAX_BYTES is denominated in, read via the
// tracelens_inflight_bytes gauge so this exercises the real production
// accounting path rather than reimplementing it) and, best-effort, actual
// Go heap growth as a cross-check. The accounted figure is the one that
// answers the operator's real question -- translating CLAUDE.md principle
// 2's "hard cap" into a MAX_BYTES setting for a target MAX_TRACES -- since
// that is the unit the config itself uses; raw heap RSS also carries map
// and slice-header overhead the accounting deliberately excludes, so it is
// reported only as a secondary sanity signal, not asserted on.
func TestBufferMemoryPerInFlightTrace(t *testing.T) {
	if testing.Short() {
		t.Skip("allocates a large number of traces; skip in -short")
	}
	const n = 50_000

	cfg := BufferConfig{
		MaxTraces:        n + 1, // never forces a decision -- isolates steady-state occupancy
		MaxBytes:         1 << 34,
		Eviction:         EvictionForcedDecision,
		DecisionWait:     time.Hour,
		DecidedCacheSize: 1000,
		DecidedCacheTTL:  time.Minute,
	}
	m := observability.NewMetrics()
	buf := NewBuffer(cfg, m)
	noop := func([]storage.SpanRow) Decision { return Decision{Verdict: VerdictDrop} }

	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := uint32(0); i < n; i++ {
		buf.Ingest(spanForTraceN(i, 1, 0, 10, "ok"), noop)
	}

	accountedBytes := testutil.ToFloat64(m.InflightBytes)
	accountedPerTrace := accountedBytes / float64(n)
	t.Logf("BENCH: buffer_memory_per_inflight_trace_bytes_accounted=%.1f (n=%d, total_accounted=%.0f bytes)", accountedPerTrace, n, accountedBytes)

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)
	if after.HeapAlloc > before.HeapAlloc {
		heapPerTrace := float64(after.HeapAlloc-before.HeapAlloc) / float64(n)
		t.Logf("BENCH: buffer_memory_per_inflight_trace_bytes_heap=%.1f (cross-check only, includes Go runtime overhead)", heapPerTrace)
	} else {
		t.Logf("heap cross-check inconclusive (GC noise); accounted figure above is the load-bearing number")
	}
}

// TestLateSpanRateAtDifferentDecisionWaits measures how many spans arrive
// AFTER their trace has already decided, at three DecisionWait settings,
// against a fixed synthetic distribution of child-span arrival delay (real
// wall-clock delay -- Buffer.Ingest records firstSeen via time.Now()
// internally, so there is no fake-clock hook to fast-forward this without
// actually elapsing time). Each trace's decision is driven at exactly
// DecisionWait after its root by explicitly sequencing sleep/decide/ingest
// around the delay-vs-wait comparison, rather than racing a real background
// sweep goroutine -- deterministic outcome, at the cost of the test itself
// taking real time proportional to the delay distribution (kept small: well
// under 2s total).
func TestLateSpanRateAtDifferentDecisionWaits(t *testing.T) {
	waits := []time.Duration{20 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond}
	delays := []time.Duration{
		0, 5 * time.Millisecond, 10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond,
		80 * time.Millisecond, 160 * time.Millisecond, 320 * time.Millisecond, 640 * time.Millisecond,
	}

	for _, wait := range waits {
		t.Run(wait.String(), func(t *testing.T) {
			cfg := BufferConfig{
				MaxTraces:        10_000,
				MaxBytes:         1 << 30,
				Eviction:         EvictionForcedDecision,
				DecisionWait:     wait,
				DecidedCacheSize: 10_000,
				DecidedCacheTTL:  time.Minute,
			}
			m := observability.NewMetrics()
			buf := NewBuffer(cfg, m)
			chain := NewPolicyChain([]Policy{NewProbabilistic(1.0)}) // always sample, so a late span always attaches rather than drops
			forceDecide := func(spans []storage.SpanRow) Decision { return chain.Decide(traceViewFrom(spans)) }
			decide := func(tr *bufferedTrace) {
				d := chain.Decide(traceViewFrom(tr.spans))
				buf.RecordDecision(tr.traceID, d)
			}

			var late, total int
			for i, delay := range delays {
				traceID := uint32(i)
				root := spanForTraceN(traceID, 1, 0, 5, "ok")
				buf.Ingest(root, forceDecide)

				if delay <= wait {
					time.Sleep(delay)
					buf.Ingest(spanForTraceN(traceID, 2, int(delay.Milliseconds()), 5, "ok"), forceDecide)
					time.Sleep(wait - delay)
					for _, tr := range buf.PopReady(time.Now()) {
						decide(tr)
					}
				} else {
					time.Sleep(wait)
					for _, tr := range buf.PopReady(time.Now()) {
						decide(tr)
					}
					time.Sleep(delay - wait)
					result, _ := buf.Ingest(spanForTraceN(traceID, 2, int(delay.Milliseconds()), 5, "ok"), forceDecide)
					if result == IngestLateAttached || result == IngestLateDropped {
						late++
					}
				}
				total++
			}
			rate := float64(late) / float64(total)
			t.Logf("BENCH: late_span_rate wait=%s rate=%.3f (%d/%d)", wait, rate, late, total)
		})
	}
}
