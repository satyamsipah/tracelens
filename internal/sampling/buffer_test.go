package sampling

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

func testBufferConfig(policy EvictionPolicy) BufferConfig {
	return BufferConfig{
		MaxTraces:        10,
		MaxBytes:         1 << 20,
		Eviction:         policy,
		DecisionWait:     100 * time.Millisecond,
		DecidedCacheSize: 1000,
		DecidedCacheTTL:  time.Minute,
	}
}

func traceIDOf(spans []storage.SpanRow) [16]byte {
	var id [16]byte
	if len(spans) > 0 {
		copy(id[:], spans[0].TraceID)
	}
	return id
}

func spanForTrace(traceID byte, spanID, parent byte, startMS, durMS int, status string) storage.SpanRow {
	s := span(spanID, parent, startMS, durMS, status)
	tid := make([]byte, 16)
	tid[0] = traceID
	s.TraceID = tid
	return s
}

func alwaysDropDecide(spans []storage.SpanRow) Decision {
	return Decision{Verdict: VerdictDrop, Probability: 1}
}

func alwaysSampleDecide(spans []storage.SpanRow) Decision {
	return Decision{Verdict: VerdictSample, Probability: 1}
}

func TestBufferOutOfOrderArrival(t *testing.T) {
	t.Run("should assemble a correct tree regardless of span arrival order", func(t *testing.T) {
		m := observability.NewMetrics()
		b := NewBuffer(testBufferConfig(EvictionForcedDecision), m)

		// Child arrives BEFORE its parent -- the exact out-of-order case the
		// phase calls out.
		child := spanForTrace(1, 2, 1, 10, 20, "ok")
		root := spanForTrace(1, 1, 0, 0, 100, "ok")

		b.Ingest(child, alwaysDropDecide)
		b.Ingest(root, alwaysDropDecide)

		traces, _ := b.Stats()
		require.Equal(t, 1, traces, "both spans must join the SAME trace regardless of arrival order")

		ready := b.PopReady(time.Now().Add(200 * time.Millisecond))
		require.Len(t, ready, 1)
		require.Len(t, ready[0].spans, 2)

		tree := BuildTree(ready[0].spans)
		require.Len(t, tree.Roots, 1, "the tree must find the root correctly even though it arrived second")
		require.Empty(t, tree.Orphans)
		require.Len(t, tree.Roots[0].Children, 1)
	})
}

func TestBufferSaturationForcedDecision(t *testing.T) {
	t.Run("should force-decide the oldest trace when the trace-count cap is hit, never discarding it", func(t *testing.T) {
		m := observability.NewMetrics()
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.MaxTraces = 3
		cfg.MaxBytes = 1 << 30
		b := NewBuffer(cfg, m)

		var decidedSpans [][]storage.SpanRow
		decide := func(spans []storage.SpanRow) Decision {
			decidedSpans = append(decidedSpans, spans)
			return Decision{Verdict: VerdictSample, Probability: 1}
		}

		for i := byte(1); i <= 4; i++ {
			b.Ingest(spanForTrace(i, 1, 0, 0, 10, "ok"), decide)
		}

		traces, _ := b.Stats()
		require.Equal(t, 3, traces, "buffer must stay at the cap, not grow past it")
		require.Len(t, decidedSpans, 1, "exactly one trace must have been forced to make room for the 4th")
		require.Equal(t, byte(1), decidedSpans[0][0].TraceID[0], "the OLDEST trace must be the one forced, not an arbitrary one")

		require.Equal(t, float64(3), testutil.ToFloat64(m.InflightTraces), "gauge must track actual occupancy")
		require.Equal(t, float64(0), testutil.ToFloat64(m.EvictedTracesTotal), "forced decision must not ALSO count as a discard")
		require.Greater(t, testutil.ToFloat64(m.ForcedDecisionsTotal), float64(0))
	})

	t.Run("should never let the buffer grow past its cap even under sustained pressure", func(t *testing.T) {
		m := observability.NewMetrics()
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.MaxTraces = 5
		b := NewBuffer(cfg, m)

		for i := 0; i < 5000; i++ {
			id := byte(i % 256)
			b.Ingest(spanForTrace(id, byte(i%251+1), 0, 0, 10, "ok"), alwaysSampleDecide)
			traces, bytes := b.Stats()
			require.LessOrEqualf(t, traces, cfg.MaxTraces, "trace count must never exceed the cap, even transiently")
			require.LessOrEqualf(t, bytes, cfg.MaxBytes, "byte total must never exceed the cap")
		}
	})
}

func TestBufferSaturationDiscard(t *testing.T) {
	t.Run("should discard the oldest trace's spans outright under the discard policy", func(t *testing.T) {
		m := observability.NewMetrics()
		cfg := testBufferConfig(EvictionDiscard)
		cfg.MaxTraces = 2
		b := NewBuffer(cfg, m)

		forceDecideCalled := false
		decide := func(spans []storage.SpanRow) Decision {
			forceDecideCalled = true
			return Decision{Verdict: VerdictSample, Probability: 1}
		}

		b.Ingest(spanForTrace(1, 1, 0, 0, 10, "ok"), decide)
		b.Ingest(spanForTrace(2, 1, 0, 0, 10, "ok"), decide)
		b.Ingest(spanForTrace(3, 1, 0, 0, 10, "ok"), decide)

		require.False(t, forceDecideCalled, "discard policy must never invoke the decision function")
		require.Equal(t, float64(1), testutil.ToFloat64(m.EvictedTracesTotal))
		require.Equal(t, float64(0), testutil.ToFloat64(m.ForcedDecisionsTotal))

		traces, _ := b.Stats()
		require.Equal(t, 2, traces)
	})
}

func TestBufferByteCap(t *testing.T) {
	t.Run("should trigger eviction on byte pressure even when trace count is under its cap", func(t *testing.T) {
		m := observability.NewMetrics()
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.MaxTraces = 1000 // effectively unbounded by count
		cfg.MaxBytes = 500   // tiny, so a handful of spans trips it

		b := NewBuffer(cfg, m)
		decided := 0
		decide := func(spans []storage.SpanRow) Decision {
			decided++
			return Decision{Verdict: VerdictSample, Probability: 1}
		}

		for i := byte(1); i <= 20; i++ {
			s := spanForTrace(i, 1, 0, 0, 10, "ok")
			s.SpanAttributes = map[string]string{"k": "some reasonably sized attribute value here"}
			b.Ingest(s, decide)
		}

		require.Greater(t, decided, 0, "byte pressure alone must trigger forced decisions")
		_, bytes := b.Stats()
		require.LessOrEqual(t, bytes, cfg.MaxBytes)
	})
}

func TestBufferLateSpans(t *testing.T) {
	t.Run("should attach a late span with the trace's weight when it was sampled", func(t *testing.T) {
		m := observability.NewMetrics()
		b := NewBuffer(testBufferConfig(EvictionForcedDecision), m)

		s := spanForTrace(1, 1, 0, 0, 10, "ok")
		b.Ingest(s, alwaysSampleDecide)
		ready := b.PopReady(time.Now().Add(time.Second))
		require.Len(t, ready, 1)

		decision := Decision{Verdict: VerdictSample, Probability: 0.1}
		b.RecordDecision(traceIDOf(ready[0].spans), decision)

		late := spanForTrace(1, 2, 1, 5, 5, "ok")
		result, weight := b.Ingest(late, alwaysDropDecide)
		require.Equal(t, IngestLateAttached, result)
		require.Equal(t, decision.Weight(), weight, "a late-attached span must carry its trace's ORIGINAL weight, not a freshly computed one")
		require.Equal(t, float64(1), testutil.ToFloat64(m.LateSpansAttached))
	})

	t.Run("should drop a late span, counted, when the trace was not sampled", func(t *testing.T) {
		m := observability.NewMetrics()
		b := NewBuffer(testBufferConfig(EvictionForcedDecision), m)

		s := spanForTrace(1, 1, 0, 0, 10, "ok")
		b.Ingest(s, alwaysDropDecide)
		ready := b.PopReady(time.Now().Add(time.Second))
		b.RecordDecision(traceIDOf(ready[0].spans), Decision{Verdict: VerdictDrop, Probability: 1})

		late := spanForTrace(1, 2, 1, 5, 5, "ok")
		result, _ := b.Ingest(late, alwaysDropDecide)
		require.Equal(t, IngestLateDropped, result)
		require.Equal(t, float64(1), testutil.ToFloat64(m.LateSpansDropped))
	})

	t.Run("should treat a span as a brand new trace once its decided-cache entry has expired", func(t *testing.T) {
		m := observability.NewMetrics()
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecidedCacheSize = 1 // tiny, so the next decision evicts this one
		b := NewBuffer(cfg, m)

		b.RecordDecision(traceIDOf([]storage.SpanRow{spanForTrace(1, 1, 0, 0, 10, "ok")}), Decision{Verdict: VerdictSample, Probability: 1})
		// Push a second decision to evict the first from the tiny cache.
		b.RecordDecision(traceIDOf([]storage.SpanRow{spanForTrace(2, 1, 0, 0, 10, "ok")}), Decision{Verdict: VerdictSample, Probability: 1})

		result, _ := b.Ingest(spanForTrace(1, 3, 0, 0, 10, "ok"), alwaysDropDecide)
		require.Equal(t, IngestBuffered, result, "an evicted decided-cache entry must fall back to fresh assembly, not error")
	})
}

func TestBufferEarlyExit(t *testing.T) {
	t.Run("should complete early when the root is present and every span fits inside its interval", func(t *testing.T) {
		m := observability.NewMetrics()
		b := NewBuffer(testBufferConfig(EvictionForcedDecision), m)

		root := spanForTrace(1, 1, 0, 0, 100, "ok")
		child := spanForTrace(1, 2, 1, 10, 50, "ok")
		b.Ingest(root, alwaysDropDecide)
		b.Ingest(child, alwaysDropDecide)

		spans, ok := b.TryEarlyExit(traceIDOf([]storage.SpanRow{root}))
		require.True(t, ok)
		require.Len(t, spans, 2)

		traces, _ := b.Stats()
		require.Equal(t, 0, traces, "an early-exited trace must leave the buffer")
	})

	t.Run("should NOT complete early when a span extends past the root's end", func(t *testing.T) {
		m := observability.NewMetrics()
		b := NewBuffer(testBufferConfig(EvictionForcedDecision), m)

		root := spanForTrace(1, 1, 0, 0, 50, "ok")
		lateFinishingChild := spanForTrace(1, 2, 1, 10, 100, "ok") // ends at 110, past root's 50
		b.Ingest(root, alwaysDropDecide)
		b.Ingest(lateFinishingChild, alwaysDropDecide)

		_, ok := b.TryEarlyExit(traceIDOf([]storage.SpanRow{root}))
		require.False(t, ok)
	})

	t.Run("should NOT complete early when no root has arrived yet", func(t *testing.T) {
		m := observability.NewMetrics()
		b := NewBuffer(testBufferConfig(EvictionForcedDecision), m)

		child := spanForTrace(1, 2, 1, 10, 50, "ok") // parent id 1 never arrived
		b.Ingest(child, alwaysDropDecide)

		_, ok := b.TryEarlyExit(traceIDOf([]storage.SpanRow{child}))
		require.False(t, ok)
	})
}

func TestBufferPopReady(t *testing.T) {
	t.Run("should not pop a trace before its decision wait has elapsed", func(t *testing.T) {
		m := observability.NewMetrics()
		b := NewBuffer(testBufferConfig(EvictionForcedDecision), m)
		b.Ingest(spanForTrace(1, 1, 0, 0, 10, "ok"), alwaysDropDecide)

		ready := b.PopReady(time.Now())
		require.Empty(t, ready)
	})

	t.Run("should pop in oldest-first order", func(t *testing.T) {
		m := observability.NewMetrics()
		b := NewBuffer(testBufferConfig(EvictionForcedDecision), m)

		b.Ingest(spanForTrace(1, 1, 0, 0, 10, "ok"), alwaysDropDecide)
		time.Sleep(2 * time.Millisecond)
		b.Ingest(spanForTrace(2, 1, 0, 0, 10, "ok"), alwaysDropDecide)

		ready := b.PopReady(time.Now().Add(time.Hour))
		require.Len(t, ready, 2)
		require.Equal(t, byte(1), ready[0].spans[0].TraceID[0])
		require.Equal(t, byte(2), ready[1].spans[0].TraceID[0])
	})
}
