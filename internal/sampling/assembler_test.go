package sampling

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

type emitCapture struct {
	mu      sync.Mutex
	emitted []emittedTrace
	late    []storage.SpanRow
}

type emittedTrace struct {
	spans    []storage.SpanRow
	decision Decision
}

func (c *emitCapture) emit(spans []storage.SpanRow, d Decision) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.emitted = append(c.emitted, emittedTrace{spans: spans, decision: d})
	return nil
}

func (c *emitCapture) lateAttach(s storage.SpanRow) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.late = append(c.late, s)
	return nil
}

func (c *emitCapture) snapshot() []emittedTrace {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]emittedTrace(nil), c.emitted...)
}

func newTestAssembler(t *testing.T, bufCfg BufferConfig, chain *PolicyChain) (*Assembler, *emitCapture, *observability.Metrics) {
	t.Helper()
	m := observability.NewMetrics()
	buf := NewBuffer(bufCfg, m)
	capture := &emitCapture{}
	a := NewAssembler(buf, chain, m, observability.NewLogger("test"), capture.emit, capture.lateAttach)
	return a, capture, m
}

func TestAssemblerFixedWaitCompletion(t *testing.T) {
	t.Run("should decide a trace only after its decision wait elapses via the sweep", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = 50 * time.Millisecond
		chain := NewPolicyChain([]Policy{NewProbabilistic(1.0)})
		a, capture, _ := newTestAssembler(t, cfg, chain)

		a.Ingest(spanForTrace(1, 1, 0, 0, 10, "ok"), 0, 0)

		stop := make(chan struct{})
		go a.RunSweep(stop, 10*time.Millisecond)
		defer close(stop)

		require.Eventually(t, func() bool { return len(capture.snapshot()) == 1 }, time.Second, 5*time.Millisecond)
		require.Equal(t, VerdictSample, capture.snapshot()[0].decision.Verdict)
	})
}

func TestAssemblerRootClosureEarlyExit(t *testing.T) {
	t.Run("should decide immediately when the root closure condition is met, before the decision wait", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = time.Hour // would never fire naturally within the test
		chain := NewPolicyChain([]Policy{NewProbabilistic(1.0)})
		a, capture, _ := newTestAssembler(t, cfg, chain)

		root := spanForTrace(1, 1, 0, 0, 100, "ok")
		child := spanForTrace(1, 2, 1, 10, 50, "ok") // fits inside [0,100)

		a.Ingest(root, 0, 0)
		a.Ingest(child, 0, 1)

		require.Len(t, capture.snapshot(), 1, "root-closure early exit must decide without waiting for the sweep")
	})
}

func TestAssemblerForcedEvictionEmitsAndResolvesWatermark(t *testing.T) {
	t.Run("should emit and resolve the watermark for a trace forced out by capacity", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.MaxTraces = 1
		cfg.DecisionWait = time.Hour
		chain := NewPolicyChain([]Policy{NewProbabilistic(1.0)})
		a, capture, _ := newTestAssembler(t, cfg, chain)

		// spanID=2 with parent=1: NOT a root (parent never arrives), so this
		// does not satisfy root-closure and stays in-flight rather than
		// early-exiting immediately -- exactly what this test needs to force
		// it out via capacity instead.
		a.Ingest(spanForTrace(1, 2, 1, 0, 10, "ok"), 0, 100)
		require.Equal(t, 0, len(capture.snapshot()))

		// A second, distinct trace forces trace 1 out under MaxTraces=1.
		a.Ingest(spanForTrace(2, 2, 1, 0, 10, "ok"), 0, 200)

		require.Len(t, capture.snapshot(), 1)
		require.Equal(t, byte(1), capture.snapshot()[0].spans[0].TraceID[0])

		// The forced trace's watermark entry must be resolved: with trace 2
		// still undecided at offset 200, the safe commit floor must sit at
		// 199, NOT held back by trace 1's now-resolved offset 100.
		offset, hasFloor := a.SafeCommitOffset(0)
		require.True(t, hasFloor)
		require.Equal(t, int64(199), offset)
	})
}

func TestAssemblerLateSpanWeight(t *testing.T) {
	t.Run("should attach a late span carrying the trace's original weight", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = 20 * time.Millisecond
		// A test-only policy with a known, exact probability so the resulting
		// weight (1/p = 4) is precisely assertable, rather than approximately
		// so as it would be with a real probabilistic policy over one sample.
		chain := NewPolicyChain([]Policy{probabilisticFixedWeight{weight: 4}})
		a, capture, m := newTestAssembler(t, cfg, chain)

		a.Ingest(spanForTrace(1, 1, 0, 0, 10, "ok"), 0, 0)

		stop := make(chan struct{})
		go a.RunSweep(stop, 5*time.Millisecond)
		require.Eventually(t, func() bool { return len(capture.snapshot()) == 1 }, time.Second, 5*time.Millisecond)
		close(stop)

		late := spanForTrace(1, 2, 1, 5, 5, "ok")
		a.Ingest(late, 0, 1)

		require.Eventually(t, func() bool {
			capture.mu.Lock()
			defer capture.mu.Unlock()
			return len(capture.late) == 1
		}, time.Second, 5*time.Millisecond)

		require.Equal(t, float64(4), capture.late[0].SamplingWeight)
		require.Equal(t, float64(1), testutil.ToFloat64(m.LateSpansAttached))
	})
}

// probabilisticFixedWeight is a test-only policy with a fixed, known
// probability, used so the resulting weight is exact and assertable.
type probabilisticFixedWeight struct{ weight float64 }

func (p probabilisticFixedWeight) Name() string { return "test_fixed_weight" }
func (p probabilisticFixedWeight) Evaluate(TraceView) Decision {
	return Decision{Verdict: VerdictSample, Probability: 1 / p.weight}
}

// TestAssemblerWithholdsWatermarkOnWriteFailure locks down the mechanism
// EmitFunc's contract depends on: a failed write must NOT resolve the
// watermark, so the offset stays held for Kafka to redeliver, and must NOT
// record a decision either, so the redelivered trace restarts fresh
// assembly rather than being misclassified as an already-decided late span.
func TestAssemblerWithholdsWatermarkOnWriteFailure(t *testing.T) {
	t.Run("should hold the offset and leave the decision unrecorded so redelivery restarts fresh assembly", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = time.Hour
		m := observability.NewMetrics()
		buf := NewBuffer(cfg, m)

		var emitCalls int
		emit := func(spans []storage.SpanRow, d Decision) error {
			emitCalls++
			if emitCalls == 1 {
				return errWriteFailed // simulate a durable-write failure on the first attempt
			}
			return nil // "retry" (redelivery) succeeds
		}
		a := NewAssembler(buf, NewPolicyChain([]Policy{NewProbabilistic(1.0)}), m,
			observability.NewLogger("test"), emit, func(storage.SpanRow) error { return nil })

		// A single root span satisfies early-exit immediately, so the
		// decision fires (and fails to write) synchronously within Ingest.
		root := spanForTrace(1, 1, 0, 0, 10, "ok")
		a.Ingest(root, 0, 42)
		require.Equal(t, 1, emitCalls)

		offset, hasFloor := a.SafeCommitOffset(0)
		require.True(t, hasFloor, "a failed write must leave the offset held, not cleared")
		require.Equal(t, int64(41), offset, "the floor must sit at the failed trace's own offset minus one")

		// Simulate Kafka redelivery: the identical span arrives again (same
		// trace_id, same offset, since nothing was committed). If the first
		// failure had wrongly recorded a decision, this would be
		// misclassified as a late span of an already-decided trace and
		// routed to lateAttach instead of emit.
		a.Ingest(root, 0, 42)
		require.Equal(t, 2, emitCalls, "redelivery must trigger a FRESH decide-and-emit, not a late-attach")

		_, hasFloor = a.SafeCommitOffset(0)
		require.False(t, hasFloor, "the retry succeeding must finally clear the watermark")
	})
}

var errWriteFailed = fmt.Errorf("simulated durable write failure")

func TestAssemblerHotReload(t *testing.T) {
	t.Run("should use the newly set chain for decisions made after SetChain", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = time.Hour
		a, capture, _ := newTestAssembler(t, cfg, NewPolicyChain([]Policy{NewProbabilistic(0.0)}))

		root := spanForTrace(1, 1, 0, 0, 10, "ok")
		a.Ingest(root, 0, 0) // root-closure early exit fires immediately (single span is its own closure)
		require.Equal(t, VerdictDrop, capture.snapshot()[0].decision.Verdict)

		a.SetChain(NewPolicyChain([]Policy{NewProbabilistic(1.0)}))

		root2 := spanForTrace(2, 1, 0, 0, 10, "ok")
		a.Ingest(root2, 0, 1)
		require.Equal(t, VerdictSample, capture.snapshot()[1].decision.Verdict)
	})
}

// TestErrorsAlwaysRetainedAtOverallLowRate is the phase's explicit
// requirement: error traces at ~1% overall volume must be retained at 100%,
// verified by actually running the composed chain over a mixed population.
func TestErrorsAlwaysRetainedAtOverallLowRate(t *testing.T) {
	t.Run("should retain 100% of error traces while sampling non-errors at the baseline rate", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = time.Hour
		chain := NewPolicyChain([]Policy{
			NewAlwaysSampleErrors(),
			NewProbabilistic(0.05),
		})
		a, capture, _ := newTestAssembler(t, cfg, chain)

		const total = 20_000
		const errorRate = 0.01 // errors are 1% of overall traffic
		errorCount := 0

		for i := 0; i < total; i++ {
			status := "ok"
			isError := float64(i%100) < errorRate*100
			if isError {
				status = "error"
				errorCount++
			}
			s := spanForTrace(byte(i%256), byte(i%251+1), 0, 0, 10, status)
			// vary trace id across the full 16 bytes so probabilistic sampling
			// actually spreads, not just the low byte used elsewhere in tests
			s.TraceID[1] = byte(i >> 8)
			s.TraceID[2] = byte(i >> 16)
			a.Ingest(s, 0, int64(i))
		}

		emitted := capture.snapshot()
		sampledErrors, totalErrors, sampledNormal, totalNormal := 0, 0, 0, 0
		for _, e := range emitted {
			isError := HasError(e.spans)
			if isError {
				totalErrors++
				if e.decision.Verdict == VerdictSample {
					sampledErrors++
				}
			} else {
				totalNormal++
				if e.decision.Verdict == VerdictSample {
					sampledNormal++
				}
			}
		}

		require.Equal(t, totalErrors, sampledErrors, "every single error trace must be retained -- zero tolerance")
		require.Greater(t, totalErrors, 0, "test must actually generate some errors to be meaningful")

		normalSampleRate := float64(sampledNormal) / float64(totalNormal)
		t.Logf("errors: %d/%d retained (100%%). normal: %d/%d retained (%.1f%%, target ~5%%)",
			sampledErrors, totalErrors, sampledNormal, totalNormal, normalSampleRate*100)
		require.InDelta(t, 0.05, normalSampleRate, 0.02, "non-error traces must sample near the configured baseline rate")
	})
}

func TestAssemblerHandleDataLoss(t *testing.T) {
	t.Run("should release a watermark entry Kafka proved is unreachable, and count it", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = time.Hour
		m := observability.NewMetrics()
		buf := NewBuffer(cfg, m)

		emit := func([]storage.SpanRow, Decision) error { return errWriteFailed }
		a := NewAssembler(buf, NewPolicyChain([]Policy{NewProbabilistic(1.0)}), m,
			observability.NewLogger("test"), emit, func(storage.SpanRow) error { return nil })

		// Root-closure early exit decides (and fails to write) synchronously,
		// leaving the watermark held at offset 41 -- identical setup to
		// TestAssemblerWithholdsWatermarkOnWriteFailure.
		root := spanForTrace(1, 1, 0, 0, 10, "ok")
		a.Ingest(root, 0, 42)

		_, hasFloor := a.SafeCommitOffset(0)
		require.True(t, hasFloor, "precondition: the failed write must leave the offset held")

		// Kafka proves everything below offset 100 on partition 0 is gone --
		// this covers the held offset (41).
		a.HandleDataLoss(0, 0, 100)

		_, hasFloor = a.SafeCommitOffset(0)
		require.False(t, hasFloor, "a proven-lost offset must release the floor immediately")
		require.Equal(t, float64(1), testutil.ToFloat64(m.OffsetWatermarkDataLossTotal))
	})

	t.Run("should not touch a watermark entry outside the lost range", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = time.Hour
		m := observability.NewMetrics()
		buf := NewBuffer(cfg, m)

		emit := func([]storage.SpanRow, Decision) error { return errWriteFailed }
		a := NewAssembler(buf, NewPolicyChain([]Policy{NewProbabilistic(1.0)}), m,
			observability.NewLogger("test"), emit, func(storage.SpanRow) error { return nil })

		root := spanForTrace(1, 1, 0, 0, 10, "ok")
		a.Ingest(root, 0, 500) // held at offset 500, well above the lost range

		a.HandleDataLoss(0, 0, 100) // proven-lost range is [0, 100)

		_, hasFloor := a.SafeCommitOffset(0)
		require.True(t, hasFloor, "an entry outside the proven-lost range must not be released")
		require.Equal(t, float64(0), testutil.ToFloat64(m.OffsetWatermarkDataLossTotal))
	})

	t.Run("should no-op when nothing is held on the affected partition", func(t *testing.T) {
		a, capture, m := newTestAssembler(t, testBufferConfig(EvictionForcedDecision), NewPolicyChain([]Policy{NewProbabilistic(1.0)}))
		_ = capture
		require.NotPanics(t, func() { a.HandleDataLoss(0, 0, 1000) })
		require.Equal(t, float64(0), testutil.ToFloat64(m.OffsetWatermarkDataLossTotal))
	})
}

func TestAssemblerWatermarkWatchdog(t *testing.T) {
	t.Run("should expire an orphaned entry once it exceeds maxAge, and count it", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = time.Hour
		m := observability.NewMetrics()
		buf := NewBuffer(cfg, m)

		emit := func([]storage.SpanRow, Decision) error { return errWriteFailed }
		a := NewAssembler(buf, NewPolicyChain([]Policy{NewProbabilistic(1.0)}), m,
			observability.NewLogger("test"), emit, func(storage.SpanRow) error { return nil })

		root := spanForTrace(1, 1, 0, 0, 10, "ok")
		a.Ingest(root, 0, 42) // write fails, trace leaves the buffer, watermark held at 41

		_, hasFloor := a.SafeCommitOffset(0)
		require.True(t, hasFloor, "precondition")

		a.checkWatermarkOnce(time.Now().Add(time.Hour), 10*time.Minute)

		_, hasFloor = a.SafeCommitOffset(0)
		require.False(t, hasFloor, "an orphaned entry (write failed, no redelivery, no longer tracked) must be expired past maxAge")
		require.Equal(t, float64(1), testutil.ToFloat64(m.OffsetWatermarkExpiredTotal))
	})

	t.Run("should NOT expire an entry whose trace is still legitimately in-flight", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = 24 * time.Hour // never completes on its own during this test
		m := observability.NewMetrics()
		buf := NewBuffer(cfg, m)
		a := NewAssembler(buf, NewPolicyChain([]Policy{NewProbabilistic(1.0)}), m,
			observability.NewLogger("test"), func([]storage.SpanRow, Decision) error { return nil },
			func(storage.SpanRow) error { return nil })

		// A non-root span never satisfies early-exit, so it stays genuinely
		// buffered -- exactly the case the watchdog must not disturb, even
		// though its watermark entry is now "old" by the same measure.
		child := spanForTrace(1, 2, 1, 0, 10, "ok")
		a.Ingest(child, 0, 42)

		a.checkWatermarkOnce(time.Now().Add(time.Hour), 10*time.Minute)

		_, hasFloor := a.SafeCommitOffset(0)
		require.True(t, hasFloor, "a trace still tracked in the buffer must never have its floor released out from under it")
		require.Equal(t, float64(0), testutil.ToFloat64(m.OffsetWatermarkExpiredTotal))
	})

	t.Run("should not expire an entry younger than maxAge", func(t *testing.T) {
		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = time.Hour
		m := observability.NewMetrics()
		buf := NewBuffer(cfg, m)
		emit := func([]storage.SpanRow, Decision) error { return errWriteFailed }
		a := NewAssembler(buf, NewPolicyChain([]Policy{NewProbabilistic(1.0)}), m,
			observability.NewLogger("test"), emit, func(storage.SpanRow) error { return nil })

		root := spanForTrace(1, 1, 0, 0, 10, "ok")
		a.Ingest(root, 0, 42)

		a.checkWatermarkOnce(time.Now(), 10*time.Minute) // no time has passed

		_, hasFloor := a.SafeCommitOffset(0)
		require.True(t, hasFloor, "an entry younger than maxAge must be left alone")
		require.Equal(t, float64(0), testutil.ToFloat64(m.OffsetWatermarkExpiredTotal))
	})
}
