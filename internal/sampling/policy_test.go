package sampling

import (
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/storage"
)

func traceViewFrom(spans []storage.SpanRow) TraceView {
	var id [16]byte
	if len(spans) > 0 {
		copy(id[:], spans[0].TraceID)
	}
	return TraceView{
		TraceID:  id,
		Spans:    spans,
		Tree:     BuildTree(spans),
		Duration: TraceDuration(spans),
	}
}

func TestAlwaysSampleErrors(t *testing.T) {
	p := NewAlwaysSampleErrors()

	t.Run("should sample with certainty when any span errored", func(t *testing.T) {
		d := p.Evaluate(traceViewFrom([]storage.SpanRow{span(1, 0, 0, 10, "error")}))
		require.Equal(t, VerdictSample, d.Verdict)
		require.Equal(t, float64(1), d.Probability)
		require.Equal(t, float64(1), d.Weight())
	})

	t.Run("should abstain when nothing errored, not drop", func(t *testing.T) {
		d := p.Evaluate(traceViewFrom([]storage.SpanRow{span(1, 0, 0, 10, "ok")}))
		require.Equal(t, VerdictAbstain, d.Verdict)
	})
}

func TestAlwaysSampleSlowThreshold(t *testing.T) {
	p := NewAlwaysSampleSlowThreshold(100 * time.Millisecond)

	t.Run("should sample when trace duration exceeds the threshold", func(t *testing.T) {
		d := p.Evaluate(traceViewFrom([]storage.SpanRow{span(1, 0, 0, 200, "ok")}))
		require.Equal(t, VerdictSample, d.Verdict)
		require.Equal(t, float64(1), d.Probability)
	})

	t.Run("should abstain when trace duration is under the threshold", func(t *testing.T) {
		d := p.Evaluate(traceViewFrom([]storage.SpanRow{span(1, 0, 0, 10, "ok")}))
		require.Equal(t, VerdictAbstain, d.Verdict)
	})
}

func TestAlwaysSampleSlowRollingP99(t *testing.T) {
	t.Run("should adapt to observed traffic and flag genuine outliers", func(t *testing.T) {
		p := NewAlwaysSampleSlowRollingP99(100)

		// Feed a stable baseline of ~10ms traces first.
		for i := 0; i < 150; i++ {
			p.Evaluate(traceViewFrom([]storage.SpanRow{span(1, 0, 0, 10, "ok")}))
		}

		// A trace 50x the baseline must now read as slow.
		d := p.Evaluate(traceViewFrom([]storage.SpanRow{span(1, 0, 0, 500, "ok")}))
		require.Equal(t, VerdictSample, d.Verdict)
	})

	t.Run("should abstain before enough data has accumulated to estimate a p99", func(t *testing.T) {
		p := NewAlwaysSampleSlowRollingP99(100)
		d := p.Evaluate(traceViewFrom([]storage.SpanRow{span(1, 0, 0, 999999, "ok")}))
		require.Equal(t, VerdictAbstain, d.Verdict, "the very first observation has no history to compare against")
	})
}

func TestProbabilisticDeterminism(t *testing.T) {
	t.Run("should return the same verdict for the same trace id on repeated evaluation", func(t *testing.T) {
		p := NewProbabilistic(0.1)
		view := traceViewFrom([]storage.SpanRow{span(1, 0, 0, 10, "ok")})
		view.TraceID = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}

		first := p.Evaluate(view)
		for i := 0; i < 50; i++ {
			require.Equal(t, first.Verdict, p.Evaluate(view).Verdict,
				"a replayed decision must be idempotent or weighted aggregates corrupt")
		}
	})

	t.Run("should sample approximately the configured rate over many distinct trace ids", func(t *testing.T) {
		p := NewProbabilistic(0.1)
		sampled := 0
		const n = 50_000
		for i := 0; i < n; i++ {
			var id [16]byte
			id[0] = byte(i)
			id[1] = byte(i >> 8)
			id[2] = byte(i >> 16)
			view := TraceView{TraceID: id}
			if p.Evaluate(view).Verdict == VerdictSample {
				sampled++
			}
		}
		rate := float64(sampled) / n
		require.InDelta(t, 0.1, rate, 0.01)
	})
}

func TestAttributeMatch(t *testing.T) {
	p := NewAttributeMatch("customer.tier", "enterprise")

	t.Run("should sample with certainty when a span attribute matches", func(t *testing.T) {
		s := span(1, 0, 0, 10, "ok")
		s.SpanAttributes = map[string]string{"customer.tier": "enterprise"}
		d := p.Evaluate(traceViewFrom([]storage.SpanRow{s}))
		require.Equal(t, VerdictSample, d.Verdict)
		require.Equal(t, float64(1), d.Probability)
	})

	t.Run("should sample when a resource attribute matches instead", func(t *testing.T) {
		s := span(1, 0, 0, 10, "ok")
		s.ResourceAttributes = map[string]string{"customer.tier": "enterprise"}
		d := p.Evaluate(traceViewFrom([]storage.SpanRow{s}))
		require.Equal(t, VerdictSample, d.Verdict)
	})

	t.Run("should abstain when no span matches", func(t *testing.T) {
		s := span(1, 0, 0, 10, "ok")
		s.SpanAttributes = map[string]string{"customer.tier": "free"}
		d := p.Evaluate(traceViewFrom([]storage.SpanRow{s}))
		require.Equal(t, VerdictAbstain, d.Verdict)
	})
}

func TestRateLimiter(t *testing.T) {
	t.Run("should abstain (deferring the decision) while capacity is available, then veto once exhausted", func(t *testing.T) {
		p := NewRateLimiter(5) // 5/sec, bucket starts full

		underCap, dropped := 0, 0
		for i := 0; i < 20; i++ {
			s := span(1, 0, 0, 10, "ok")
			s.ServiceName = "checkout"
			d := p.Evaluate(traceViewFrom([]storage.SpanRow{s}))
			switch d.Verdict {
			case VerdictAbstain:
				underCap++
			case VerdictDrop:
				dropped++
				require.Equal(t, float64(1), d.Probability, "an exhausted bucket's veto must be certain, not a guess")
			default:
				t.Fatalf("rate_limiting must never itself decide Sample -- that belongs to whatever policy resolves after it abstains, got %v", d.Verdict)
			}
		}
		require.LessOrEqual(t, underCap, 5, "burst above the bucket size must be capped almost immediately")
		require.Positive(t, dropped)
	})

	t.Run("should track separate buckets per service", func(t *testing.T) {
		p := NewRateLimiter(1)

		a := span(1, 0, 0, 10, "ok")
		a.ServiceName = "checkout"
		b := span(1, 0, 0, 10, "ok")
		b.ServiceName = "payments"

		da := p.Evaluate(traceViewFrom([]storage.SpanRow{a}))
		db := p.Evaluate(traceViewFrom([]storage.SpanRow{b}))
		require.Equal(t, VerdictAbstain, da.Verdict)
		require.Equal(t, VerdictAbstain, db.Verdict, "a busy service's bucket must not starve a different service")
	})
}

// TestRateLimiterComposesWithProbabilistic locks down the fix: rate_limiting
// and probabilistic both used to always resolve, making whichever came
// first in a chain make the other completely unreachable regardless of
// ordering. rate_limiting now abstains under capacity, so placing it BEFORE
// probabilistic must let probabilistic actually decide when capacity is
// available, and rate_limiting must veto once genuinely exhausted.
func TestRateLimiterComposesWithProbabilistic(t *testing.T) {
	t.Run("should let probabilistic decide when the rate limiter has capacity", func(t *testing.T) {
		chain := NewPolicyChain([]Policy{NewRateLimiter(1e9), NewProbabilistic(1.0)})
		d := chain.Decide(traceViewFrom([]storage.SpanRow{span(1, 0, 0, 10, "ok")}))
		require.Equal(t, VerdictSample, d.Verdict)
		require.Equal(t, "probabilistic", d.PolicyName,
			"probabilistic must be reachable when rate_limiting abstains, not shadowed by it")
	})

	t.Run("should veto before probabilistic ever runs once the rate limiter is exhausted", func(t *testing.T) {
		chain := NewPolicyChain([]Policy{NewRateLimiter(1), NewProbabilistic(1.0)})
		view := traceViewFrom([]storage.SpanRow{span(1, 0, 0, 10, "ok")})

		first := chain.Decide(view) // consumes the single token
		require.Equal(t, VerdictSample, first.Verdict, "the first call has capacity via probabilistic")

		second := chain.Decide(view)
		require.Equal(t, VerdictDrop, second.Verdict)
		require.Equal(t, "rate_limiting", second.PolicyName,
			"an exhausted bucket must veto directly, never reaching probabilistic")
	})
}

func TestPolicyChainComposition(t *testing.T) {
	chain := NewPolicyChain([]Policy{
		NewAlwaysSampleErrors(),
		NewAlwaysSampleSlowThreshold(1 * time.Second),
		NewProbabilistic(0.0), // never samples, isolates prior policies
	})

	t.Run("should let an earlier policy's verdict win over a later one", func(t *testing.T) {
		errSpan := span(1, 0, 0, 10, "error")
		d := chain.Decide(traceViewFrom([]storage.SpanRow{errSpan}))
		require.Equal(t, VerdictSample, d.Verdict)
		require.Equal(t, "always_sample_errors", d.PolicyName)
	})

	t.Run("should fall through to a later policy when earlier ones abstain", func(t *testing.T) {
		okSpan := span(1, 0, 0, 10, "ok")
		d := chain.Decide(traceViewFrom([]storage.SpanRow{okSpan}))
		require.Equal(t, VerdictDrop, d.Verdict)
		require.Equal(t, "probabilistic", d.PolicyName)
	})

	t.Run("should drop, not silently sample, when every policy abstains", func(t *testing.T) {
		empty := NewPolicyChain([]Policy{NewAttributeMatch("k", "v")})
		d := empty.Decide(traceViewFrom([]storage.SpanRow{span(1, 0, 0, 10, "ok")}))
		require.Equal(t, VerdictDrop, d.Verdict)
	})
}

// TestUpweightingRecoversTruePopulation is the phase's explicit statistical
// test: generate a known population, sample it, compute an aggregate with
// and without upweighting, and assert the upweighted estimate lands close to
// the true value while the naive (unweighted) one does not.
func TestUpweightingRecoversTruePopulation(t *testing.T) {
	t.Run("should recover the true count via 1/p upweighting at a known sample rate", func(t *testing.T) {
		const truePopulation = 100_000
		const rate = 0.01

		p := NewProbabilistic(rate)

		var sampledCount int
		var weightedCount float64
		for i := 0; i < truePopulation; i++ {
			var id [16]byte
			id[0], id[1], id[2] = byte(i), byte(i>>8), byte(i>>16)
			d := p.Evaluate(TraceView{TraceID: id})
			if d.Verdict == VerdictSample {
				sampledCount++
				weightedCount += d.Weight()
			}
		}

		unweightedError := math.Abs(float64(sampledCount)-truePopulation) / truePopulation
		weightedError := math.Abs(weightedCount-truePopulation) / truePopulation

		t.Logf("true=%d unweighted_estimate=%d (%.1f%% off) weighted_estimate=%.0f (%.1f%% off)",
			truePopulation, sampledCount, unweightedError*100, weightedCount, weightedError*100)

		require.Greaterf(t, unweightedError, 0.5,
			"the raw sampled count must be wildly wrong at a 1%% sample rate -- this assertion documents WHY upweighting is necessary")
		require.Lessf(t, weightedError, 0.05,
			"the upweighted (1/p) estimate must recover the true population within 5%%, got %.1f%% off", weightedError*100)
	})

	t.Run("should recover a true count from a MIXED population sampled at different rates", func(t *testing.T) {
		// A realistic mixed population: errors always kept (p=1, weight 1),
		// everything else probabilistically at 5% (weight 20). The naive sum
		// of RAW kept traces must undercount the non-error population badly;
		// the weighted sum must recover the true total closely.
		const errorCount = 1_000    // p=1
		const normalCount = 200_000 // p=0.05
		trueTotal := errorCount + normalCount

		errPolicy := NewAlwaysSampleErrors()
		normalPolicy := NewProbabilistic(0.05)
		chain := NewPolicyChain([]Policy{errPolicy, normalPolicy})

		var rawKept int
		var weightedTotal float64

		for i := 0; i < errorCount; i++ {
			s := span(1, 0, 0, 10, "error")
			d := chain.Decide(traceViewFrom([]storage.SpanRow{s}))
			require.Equal(t, VerdictSample, d.Verdict)
			rawKept++
			weightedTotal += d.Weight()
		}
		for i := 0; i < normalCount; i++ {
			var id [16]byte
			id[0], id[1], id[2] = byte(i), byte(i>>8), byte(i>>16)
			d := chain.Decide(TraceView{TraceID: id, Spans: []storage.SpanRow{{StatusCode: "ok"}}})
			if d.Verdict == VerdictSample {
				rawKept++
				weightedTotal += d.Weight()
			}
		}

		weightedError := math.Abs(weightedTotal-float64(trueTotal)) / float64(trueTotal)
		t.Logf("true=%d raw_kept=%d weighted_estimate=%.0f (%.1f%% off)",
			trueTotal, rawKept, weightedTotal, weightedError*100)

		require.Less(t, rawKept, trueTotal/5,
			"raw kept count must badly undercount a mostly-5%%-sampled population")
		require.Less(t, weightedError, 0.05,
			"weighted estimate must recover the true mixed-rate population within 5%%")
	})
}
