package sampling

import (
	"strconv"
	"testing"
	"time"

	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

// BenchmarkAssemblerIngest measures the per-span cost on the assembly hot
// path: Kafka-consume time, once per decoded span.
func BenchmarkAssemblerIngest(b *testing.B) {
	cfg := testBufferConfig(EvictionForcedDecision)
	cfg.DecisionWait = time.Hour // never fires naturally; isolates Ingest's own cost
	cfg.MaxTraces = 1 << 20
	cfg.MaxBytes = 1 << 30

	m := observability.NewMetrics()
	buf := NewBuffer(cfg, m)
	noop := func(spans []storage.SpanRow, d Decision) error { return nil }
	a := NewAssembler(buf, NewPolicyChain([]Policy{NewProbabilistic(0.1)}), m,
		observability.NewLogger("bench"), noop, func(storage.SpanRow) error { return nil })

	// Non-root spans (parent != 0) so early-exit never fires mid-benchmark
	// and skews the measurement toward decision cost instead of Ingest cost.
	spans := make([]storage.SpanRow, b.N)
	for i := range spans {
		spans[i] = spanForTrace(byte(i), 2, 1, 0, 10, "ok")
		spans[i].TraceID[1] = byte(i >> 8)
		spans[i].TraceID[2] = byte(i >> 16)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		a.Ingest(spans[i], 0, int64(i))
	}
}

// BenchmarkBuildTree measures per-decision tree assembly cost at varying
// trace sizes.
func BenchmarkBuildTree(b *testing.B) {
	sizes := []int{1, 10, 50}
	for _, n := range sizes {
		spans := make([]storage.SpanRow, n)
		spans[0] = span(1, 0, 0, 100, "ok")
		for i := 1; i < n; i++ {
			spans[i] = span(byte(i+1), byte(i), i, 10, "ok")
		}

		b.Run(strconv.Itoa(n)+"_spans", func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = BuildTree(spans)
			}
		})
	}
}

// BenchmarkPolicyChainDecide measures per-decision policy evaluation cost
// for a representative chain, under healthy load (rate limiter's cap set
// far above what a tight benchmark loop could exhaust via elapsed wall-clock
// time alone) so every policy is actually exercised -- rate_limiting
// abstains under capacity by design, deferring to attribute_match and
// probabilistic, which is the common case this benchmark should reflect.
func BenchmarkPolicyChainDecide(b *testing.B) {
	chain := NewPolicyChain([]Policy{
		NewAlwaysSampleErrors(),
		NewAlwaysSampleSlowThreshold(500 * time.Millisecond),
		NewRateLimiter(1e9),
		NewAttributeMatch("debug.force_sample", "true"),
		NewProbabilistic(0.05),
	})
	spans := []storage.SpanRow{span(1, 0, 0, 10, "ok")}
	view := traceViewFrom(spans)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = chain.Decide(view)
	}
}

// BenchmarkRateLimiterEvaluate isolates the rate limiter's own per-call cost.
func BenchmarkRateLimiterEvaluate(b *testing.B) {
	p := NewRateLimiter(1e9)
	view := traceViewFrom([]storage.SpanRow{span(1, 0, 0, 10, "ok")})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = p.Evaluate(view)
	}
}

// BenchmarkHyperLogLogEstimate measures the fixed-cost register scan.
func BenchmarkHyperLogLogEstimate(b *testing.B) {
	h := NewHyperLogLog()
	for i := 0; i < 100_000; i++ {
		h.Add(strconv.Itoa(i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = h.Estimate()
	}
}

// BenchmarkCardinalityGuardEnforce measures the common (under-budget) path,
// which is what runs on essentially every attribute occurrence in practice.
func BenchmarkCardinalityGuardEnforce(b *testing.B) {
	m := observability.NewMetrics()
	g := NewCardinalityGuard(CardinalityConfig{
		Default: KeyBudget{Budget: 1_000_000, Action: ActionBucket},
	}, m)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g.Enforce("http.route", "/checkout")
	}
}

// BenchmarkCardinalityGuardApplyToAttributes measures the per-span-attribute-map cost.
func BenchmarkCardinalityGuardApplyToAttributes(b *testing.B) {
	m := observability.NewMetrics()
	g := NewCardinalityGuard(CardinalityConfig{
		Default: KeyBudget{Budget: 1_000_000, Action: ActionBucket},
	}, m)
	attrs := map[string]string{
		"http.method": "GET", "http.route": "/checkout", "http.status_code": "200",
		"net.peer.name": "checkout-svc", "service.version": "1.0.0",
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = g.ApplyToAttributes(attrs)
	}
}
