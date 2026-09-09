package sampling

import (
	"hash/fnv"
	"sync"
	"time"

	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

// Verdict is what one policy returns for one trace.
type Verdict int

const (
	// VerdictAbstain means this policy has no opinion; the chain continues
	// to the next policy. attribute_match abstains on no-match; rate_limiting
	// abstains while capacity is available, deferring the actual decision to
	// whatever comes next, and only resolves (Drop) once truly exhausted.
	VerdictAbstain Verdict = iota
	VerdictSample
	VerdictDrop
)

func (v Verdict) String() string {
	switch v {
	case VerdictSample:
		return "sample"
	case VerdictDrop:
		return "drop"
	default:
		return "abstain"
	}
}

// TraceView is what a policy sees: everything about an assembled trace
// needed to decide, without exposing buffer-internal bookkeeping.
type TraceView struct {
	TraceID  [16]byte
	Spans    []storage.SpanRow
	Tree     *AssembledTree
	Duration time.Duration
}

// Decision is one policy's verdict, plus how confident it is expressed as a
// probability -- p=1 for anything deterministic (always_sample_errors,
// always_sample_slow, a matching attribute_match), the configured rate for
// probabilistic, and the empirically observed admit ratio for rate_limiting.
// Weight (1/p) is derived from this, never computed ad hoc at the call site.
type Decision struct {
	Verdict     Verdict
	Probability float64
	PolicyName  string
}

// Weight is 1/p, the multiplier storage.SpanRow.SamplingWeight carries so
// aggregates over sampled data can upweight correctly (CLAUDE.md principle
// 6). A p of 1 (deterministic keep) yields weight 1: a trace you always keep
// isn't a sample of a larger population, it IS the population, and
// upweighting it would overstate reality.
func (d Decision) Weight() float64 {
	if d.Probability <= 0 {
		return 1
	}
	return 1 / d.Probability
}

// Policy is one link in the sampling chain.
type Policy interface {
	Name() string
	Evaluate(view TraceView) Decision
}

// PolicyChain evaluates policies in order; the first non-abstain verdict
// wins. An entirely-abstaining chain is treated as VerdictDrop -- silently
// keeping everything nobody had an opinion about would make "no matching
// policy" a hidden 100% sample rate, which is the opposite of principle 4/6's
// intent. A real deployment should always end its chain with an explicit
// catch-all (typically probabilistic) rather than relying on this fallback.
type PolicyChain struct {
	policies []Policy
}

// NewPolicyChain builds a chain from an ordered policy list.
func NewPolicyChain(policies []Policy) *PolicyChain {
	return &PolicyChain{policies: policies}
}

// Decide walks the chain and returns the first non-abstaining decision.
func (c *PolicyChain) Decide(view TraceView) Decision {
	for _, p := range c.policies {
		d := p.Evaluate(view)
		if d.Verdict != VerdictAbstain {
			d.PolicyName = p.Name()
			return d
		}
	}
	return Decision{Verdict: VerdictDrop, Probability: 1, PolicyName: "no_policy_matched"}
}

// --- always_sample_errors ---------------------------------------------------

type alwaysSampleErrors struct{}

// NewAlwaysSampleErrors keeps every trace containing an ERROR-status span,
// with certainty (p=1): an error you always keep is not a sample of the
// error population, it IS the error population.
func NewAlwaysSampleErrors() Policy { return alwaysSampleErrors{} }

func (alwaysSampleErrors) Name() string { return "always_sample_errors" }

func (alwaysSampleErrors) Evaluate(view TraceView) Decision {
	if HasError(view.Spans) {
		return Decision{Verdict: VerdictSample, Probability: 1}
	}
	return Decision{Verdict: VerdictAbstain}
}

// --- always_sample_slow ------------------------------------------------------

// SlowMode selects between a fixed threshold and an adaptive rolling p99.
type SlowMode int

const (
	SlowModeThreshold SlowMode = iota
	SlowModeRollingP99
)

type alwaysSampleSlow struct {
	mode      SlowMode
	threshold time.Duration
	p99       *rollingP99
}

// NewAlwaysSampleSlowThreshold keeps every trace slower than a fixed duration.
func NewAlwaysSampleSlowThreshold(threshold time.Duration) Policy {
	return &alwaysSampleSlow{mode: SlowModeThreshold, threshold: threshold}
}

// NewAlwaysSampleSlowRollingP99 keeps every trace slower than an adaptively
// tracked p99 duration, learned from traces this policy has seen.
func NewAlwaysSampleSlowRollingP99(windowSize int) Policy {
	return &alwaysSampleSlow{mode: SlowModeRollingP99, p99: newRollingP99(windowSize)}
}

func (p *alwaysSampleSlow) Name() string { return "always_sample_slow" }

func (p *alwaysSampleSlow) Evaluate(view TraceView) Decision {
	if p.mode == SlowModeThreshold {
		if view.Duration > p.threshold {
			return Decision{Verdict: VerdictSample, Probability: 1}
		}
		return Decision{Verdict: VerdictAbstain}
	}

	threshold := p.p99.Estimate()
	p.p99.Observe(view.Duration)
	if threshold > 0 && view.Duration > threshold {
		return Decision{Verdict: VerdictSample, Probability: 1}
	}
	return Decision{Verdict: VerdictAbstain}
}

// rollingP99 estimates a p99 duration from a fixed-size window of recent
// observations. A sorted-insert ring buffer is adequate here: this is
// decision-time-per-trace granularity (not per-span), and window sizes are
// in the hundreds, not millions.
type rollingP99 struct {
	mu     sync.Mutex
	window []time.Duration
	size   int
	pos    int
	filled bool
}

func newRollingP99(size int) *rollingP99 {
	if size <= 0 {
		size = 200
	}
	return &rollingP99{window: make([]time.Duration, size), size: size}
}

func (r *rollingP99) Observe(d time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.window[r.pos] = d
	r.pos = (r.pos + 1) % r.size
	if r.pos == 0 {
		r.filled = true
	}
}

func (r *rollingP99) Estimate() time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()

	n := r.pos
	if r.filled {
		n = r.size
	}
	if n == 0 {
		return 0
	}

	sorted := append([]time.Duration(nil), r.window[:n]...)
	for i := 1; i < len(sorted); i++ {
		for j := i; j > 0 && sorted[j-1] > sorted[j]; j-- {
			sorted[j-1], sorted[j] = sorted[j], sorted[j-1]
		}
	}
	idx := (n * 99) / 100
	if idx >= n {
		idx = n - 1
	}
	return sorted[idx]
}

// --- probabilistic -----------------------------------------------------------

type probabilistic struct {
	rate float64
}

// NewProbabilistic samples a fixed fraction of traces, deterministically by
// trace_id: the same trace_id always gets the same verdict, which matters
// for idempotent reprocessing (a replayed batch must not flip a trace from
// sampled to dropped or vice versa, which would corrupt weighted aggregates).
func NewProbabilistic(rate float64) Policy {
	return probabilistic{rate: rate}
}

func (p probabilistic) Name() string { return "probabilistic" }

func (p probabilistic) Evaluate(view TraceView) Decision {
	// The boundaries are handled BEFORE any float->uint64 conversion, and
	// that is load bearing rather than defensive tidiness.
	//
	// Converting an out-of-range float to an integer is explicitly
	// implementation-dependent in Go. float64 cannot represent 2^64-1, so
	// `rate * float64(^uint64(0))` at rate=1.0 produces exactly 2^64 -- out
	// of range -- and the conversion then yields 2^64-1 on arm64 but 2^63 on
	// amd64. The previous code did exactly that: "sample everything" behaved
	// as 100% on an arm64 dev machine and as 50% on linux/amd64, while still
	// recording Probability 1.0, so every weighted aggregate over that data
	// would have been silently wrong by a factor of two (principle 6).
	//
	// Found because CI on linux/amd64 failed a test that passed locally.
	if p.rate >= 1 {
		return Decision{Verdict: VerdictSample, Probability: 1}
	}
	if p.rate <= 0 {
		return Decision{Verdict: VerdictDrop, Probability: 1}
	}

	// A distinct salt from the Kafka partition-key hash (which hashes the raw
	// trace_id bytes directly, unsalted) so this decision cannot correlate
	// with partition assignment even coincidentally.
	h := fnv.New64a()
	_, _ = h.Write([]byte("sampling-decision:"))
	_, _ = h.Write(view.TraceID[:])
	score := splitmix64(h.Sum64())

	// rate is strictly inside (0, 1) here, so the product is strictly below
	// 2^64 and the conversion is always in range on every architecture.
	threshold := uint64(p.rate * float64(1<<64))
	if score < threshold {
		return Decision{Verdict: VerdictSample, Probability: p.rate}
	}
	return Decision{Verdict: VerdictDrop, Probability: 1 - p.rate}
}

// --- attribute_match ---------------------------------------------------------

type attributeMatch struct {
	key   string
	value string
}

// NewAttributeMatch keeps, with certainty, any trace where at least one span
// carries the given resource or span attribute key=value. Abstains
// otherwise, so it composes as an allowlist ahead of a baseline policy
// rather than a hard gate.
func NewAttributeMatch(key, value string) Policy {
	return attributeMatch{key: key, value: value}
}

func (a attributeMatch) Name() string { return "attribute_match" }

func (a attributeMatch) Evaluate(view TraceView) Decision {
	for i := range view.Spans {
		if v, ok := view.Spans[i].SpanAttributes[a.key]; ok && v == a.value {
			return Decision{Verdict: VerdictSample, Probability: 1}
		}
		if v, ok := view.Spans[i].ResourceAttributes[a.key]; ok && v == a.value {
			return Decision{Verdict: VerdictSample, Probability: 1}
		}
	}
	return Decision{Verdict: VerdictAbstain}
}

// --- rate_limiting -----------------------------------------------------------

// rateLimiter is a token bucket per service.
//
// MEASURED DESIGN FIX: Evaluate originally always resolved (Sample or Drop,
// reporting an empirical admit-ratio as its probability either way). That
// made it mutually exclusive with `probabilistic` in a composed chain --
// PolicyChain.Decide is first-non-abstain-wins, and probabilistic ALSO
// always resolves, so whichever of the two came first in the configured
// list made the other completely unreachable regardless of ordering. Now,
// Evaluate ABSTAINS while capacity is available, deferring the actual
// keep/drop decision (and its weight) to whatever policy comes after it in
// the chain, and only actively VETOES -- with certainty, Probability=1 --
// once the bucket is genuinely exhausted. That makes it compose correctly
// as a capacity guard ahead of a baseline policy, which is the role a rate
// limiter is actually meant to play. The accepted imprecision: a trace that
// passes through while capacity is available is weighted purely by
// whatever policy resolves it downstream, not adjusted for the (normally
// small, under healthy load) probability that capacity could have been
// exhausted -- computing that joint probability correctly across an
// arbitrary chain is a harder problem this does not attempt to solve.
//
// maxTrackedServices bounds rateLimiter.buckets. Every other stateful map in
// this codebase (Buffer.inflight/decided, Drain's clusters, the cardinality
// tracker) has an explicit cap plus LRU eviction plus a counter -- this map
// previously had none, growing once per distinct service.name ever seen,
// forever. If service.name ever carries per-tenant/per-pod/per-instance
// identifiers -- precisely the anti-pattern CardinalityGuard exists
// elsewhere to police -- that was an unbounded, silent memory leak with zero
// visibility. Bounded and counted the same way now.
const maxTrackedServices = 10_000

type rateLimiter struct {
	mu      sync.Mutex
	buckets map[string]*bucketState
	perSec  float64
	nextGen uint64
	m       *observability.Metrics
}

type bucketState struct {
	tokens   float64
	lastFill time.Time
	lruGen   uint64
}

// NewRateLimiter caps admitted traces per service to ratePerSecond: a
// capacity guard that abstains while under the cap (deferring to whatever
// policy comes next) and vetoes with certainty once exhausted.
func NewRateLimiter(ratePerSecond float64) Policy {
	return &rateLimiter{buckets: map[string]*bucketState{}, perSec: ratePerSecond}
}

// NewRateLimiterWithMetrics is NewRateLimiter plus wiring for the
// tracked-service gauge and eviction counter.
func NewRateLimiterWithMetrics(ratePerSecond float64, m *observability.Metrics) Policy {
	return &rateLimiter{buckets: map[string]*bucketState{}, perSec: ratePerSecond, m: m}
}

func (r *rateLimiter) Name() string { return "rate_limiting" }

func (r *rateLimiter) Evaluate(view TraceView) Decision {
	service := "unknown_service"
	if len(view.Spans) > 0 {
		service = view.Spans[0].ServiceName
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	b, ok := r.buckets[service]
	if !ok {
		if len(r.buckets) >= maxTrackedServices {
			r.evictLRULocked()
		}
		b = &bucketState{tokens: r.perSec, lastFill: time.Now()}
		r.buckets[service] = b
		if r.m != nil {
			r.m.RateLimiterServicesTracked.Set(float64(len(r.buckets)))
		}
	}
	r.nextGen++
	b.lruGen = r.nextGen

	now := time.Now()
	elapsed := now.Sub(b.lastFill).Seconds()
	b.tokens += elapsed * r.perSec
	if b.tokens > r.perSec {
		b.tokens = r.perSec
	}
	b.lastFill = now

	if b.tokens >= 1 {
		// Capacity available: consume a token but ABSTAIN, deferring the
		// actual keep/drop decision (and its weight) to whatever policy
		// comes next in the chain. See the type doc for why this must not
		// resolve here.
		b.tokens--
		return Decision{Verdict: VerdictAbstain}
	}

	// Exhausted: a hard, certain veto -- this is the one case rate_limiting
	// actively decides, and it always means Drop.
	return Decision{Verdict: VerdictDrop, Probability: 1}
}

// evictLRULocked removes the least-recently-evaluated service's bucket.
// Caller must hold r.mu. Losing a bucket only resets that service's rate
// estimate back to a fresh full bucket on its next trace -- a brief,
// bounded accuracy cost, not a correctness one, and vastly preferable to
// unbounded growth.
func (r *rateLimiter) evictLRULocked() {
	var oldestKey string
	oldestGen := ^uint64(0)
	for k, b := range r.buckets {
		if b.lruGen < oldestGen {
			oldestGen, oldestKey = b.lruGen, k
		}
	}
	if oldestKey == "" {
		return
	}
	delete(r.buckets, oldestKey)
	if r.m != nil {
		r.m.RateLimiterServicesEvictedTotal.Inc()
	}
}
