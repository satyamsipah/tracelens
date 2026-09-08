package sampling

import (
	"fmt"
	"hash/fnv"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/satyamsipah/tracelens/internal/observability"
)

// CardinalityAction is what happens to an attribute value once its key's
// estimated distinct-value count crosses budget. All three are always
// available; a deployment picks a default and may override per key.
type CardinalityAction string

const (
	// ActionDrop removes the key entirely from the record. Maximum safety,
	// total information loss on that key from then on.
	ActionDrop CardinalityAction = "drop"

	// ActionBucket replaces the value with hash(value) % N. Retains a
	// bounded amount of correlation signal -- the same generalize-rather-
	// than-discard idea Drain uses for its own templates -- at a hard,
	// configurable cardinality ceiling of N buckets.
	ActionBucket CardinalityAction = "bucket"

	// ActionKeepAndAlert leaves the value untouched and only counts the
	// breach. CLAUDE.md's principle 4 names only "dropped or bucketed" as
	// enforcement outcomes, so this is a transitional/diagnostic mode, not a
	// recommended permanent default.
	ActionKeepAndAlert CardinalityAction = "keep_and_alert"
)

// DefaultBucketCount is used when a key's override does not specify one.
const DefaultBucketCount = 256

// KeyBudget is the per-attribute-key cardinality policy.
type KeyBudget struct {
	Budget  uint64
	Action  CardinalityAction
	Buckets int
}

// CardinalityConfig is the full cardinality policy: a default applied to any
// key with no explicit override, plus per-key overrides.
type CardinalityConfig struct {
	Default   KeyBudget
	Overrides map[string]KeyBudget
}

// budgetFor resolves the effective policy for a key.
func (c CardinalityConfig) budgetFor(key string) KeyBudget {
	if kb, ok := c.Overrides[key]; ok {
		if kb.Buckets <= 0 {
			kb.Buckets = DefaultBucketCount
		}
		if kb.Action == "" {
			kb.Action = c.Default.Action
		}
		return kb
	}
	def := c.Default
	if def.Buckets <= 0 {
		def.Buckets = DefaultBucketCount
	}
	return def
}

// CardinalityGuard enforces CardinalityConfig against live attribute
// key/value pairs, backed by one HyperLogLog per key.
//
// Every observation is fed into the sketch regardless of the action taken on
// the OUTPUT value: the sketch's job is to track what is actually arriving,
// independent of what gets stored, so a later budget change has accurate
// history to evaluate against rather than a gap starting from whenever the
// key first breached.
type CardinalityGuard struct {
	cfg      CardinalityConfig
	tracker  *CardinalityTracker
	breach   *prometheus.CounterVec
	estimate *prometheus.GaugeVec
}

// NewCardinalityGuard builds a guard from config, wiring the breach counter,
// the per-key estimate gauge the dashboard's top-offenders panel reads, and
// the tracked-key eviction counter (see maxTrackedKeys).
func NewCardinalityGuard(cfg CardinalityConfig, m *observability.Metrics) *CardinalityGuard {
	return &CardinalityGuard{
		cfg:      cfg,
		tracker:  NewCardinalityTrackerWithMetrics(m.CardinalityKeysEvictedTotal),
		breach:   m.CardinalityBreaches,
		estimate: m.CardinalityEstimate,
	}
}

// Enforce records one occurrence of (key, value) and returns the value to
// actually store, and whether the key should be kept in the record at all.
//
// A false return means: omit this key entirely from the record (ActionDrop
// on a breach). A true return with a modified value means ActionBucket
// rewrote it. A true return with the original value means either the key is
// under budget, or ActionKeepAndAlert is configured for it.
func (g *CardinalityGuard) Enforce(key, value string) (outValue string, keep bool) {
	est := g.tracker.Observe(key, value)
	g.estimate.WithLabelValues(key).Set(est)
	kb := g.cfg.budgetFor(key)

	if kb.Budget == 0 || est <= float64(kb.Budget) {
		return value, true
	}

	g.breach.WithLabelValues(key).Inc()

	switch kb.Action {
	case ActionDrop:
		return "", false
	case ActionBucket:
		return bucketValue(value, kb.Buckets), true
	default: // ActionKeepAndAlert, or unset
		return value, true
	}
}

// Estimate exposes the current distinct-value estimate for a key, for the
// top-offenders dashboard panel.
func (g *CardinalityGuard) Estimate(key string) float64 { return g.tracker.Estimate(key) }

// Keys exposes every tracked attribute key, for the dashboard panel.
func (g *CardinalityGuard) Keys() []string { return g.tracker.Keys() }

// ApplyToAttributes enforces the guard against every key/value pair in attrs
// and returns the (possibly modified, possibly narrower) result.
//
// This is what makes cardinality control an INGEST-time thing rather than a
// query-time one (CLAUDE.md principle 4): called from the decode path,
// before a span or log record ever reaches the trace buffer or storage, so
// a runaway key's cost is capped before it can inflate buffer memory OR
// LowCardinality dictionaries downstream.
func (g *CardinalityGuard) ApplyToAttributes(attrs map[string]string) map[string]string {
	if len(attrs) == 0 {
		return attrs
	}
	out := make(map[string]string, len(attrs))
	for k, v := range attrs {
		if newV, keep := g.Enforce(k, v); keep {
			out[k] = newV
		}
	}
	return out
}

// bucketValue deterministically maps value into one of n buckets. Same value
// in, same bucket out, always -- but two different values in the same bucket
// carry no relationship beyond "happened to hash together".
func bucketValue(value string, n int) string {
	if n <= 0 {
		n = DefaultBucketCount
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(value))
	return fmt.Sprintf("bucket_%d", h.Sum64()%uint64(n))
}
