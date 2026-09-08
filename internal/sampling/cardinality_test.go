package sampling

import (
	"fmt"
	"math"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/observability"
)

func TestHyperLogLogAccuracy(t *testing.T) {
	tests := []struct {
		name      string
		trueCount int
		tolerance float64 // fraction of trueCount
	}{
		{"should estimate accurately at small cardinality", 100, 0.10},
		{"should estimate accurately at medium cardinality", 10_000, 0.05},
		{"should estimate accurately at large cardinality", 1_000_000, 0.03},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := NewHyperLogLog()
			for i := 0; i < tt.trueCount; i++ {
				h.Add(fmt.Sprintf("value-%d", i))
			}

			estimate := h.Estimate()
			deviation := math.Abs(estimate-float64(tt.trueCount)) / float64(tt.trueCount)

			require.Lessf(t, deviation, tt.tolerance,
				"true=%d estimate=%.0f deviation=%.4f exceeds tolerance %.4f",
				tt.trueCount, estimate, deviation, tt.tolerance)
		})
	}
}

func TestHyperLogLogRepeatedValuesDoNotInflate(t *testing.T) {
	t.Run("should not grow the estimate when the same value repeats", func(t *testing.T) {
		h := NewHyperLogLog()
		for i := 0; i < 100_000; i++ {
			h.Add("only-one-distinct-value")
		}
		require.Less(t, h.Estimate(), 5.0,
			"a single repeated value must estimate close to 1, not scale with observation count")
	})
}

func TestHyperLogLogMerge(t *testing.T) {
	t.Run("should combine two sketches losslessly for the union of their values", func(t *testing.T) {
		a := NewHyperLogLog()
		b := NewHyperLogLog()
		for i := 0; i < 5000; i++ {
			a.Add(fmt.Sprintf("a-%d", i))
		}
		for i := 0; i < 5000; i++ {
			b.Add(fmt.Sprintf("b-%d", i))
		}
		a.Merge(b)

		deviation := math.Abs(a.Estimate()-10000) / 10000
		require.Less(t, deviation, 0.05)
	})
}

func TestCardinalityTracker(t *testing.T) {
	t.Run("should track independent estimates per key", func(t *testing.T) {
		tr := NewCardinalityTracker()
		for i := 0; i < 1000; i++ {
			tr.Observe("low_card_key", "constant")
			tr.Observe("high_card_key", fmt.Sprintf("v-%d", i))
		}

		require.Less(t, tr.Estimate("low_card_key"), 5.0)
		require.InDelta(t, 1000, tr.Estimate("high_card_key"), 100)
	})

	t.Run("should return zero for a key never observed", func(t *testing.T) {
		tr := NewCardinalityTracker()
		require.Equal(t, float64(0), tr.Estimate("never_seen"))
	})
}

func TestCardinalityGuardActions(t *testing.T) {
	tests := []struct {
		name       string
		action     CardinalityAction
		wantKeep   bool
		checkValue func(t *testing.T, original, got string)
	}{
		{
			name:     "should drop the key entirely when action is drop",
			action:   ActionDrop,
			wantKeep: false,
			checkValue: func(t *testing.T, original, got string) {
				require.Empty(t, got)
			},
		},
		{
			name:     "should replace the value with a stable bucket when action is bucket",
			action:   ActionBucket,
			wantKeep: true,
			checkValue: func(t *testing.T, original, got string) {
				require.NotEqual(t, original, got)
				require.Contains(t, got, "bucket_")
			},
		},
		{
			name:     "should keep the value unchanged when action is keep_and_alert",
			action:   ActionKeepAndAlert,
			wantKeep: true,
			checkValue: func(t *testing.T, original, got string) {
				require.Equal(t, original, got)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := observability.NewMetrics()
			g := NewCardinalityGuard(CardinalityConfig{
				Default: KeyBudget{Budget: 10, Action: tt.action},
			}, m)

			// Breach injection: push the key well past its budget of 10.
			var lastValue string
			for i := 0; i < 500; i++ {
				lastValue = fmt.Sprintf("user-%d", i)
				g.Enforce("user.id", lastValue)
			}

			outValue, keep := g.Enforce("user.id", "user-final")
			require.Equal(t, tt.wantKeep, keep)
			tt.checkValue(t, "user-final", outValue)

			require.Positive(t, testutil.ToFloat64(m.CardinalityBreaches.WithLabelValues("user.id")),
				"a sustained breach must be counted")
		})
	}
}

func TestCardinalityGuardBucketIsDeterministic(t *testing.T) {
	t.Run("should map the same value to the same bucket every time", func(t *testing.T) {
		m := observability.NewMetrics()
		g := NewCardinalityGuard(CardinalityConfig{
			Default: KeyBudget{Budget: 1, Action: ActionBucket, Buckets: 16},
		}, m)

		g.Enforce("key", "seed") // push past budget of 1

		first, _ := g.Enforce("key", "repeated-value")
		second, _ := g.Enforce("key", "repeated-value")
		require.Equal(t, first, second)
	})

	t.Run("should stay within the configured bucket count", func(t *testing.T) {
		m := observability.NewMetrics()
		g := NewCardinalityGuard(CardinalityConfig{
			Default: KeyBudget{Budget: 1, Action: ActionBucket, Buckets: 4},
		}, m)
		g.Enforce("key", "seed")

		seen := map[string]bool{}
		for i := 0; i < 200; i++ {
			v, _ := g.Enforce("key", fmt.Sprintf("v-%d", i))
			seen[v] = true
		}
		require.LessOrEqualf(t, len(seen), 4, "bucketed values must not exceed the configured bucket count, got %v", seen)
	})
}

func TestCardinalityGuardUnderBudget(t *testing.T) {
	t.Run("should pass values through unchanged while under budget", func(t *testing.T) {
		m := observability.NewMetrics()
		g := NewCardinalityGuard(CardinalityConfig{
			Default: KeyBudget{Budget: 10000, Action: ActionDrop},
		}, m)

		v, keep := g.Enforce("service.name", "checkout")
		require.True(t, keep)
		require.Equal(t, "checkout", v)
		require.Equal(t, float64(0), testutil.ToFloat64(m.CardinalityBreaches.WithLabelValues("service.name")))
	})
}

func TestCardinalityGuardApplyToAttributes(t *testing.T) {
	t.Run("should drop only the breaching key, leaving other attributes untouched", func(t *testing.T) {
		m := observability.NewMetrics()
		g := NewCardinalityGuard(CardinalityConfig{
			Default: KeyBudget{Budget: 10000, Action: ActionKeepAndAlert},
			Overrides: map[string]KeyBudget{
				"user.id": {Budget: 3, Action: ActionDrop},
			},
		}, m)

		for i := 0; i < 20; i++ {
			g.ApplyToAttributes(map[string]string{"user.id": fmt.Sprintf("u%d", i)})
		}

		out := g.ApplyToAttributes(map[string]string{
			"user.id":      "u999",
			"service.name": "checkout",
		})
		_, hasUserID := out["user.id"]
		require.False(t, hasUserID, "the breaching key must be dropped")
		require.Equal(t, "checkout", out["service.name"], "a non-breaching key must survive untouched")
	})

	t.Run("should return the input unchanged for an empty map", func(t *testing.T) {
		m := observability.NewMetrics()
		g := NewCardinalityGuard(CardinalityConfig{Default: KeyBudget{Budget: 100, Action: ActionDrop}}, m)
		require.Empty(t, g.ApplyToAttributes(nil))
		require.Empty(t, g.ApplyToAttributes(map[string]string{}))
	})
}

func TestCardinalityGuardPerKeyOverride(t *testing.T) {
	t.Run("should apply a key-specific budget and action over the default", func(t *testing.T) {
		m := observability.NewMetrics()
		g := NewCardinalityGuard(CardinalityConfig{
			Default: KeyBudget{Budget: 100000, Action: ActionKeepAndAlert},
			Overrides: map[string]KeyBudget{
				"customer.email": {Budget: 5, Action: ActionDrop},
			},
		}, m)

		for i := 0; i < 200; i++ {
			g.Enforce("customer.email", fmt.Sprintf("user%d@example.com", i))
		}
		_, keep := g.Enforce("customer.email", "final@example.com")
		require.False(t, keep, "override budget of 5 must have been breached and dropped")

		v, keep := g.Enforce("service.name", "checkout")
		require.True(t, keep)
		require.Equal(t, "checkout", v, "keys with no override must use the default action")
	})
}
