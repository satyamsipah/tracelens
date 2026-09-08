package sampling

import (
	"crypto/rand"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func randomKey(t *testing.T) []byte {
	t.Helper()
	b := make([]byte, 16)
	_, err := rand.Read(b)
	require.NoError(t, err)
	return b
}

func TestRouterDeterminism(t *testing.T) {
	t.Run("should return the same owner for the same key on repeated calls", func(t *testing.T) {
		r := NewRouter([]string{"a", "b", "c"})
		key := randomKey(t)

		first := r.Owner(key)
		for i := 0; i < 100; i++ {
			require.Equal(t, first, r.Owner(key))
		}
	})

	t.Run("should return the same owner regardless of replica construction order", func(t *testing.T) {
		r1 := NewRouter([]string{"a", "b", "c"})
		r2 := NewRouter([]string{"c", "a", "b"})
		key := randomKey(t)

		require.Equal(t, r1.Owner(key), r2.Owner(key))
	})
}

func TestRouterSpread(t *testing.T) {
	t.Run("should distribute keys roughly evenly across replicas", func(t *testing.T) {
		replicas := []string{"a", "b", "c", "d"}
		r := NewRouter(replicas)

		const n = 40000
		counts := map[string]int{}
		for i := 0; i < n; i++ {
			counts[r.Owner(randomKey(t))]++
		}

		require.Len(t, counts, len(replicas), "every replica must receive some share")

		expected := float64(n) / float64(len(replicas))
		for replica, count := range counts {
			deviation := (float64(count) - expected) / expected
			require.Lessf(t, deviation, 0.10, "replica %s got %d, expected ~%.0f (>10%% high)", replica, count, expected)
			require.Greaterf(t, deviation, -0.10, "replica %s got %d, expected ~%.0f (>10%% low)", replica, count, expected)
		}
	})
}

// TestRouterResizeStability is the test that proves this is actually
// consistent hashing and not a relabelled modulo. Adding one replica to a set
// of N must remap only ~1/(N+1) of previously-assigned keys; naive
// hash(key)%N would remap nearly all of them.
func TestRouterResizeStability(t *testing.T) {
	t.Run("should remap close to the theoretical 1/(N+1) share when a replica is added", func(t *testing.T) {
		before := NewRouter([]string{"a", "b", "c", "d"})
		after := NewRouter([]string{"a", "b", "c", "d", "e"})

		const n = 40000
		keys := make([][]byte, n)
		for i := range keys {
			keys[i] = randomKey(t)
		}

		remapped := 0
		for _, k := range keys {
			if before.Owner(k) != after.Owner(k) {
				remapped++
			}
		}

		fraction := float64(remapped) / float64(n)
		theoretical := 1.0 / 5.0 // 1/(N+1), N=4 -> 5 replicas after

		// Generous tolerance band: this is a statistical property over random
		// keys, not an exact guarantee, but it must land near the theoretical
		// value and nowhere near "nearly everything moved".
		require.InDeltaf(t, theoretical, fraction, 0.05,
			"expected close to %.2f of keys remapped (consistent hashing), got %.2f", theoretical, fraction)
	})

	t.Run("naive modulo remaps nearly everything under the identical resize, for contrast", func(t *testing.T) {
		// hash(key) % N is exactly what Kafka partitioning does (phase 1),
		// and exactly why partition count is fixed forever there: changing N
		// reshuffles almost every key. Demonstrated here on the SAME keys and
		// the SAME resize (4 -> 5) as the Router test above, so the contrast
		// is a direct, apples-to-apples measurement, not an assertion.
		modulo := func(key []byte, n int) int { return int(hrwScore(key, "") % uint64(n)) }

		const n = 40000
		keys := make([][]byte, n)
		for i := range keys {
			keys[i] = randomKey(t)
		}

		remapped := 0
		for _, k := range keys {
			if modulo(k, 4) != modulo(k, 5) {
				remapped++
			}
		}

		fraction := float64(remapped) / float64(n)
		require.Greaterf(t, fraction, 0.70,
			"expected naive modulo to remap the large majority of keys on resize, got only %.2f", fraction)
	})
}

func TestRouterReplicas(t *testing.T) {
	t.Run("should return the sorted replica set", func(t *testing.T) {
		r := NewRouter([]string{"c", "a", "b"})
		require.Equal(t, []string{"a", "b", "c"}, r.Replicas())
	})
}

func TestPartitionKey(t *testing.T) {
	t.Run("should produce distinct keys for distinct partitions", func(t *testing.T) {
		seen := map[string]bool{}
		for p := int32(0); p < 12; p++ {
			k := fmt.Sprintf("%x", PartitionKey(p))
			require.False(t, seen[k], "partition key collision at partition %d", p)
			seen[k] = true
		}
	})
}
