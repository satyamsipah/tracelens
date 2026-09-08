package logs

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func testConfig() Config {
	return Config{
		Depth:               4,
		SimilarityThreshold: 0.6,
		MaxChildren:         100,
		MaxTemplates:        10_000,
	}
}

// knownCorpus has an EXACTLY KNOWN template count: 5 distinct shapes, each
// instantiated many times with different variable parts. This is the
// "known-template-count corpus, assert recovery" test the phase asked for.
func knownCorpus() []string {
	var lines []string
	for i := 0; i < 50; i++ {
		lines = append(lines,
			fmt.Sprintf("user %d logged in from %s", i, "10.0.0."+fmt.Sprint(i%255)),
			fmt.Sprintf("order %d shipped to warehouse %d", i, i%7),
			fmt.Sprintf("payment declined for account %d code %d", i, i%5),
			fmt.Sprintf("cache miss for key session-%d", i),
			"health check ok",
		)
	}
	return lines
}

func TestDrainRecoversKnownTemplateCount(t *testing.T) {
	t.Run("should recover exactly 5 templates from a corpus with 5 known shapes", func(t *testing.T) {
		tree := NewTree(testConfig(), nil)
		for _, line := range knownCorpus() {
			tree.Parse(line)
		}
		require.Equal(t, 5, tree.TemplateCount())
	})

	t.Run("should assign the SAME template id to every instance of one shape", func(t *testing.T) {
		tree := NewTree(testConfig(), nil)
		ids := map[uint32]int{}
		for i := 0; i < 200; i++ {
			m := tree.Parse(fmt.Sprintf("order %d shipped to warehouse %d", i, i%7))
			ids[m.TemplateID]++
		}
		require.Len(t, ids, 1, "one shape must never fragment into multiple template ids")
	})

	t.Run("should stabilize template count as corpus volume grows, not grow with it", func(t *testing.T) {
		tree := NewTree(testConfig(), nil)
		for i := 0; i < 50; i++ {
			tree.Parse(fmt.Sprintf("user %d logged in from 10.0.0.%d", i, i%255))
		}
		after50 := tree.TemplateCount()

		for i := 50; i < 5000; i++ {
			tree.Parse(fmt.Sprintf("user %d logged in from 10.0.0.%d", i, i%255))
		}
		after5000 := tree.TemplateCount()

		require.Equal(t, after50, after5000,
			"template count must be a function of log SHAPE diversity, not log VOLUME")
	})
}

func TestDrainParamExtraction(t *testing.T) {
	t.Run("should extract exactly the variable positions as params", func(t *testing.T) {
		tree := NewTree(testConfig(), nil)

		tree.Parse("user 1 logged in from 10.0.0.1")
		m := tree.Parse("user 2 logged in from 10.0.0.2")

		require.Equal(t, []string{"2", "10.0.0.2"}, m.Params)
		require.Equal(t, []string{"user", "<*>", "logged", "in", "from", "<*>"}, m.Template)
	})

	t.Run("should report no params for a template with no variable positions", func(t *testing.T) {
		tree := NewTree(testConfig(), nil)
		tree.Parse("health check ok")
		m := tree.Parse("health check ok")
		require.Empty(t, m.Params)
	})
}

func TestDrainNumericEagerWildcard(t *testing.T) {
	t.Run("should route a numeric-looking leading token to a wildcard child during descent", func(t *testing.T) {
		tree := NewTree(testConfig(), nil)

		// Two genuinely different template SHAPES that happen to share a
		// numeric first token. Eager wildcarding must not conflate a numeric
		// value with tree structure -- the two must still end up as two
		// distinct clusters (matched by their own similarity), not forced
		// together purely because both descended through the same wildcard
		// branch for token 0.
		a := tree.Parse("404 not found for path /a")
		b := tree.Parse("500 internal error in module x")

		require.NotEqual(t, a.TemplateID, b.TemplateID)
	})

	t.Run("should not fragment the tree across many distinct numeric ids", func(t *testing.T) {
		cfg := testConfig()
		cfg.MaxChildren = 5 // deliberately tiny, to prove numerics never even attempt literal branches
		tree := NewTree(cfg, nil)

		for i := 0; i < 1000; i++ {
			tree.Parse(fmt.Sprintf("%d request completed", i))
		}
		require.Equal(t, 1, tree.TemplateCount(),
			"1000 distinct leading numeric ids must collapse to one template, not exhaust MaxChildren")
	})
}

func TestDrainGeneralizesOverTime(t *testing.T) {
	t.Run("should widen the template to a wildcard once two logs disagree at a position", func(t *testing.T) {
		tree := NewTree(testConfig(), nil)

		first := tree.Parse("retry attempt failed for service checkout")
		require.Equal(t, []string{"retry", "attempt", "failed", "for", "service", "checkout"}, first.Template)

		second := tree.Parse("retry attempt failed for service payments")
		require.Equal(t, first.TemplateID, second.TemplateID)
		require.Equal(t, []string{"retry", "attempt", "failed", "for", "service", "<*>"}, second.Template)

		// Generalization must not un-generalize: a later exact repeat of the
		// FIRST literal value must still see the wildcarded template, with
		// the literal value extracted as the param (params hold the actual
		// token, never the "<*>" marker itself).
		third := tree.Parse("retry attempt failed for service checkout")
		require.Equal(t, first.TemplateID, third.TemplateID)
		require.Equal(t, []string{"checkout"}, third.Params)
	})
}

func TestDrainSimilarityThreshold(t *testing.T) {
	// Tree DESCENT is keyed on the leading tokens literally, not by
	// similarity -- with the test config's depth=4, that consumes tokens[0]
	// and tokens[1]. So the two sentences must share those exact two leading
	// tokens (else they'd branch to different leaves and never even be
	// compared for merge, passing a "must not merge" assertion for the wrong
	// reason) and differ only at LEAF-level-compared positions (2+):
	// ["connection","reset","by","peer","during","handshake"]
	// ["connection","reset","unexpectedly","near","during","timeout"]
	// -> positions 0,1,4 agree, 2,3,5 differ: similarity = 3/6 = 0.5.
	const a = "connection reset by peer during handshake"
	const b = "connection reset unexpectedly near during timeout"

	t.Run("should start a new cluster when similarity falls below threshold", func(t *testing.T) {
		cfg := testConfig()
		cfg.SimilarityThreshold = 0.9 // stricter than the measured 0.5
		tree := NewTree(cfg, nil)

		first := tree.Parse(a)
		second := tree.Parse(b)
		require.NotEqual(t, first.TemplateID, second.TemplateID)
	})

	t.Run("should merge when similarity clears a lenient threshold", func(t *testing.T) {
		cfg := testConfig()
		cfg.SimilarityThreshold = 0.4 // more lenient than the measured 0.5
		tree := NewTree(cfg, nil)

		first := tree.Parse(a)
		second := tree.Parse(b)
		require.Equal(t, first.TemplateID, second.TemplateID)
	})
}

func TestDrainTemplateCap(t *testing.T) {
	t.Run("should evict the least-recently-matched cluster when the cap is reached", func(t *testing.T) {
		cfg := testConfig()
		cfg.MaxTemplates = 3
		tree := NewTree(cfg, nil)

		// Three genuinely distinct shapes, well below the cap.
		a := tree.Parse("alpha shape one")
		_ = tree.Parse("beta shape two")
		_ = tree.Parse("gamma shape three")

		// Keep "alpha" freshly matched so it is NOT the least-recently-used
		// when the fourth distinct shape forces an eviction.
		tree.Parse("alpha shape one")

		fourth := tree.Parse("delta shape four")
		require.True(t, fourth.IsNew)
		require.True(t, fourth.Evicted, "creating the 4th distinct template over a cap of 3 must evict one")
		require.LessOrEqual(t, tree.TemplateCount(), 3)

		// "alpha" was freshly touched, so re-parsing it must still be a hit,
		// not a fresh (evicted-and-recreated) cluster.
		again := tree.Parse("alpha shape one")
		require.Equal(t, a.TemplateID, again.TemplateID)
	})

	t.Run("should never exceed the cap even under sustained new-shape pressure", func(t *testing.T) {
		cfg := testConfig()
		cfg.MaxTemplates = 10
		tree := NewTree(cfg, nil)

		for i := 0; i < 500; i++ {
			// Each line has a unique SHAPE (different token count AND
			// different literal words), guaranteeing a new cluster every time.
			words := make([]string, i%20+1)
			for j := range words {
				words[j] = fmt.Sprintf("w%d_%d", i, j)
			}
			line := ""
			for _, w := range words {
				line += w + " "
			}
			tree.Parse(line)
			require.LessOrEqualf(t, tree.TemplateCount(), 10,
				"template count must never exceed MaxTemplates, even transiently")
		}
	})
}

func TestDrainPerLeafClusterCap(t *testing.T) {
	t.Run("should bound one leaf's cluster count independent of the tree-wide cap", func(t *testing.T) {
		cfg := testConfig()
		cfg.MaxTemplates = 100_000 // effectively unbounded tree-wide, isolates the per-leaf cap
		cfg.MaxClustersPerLeaf = 5
		tree := NewTree(cfg, nil)

		// All land at the SAME leaf: same length, same leading two tokens
		// ("connection","reset"), differing only at positions the leaf-level
		// similarity comparison handles.
		for i := 0; i < 50; i++ {
			tree.Parse(fmt.Sprintf("connection reset by peer_%d during_%d handshake_%d", i, i, i))
		}

		require.LessOrEqual(t, tree.TemplateCount(), 5,
			"one leaf must never exceed MaxClustersPerLeaf, even though MaxTemplates has ample room left")
	})

	t.Run("should evict the least-recently-matched cluster at that leaf, keeping recently-touched ones", func(t *testing.T) {
		cfg := testConfig()
		cfg.MaxTemplates = 100_000
		cfg.MaxClustersPerLeaf = 3
		tree := NewTree(cfg, nil)

		a := tree.Parse("connection reset by peer_A during_A handshake_A")
		_ = tree.Parse("connection reset by peer_B during_B handshake_B")
		_ = tree.Parse("connection reset by peer_C during_C handshake_C")

		// Re-touch "A" so it is NOT the least-recently-matched when the
		// leaf's 4th distinct shape forces an eviction.
		tree.Parse("connection reset by peer_A during_A handshake_A")

		tree.Parse("connection reset by peer_D during_D handshake_D")

		again := tree.Parse("connection reset by peer_A during_A handshake_A")
		require.Equal(t, a.TemplateID, again.TemplateID,
			"a freshly-touched cluster must survive a per-leaf eviction triggered by a new shape")
	})

	t.Run("should not evict at all when MaxClustersPerLeaf is left at zero (disabled)", func(t *testing.T) {
		cfg := testConfig() // MaxClustersPerLeaf defaults to 0
		tree := NewTree(cfg, nil)

		for i := 0; i < 50; i++ {
			tree.Parse(fmt.Sprintf("connection reset by peer_%d during_%d handshake_%d", i, i, i))
		}
		require.Greater(t, tree.TemplateCount(), 5,
			"a zero MaxClustersPerLeaf must mean no per-leaf cap, preserving existing behavior")
	})
}

func TestDrainEmptyLine(t *testing.T) {
	t.Run("should handle an empty log body without panicking", func(t *testing.T) {
		tree := NewTree(testConfig(), nil)
		require.NotPanics(t, func() { tree.Parse("") })
	})
}
