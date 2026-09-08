package logs

import (
	"fmt"
	"testing"
)

// BenchmarkDrainParseSteadyState measures the per-log-line cost once a
// template is already known -- the common case in steady-state operation.
func BenchmarkDrainParseSteadyState(b *testing.B) {
	tree := NewTree(testConfig(), nil)
	tree.Parse("user 1 logged in from 10.0.0.1") // seed the cluster once

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Parse(fmt.Sprintf("user %d logged in from 10.0.0.%d", i, i%255))
	}
}

// BenchmarkDrainParseNewCluster measures the cost of a genuinely new shape
// every call -- descend, compare against nothing, create.
func BenchmarkDrainParseNewCluster(b *testing.B) {
	tree := NewTree(testConfig(), nil)

	lines := make([]string, b.N)
	for i := range lines {
		lines[i] = fmt.Sprintf("distinct_shape_%d has its own unique words %d %d", i, i, i*7)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Parse(lines[i])
	}
}

// BenchmarkDrainParseManyClustersAtOneLeaf stresses findOrCreate's per-leaf
// linear scan WITHOUT the per-leaf cap (MaxClustersPerLeaf: 0 = disabled,
// matching testConfig()'s default), to keep this as the documented "before"
// measurement. All these lines share the same length and leading tokens (so
// they land at the same leaf) but differ enough in content to each become
// their own cluster.
func BenchmarkDrainParseManyClustersAtOneLeaf(b *testing.B) {
	cfg := testConfig()
	cfg.MaxTemplates = 5000
	tree := NewTree(cfg, nil)

	for i := 0; i < 2000; i++ {
		tree.Parse(fmt.Sprintf("connection reset by peer_%d during_%d handshake_%d", i, i, i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Re-parsing an existing shape still must scan the whole leaf's
		// cluster list to find its match.
		tree.Parse(fmt.Sprintf("connection reset by peer_%d during_%d handshake_%d", i%2000, i%2000, i%2000))
	}
}

// BenchmarkDrainParseManyClustersAtOneLeafCapped is the "after": identical
// setup, but with MaxClustersPerLeaf bounding the same leaf's cluster list.
func BenchmarkDrainParseManyClustersAtOneLeafCapped(b *testing.B) {
	cfg := testConfig()
	cfg.MaxTemplates = 5000
	cfg.MaxClustersPerLeaf = 200
	tree := NewTree(cfg, nil)

	for i := 0; i < 2000; i++ {
		tree.Parse(fmt.Sprintf("connection reset by peer_%d during_%d handshake_%d", i, i, i))
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tree.Parse(fmt.Sprintf("connection reset by peer_%d during_%d handshake_%d", i%2000, i%2000, i%2000))
	}
}
