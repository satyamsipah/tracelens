package sampling

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/storage"
)

func TestBuildPolicyChainAllTypes(t *testing.T) {
	cfg := PolicyFileConfig{Policies: []PolicyEntry{
		{Type: "always_sample_errors"},
		{Type: "always_sample_slow", Mode: "threshold", Threshold: 500 * time.Millisecond},
		{Type: "always_sample_slow", Mode: "rolling_p99", Window: 100},
		{Type: "rate_limiting", RatePerSecond: 50},
		{Type: "attribute_match", Key: "customer.tier", Value: "enterprise"},
		{Type: "probabilistic", Rate: 0.1},
	}}

	chain, err := BuildPolicyChain(cfg)
	require.NoError(t, err)

	// Exercise it: an error trace must be sampled by the first policy.
	errSpan := span(1, 0, 0, 10, "error")
	d := chain.Decide(traceViewFrom([]storage.SpanRow{errSpan}))
	require.Equal(t, VerdictSample, d.Verdict)
	require.Equal(t, "always_sample_errors", d.PolicyName)
}

func TestBuildPolicyChainRejectsUnknownType(t *testing.T) {
	t.Run("should error rather than silently skip an unrecognized policy type", func(t *testing.T) {
		_, err := BuildPolicyChain(PolicyFileConfig{Policies: []PolicyEntry{{Type: "does_not_exist"}}})
		require.Error(t, err)
	})
}

func TestBuildPolicyChainValidation(t *testing.T) {
	tests := []struct {
		name  string
		entry PolicyEntry
	}{
		{"threshold mode needs a positive threshold", PolicyEntry{Type: "always_sample_slow", Mode: "threshold", Threshold: 0}},
		{"unknown slow mode", PolicyEntry{Type: "always_sample_slow", Mode: "bogus"}},
		{"rate_limiting needs a positive rate", PolicyEntry{Type: "rate_limiting", RatePerSecond: 0}},
		{"attribute_match needs a key", PolicyEntry{Type: "attribute_match", Key: ""}},
		{"probabilistic rate must be in [0,1]", PolicyEntry{Type: "probabilistic", Rate: 1.5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := BuildPolicyChain(PolicyFileConfig{Policies: []PolicyEntry{tt.entry}})
			require.Error(t, err)
		})
	}
}

func TestLoadPolicyFile(t *testing.T) {
	t.Run("should parse a real YAML file end to end", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "policies.yaml")
		yaml := `
policies:
  - type: always_sample_errors
  - type: always_sample_slow
    mode: threshold
    threshold: 1s
  - type: probabilistic
    rate: 0.05
`
		require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

		chain, err := LoadPolicyFile(path)
		require.NoError(t, err)

		okSpan := span(1, 0, 0, 10, "ok")
		d := chain.Decide(traceViewFrom([]storage.SpanRow{okSpan}))
		require.Equal(t, "probabilistic", d.PolicyName)
	})

	t.Run("should error on a malformed file rather than partially applying it", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "bad.yaml")
		require.NoError(t, os.WriteFile(path, []byte("not: valid: yaml: [["), 0o644))

		_, err := LoadPolicyFile(path)
		require.Error(t, err)
	})
}

func TestBuildCardinalityConfig(t *testing.T) {
	t.Run("should apply defaults for an unset action and bucket count", func(t *testing.T) {
		cfg, err := BuildCardinalityConfig(CardinalityFileConfig{
			Default: KeyBudgetEntry{Budget: 5000},
		})
		require.NoError(t, err)
		require.Equal(t, ActionBucket, cfg.Default.Action)
	})

	t.Run("should reject an unknown action", func(t *testing.T) {
		_, err := BuildCardinalityConfig(CardinalityFileConfig{
			Default: KeyBudgetEntry{Budget: 100, Action: "explode"},
		})
		require.Error(t, err)
	})

	t.Run("should build per-key overrides", func(t *testing.T) {
		cfg, err := BuildCardinalityConfig(CardinalityFileConfig{
			Default: KeyBudgetEntry{Budget: 10000, Action: "keep_and_alert"},
			Overrides: map[string]KeyBudgetEntry{
				"user.id": {Budget: 50, Action: "drop"},
			},
		})
		require.NoError(t, err)
		require.Equal(t, ActionDrop, cfg.Overrides["user.id"].Action)
		require.Equal(t, uint64(50), cfg.Overrides["user.id"].Budget)
	})
}

func TestLoadCardinalityFile(t *testing.T) {
	t.Run("should parse a real YAML file end to end", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "cardinality.yaml")
		yaml := `
default:
  budget: 10000
  action: bucket
  buckets: 256
overrides:
  user.id:
    budget: 100000
    action: drop
  customer.email:
    budget: 5000
    action: bucket
    buckets: 64
`
		require.NoError(t, os.WriteFile(path, []byte(yaml), 0o644))

		cfg, err := LoadCardinalityFile(path)
		require.NoError(t, err)
		require.Equal(t, uint64(10000), cfg.Default.Budget)
		require.Equal(t, ActionDrop, cfg.Overrides["user.id"].Action)
		require.Equal(t, 64, cfg.Overrides["customer.email"].Buckets)
	})
}

// TestDeployedConfigFilesAreValid guards against the demo deployment's actual
// YAML files silently rotting: a typo in either file would otherwise only
// surface as the assembler failing to start in Compose, discovered late.
func TestDeployedConfigFilesAreValid(t *testing.T) {
	t.Run("should parse the deployed policies.yaml", func(t *testing.T) {
		chain, err := LoadPolicyFile("../../deploy/tracelens/policies.yaml")
		require.NoError(t, err)

		// The deployed chain must end in an unconditional catch-all, or
		// ordinary traffic reaching the end of the chain is silently dropped
		// (PolicyChain.Decide treats an all-abstain chain as VerdictDrop).
		okSpan := span(1, 0, 0, 10, "ok")
		d := chain.Decide(traceViewFrom([]storage.SpanRow{okSpan}))
		require.NotEqual(t, "no_policy_matched", d.PolicyName,
			"the deployed chain must not silently fall through with no catch-all policy")
	})

	t.Run("should parse the deployed cardinality.yaml", func(t *testing.T) {
		cfg, err := LoadCardinalityFile("../../deploy/tracelens/cardinality.yaml")
		require.NoError(t, err)
		require.Positive(t, cfg.Default.Budget)
		require.NotEmpty(t, cfg.Overrides)
	})
}

func TestPolicyFileWatcherHotReload(t *testing.T) {
	t.Run("should swap the assembler's chain when the file changes", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "policies.yaml")
		require.NoError(t, os.WriteFile(path, []byte("policies:\n  - type: probabilistic\n    rate: 0.0\n"), 0o644))

		initial, err := LoadPolicyFile(path)
		require.NoError(t, err)

		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = time.Hour
		a, capture, _ := newTestAssembler(t, cfg, initial)

		watcher := NewPolicyFileWatcher(path, 10*time.Millisecond, a, observability.NewLogger("test"))
		stop := make(chan struct{})
		go watcher.Run(stop)
		defer close(stop)

		// Rewrite the file with a different rate; must NOT be picked up
		// before the poll interval, and MUST be picked up after.
		require.NoError(t, os.WriteFile(path, []byte("policies:\n  - type: probabilistic\n    rate: 1.0\n"), 0o644))

		// A FRESH trace id each attempt: re-ingesting the SAME trace id would
		// hit the late-span path (already decided) on every retry after the
		// first, never re-running the policy chain against whatever is
		// currently loaded.
		var attempt byte
		require.Eventually(t, func() bool {
			attempt++
			root := spanForTrace(attempt, 1, 0, 0, 10, "ok")
			a.Ingest(root, 0, int64(attempt))
			snap := capture.snapshot()
			return len(snap) > 0 && snap[len(snap)-1].decision.Verdict == VerdictSample
		}, 2*time.Second, 20*time.Millisecond, "the reloaded rate=1.0 chain must eventually take effect")
	})

	t.Run("should keep the previous chain when a reload is malformed", func(t *testing.T) {
		dir := t.TempDir()
		path := filepath.Join(dir, "policies.yaml")
		require.NoError(t, os.WriteFile(path, []byte("policies:\n  - type: probabilistic\n    rate: 1.0\n"), 0o644))

		initial, err := LoadPolicyFile(path)
		require.NoError(t, err)

		cfg := testBufferConfig(EvictionForcedDecision)
		cfg.DecisionWait = time.Hour
		a, capture, _ := newTestAssembler(t, cfg, initial)

		watcher := NewPolicyFileWatcher(path, 10*time.Millisecond, a, observability.NewLogger("test"))
		watcher.checkAndReload() // prime lastMod without waiting on Run's ticker

		require.NoError(t, os.WriteFile(path, []byte("this is not valid yaml: [["), 0o644))
		time.Sleep(30 * time.Millisecond)
		watcher.checkAndReload()

		root := spanForTrace(1, 1, 0, 0, 10, "ok")
		a.Ingest(root, 0, 0)
		require.Equal(t, VerdictSample, capture.snapshot()[0].decision.Verdict,
			"a malformed reload must leave the previous (rate=1.0) chain active")
	})
}
