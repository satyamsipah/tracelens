package sampling

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/satyamsipah/tracelens/internal/observability"
)

// PolicyFileConfig is the YAML shape for the sampling policy chain.
//
// Policies are evaluated in the order they appear in the list -- first
// non-abstaining verdict wins (PolicyChain.Decide). A real deployment should
// end its list with an explicit catch-all (typically probabilistic), since an
// entirely-abstaining chain is treated as a drop, not a silent 100% sample.
type PolicyFileConfig struct {
	Policies []PolicyEntry `yaml:"policies"`
}

// PolicyEntry is one policy's YAML entry. Fields not relevant to Type are
// simply ignored, so the same struct covers every policy kind.
type PolicyEntry struct {
	Type string `yaml:"type"`

	// always_sample_slow
	Mode      string        `yaml:"mode"` // "threshold" or "rolling_p99"
	Threshold time.Duration `yaml:"threshold"`
	Window    int           `yaml:"window"`

	// rate_limiting
	RatePerSecond float64 `yaml:"rate_per_second"`

	// attribute_match
	Key   string `yaml:"key"`
	Value string `yaml:"value"`

	// probabilistic
	Rate float64 `yaml:"rate"`
}

// BuildPolicyChain converts parsed YAML into a live PolicyChain. Returns an
// error for any policy type or required field it does not recognize, rather
// than silently skipping it -- a config typo that silently dropped a policy
// would be exactly the kind of quiet failure this project avoids everywhere
// else.
//
// Policies built this way have no metrics wiring (rate_limiting's
// tracked-service gauge/eviction counter stay unset) -- fine for tests,
// which is the overwhelming majority of callers. Use BuildPolicyChainWithMetrics
// for a live deployment.
func BuildPolicyChain(cfg PolicyFileConfig) (*PolicyChain, error) {
	return BuildPolicyChainWithMetrics(cfg, nil)
}

// BuildPolicyChainWithMetrics is BuildPolicyChain with metrics wired into
// every policy that reports one (currently just rate_limiting).
func BuildPolicyChainWithMetrics(cfg PolicyFileConfig, m *observability.Metrics) (*PolicyChain, error) {
	policies := make([]Policy, 0, len(cfg.Policies))

	for i, e := range cfg.Policies {
		p, err := buildPolicy(e, m)
		if err != nil {
			return nil, fmt.Errorf("policy[%d] (type=%q): %w", i, e.Type, err)
		}
		policies = append(policies, p)
	}
	return NewPolicyChain(policies), nil
}

func buildPolicy(e PolicyEntry, m *observability.Metrics) (Policy, error) {
	switch e.Type {
	case "always_sample_errors":
		return NewAlwaysSampleErrors(), nil

	case "always_sample_slow":
		switch e.Mode {
		case "", "threshold":
			if e.Threshold <= 0 {
				return nil, fmt.Errorf("threshold mode requires a positive threshold")
			}
			return NewAlwaysSampleSlowThreshold(e.Threshold), nil
		case "rolling_p99":
			return NewAlwaysSampleSlowRollingP99(e.Window), nil
		default:
			return nil, fmt.Errorf("unknown mode %q, want threshold or rolling_p99", e.Mode)
		}

	case "rate_limiting":
		if e.RatePerSecond <= 0 {
			return nil, fmt.Errorf("rate_per_second must be positive")
		}
		if m != nil {
			return NewRateLimiterWithMetrics(e.RatePerSecond, m), nil
		}
		return NewRateLimiter(e.RatePerSecond), nil

	case "attribute_match":
		if e.Key == "" {
			return nil, fmt.Errorf("attribute_match requires a key")
		}
		return NewAttributeMatch(e.Key, e.Value), nil

	case "probabilistic":
		if e.Rate < 0 || e.Rate > 1 {
			return nil, fmt.Errorf("rate must be in [0,1], got %v", e.Rate)
		}
		return NewProbabilistic(e.Rate), nil

	default:
		return nil, fmt.Errorf("unknown policy type")
	}
}

// LoadPolicyFile reads and parses a policy YAML file.
func LoadPolicyFile(path string) (*PolicyChain, error) {
	return LoadPolicyFileWithMetrics(path, nil)
}

// LoadPolicyFileWithMetrics is LoadPolicyFile with metrics wired into every
// policy that reports one. Used by cmd/assembler; tests use the plain form.
func LoadPolicyFileWithMetrics(path string, m *observability.Metrics) (*PolicyChain, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read policy file: %w", err)
	}
	var cfg PolicyFileConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse policy file: %w", err)
	}
	return BuildPolicyChainWithMetrics(cfg, m)
}

// CardinalityFileConfig is the YAML shape for cardinality budgets.
type CardinalityFileConfig struct {
	Default   KeyBudgetEntry            `yaml:"default"`
	Overrides map[string]KeyBudgetEntry `yaml:"overrides"`
}

// KeyBudgetEntry is one key's budget/action/bucket-count triple in YAML.
type KeyBudgetEntry struct {
	Budget  uint64 `yaml:"budget"`
	Action  string `yaml:"action"` // "drop", "bucket", "keep_and_alert"
	Buckets int    `yaml:"buckets"`
}

func (e KeyBudgetEntry) toKeyBudget() (KeyBudget, error) {
	action := CardinalityAction(e.Action)
	switch action {
	case ActionDrop, ActionBucket, ActionKeepAndAlert:
	case "":
		action = ActionBucket
	default:
		return KeyBudget{}, fmt.Errorf("unknown cardinality action %q", e.Action)
	}
	return KeyBudget{Budget: e.Budget, Action: action, Buckets: e.Buckets}, nil
}

// BuildCardinalityConfig converts parsed YAML into a CardinalityConfig.
func BuildCardinalityConfig(cfg CardinalityFileConfig) (CardinalityConfig, error) {
	def, err := cfg.Default.toKeyBudget()
	if err != nil {
		return CardinalityConfig{}, fmt.Errorf("default: %w", err)
	}
	if def.Budget == 0 {
		def.Budget = 10_000
	}

	overrides := make(map[string]KeyBudget, len(cfg.Overrides))
	for key, e := range cfg.Overrides {
		kb, err := e.toKeyBudget()
		if err != nil {
			return CardinalityConfig{}, fmt.Errorf("override[%s]: %w", key, err)
		}
		overrides[key] = kb
	}
	return CardinalityConfig{Default: def, Overrides: overrides}, nil
}

// LoadCardinalityFile reads and parses a cardinality budget YAML file.
func LoadCardinalityFile(path string) (CardinalityConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return CardinalityConfig{}, fmt.Errorf("read cardinality file: %w", err)
	}
	var cfg CardinalityFileConfig
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return CardinalityConfig{}, fmt.Errorf("parse cardinality file: %w", err)
	}
	return BuildCardinalityConfig(cfg)
}
