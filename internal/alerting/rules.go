// Package alerting evaluates YAML-configured rules on a schedule and
// notifies a webhook or Slack incoming-webhook on state transitions, with
// dedup (only the transition is notified, not every tick a rule stays
// firing) and a cooldown (a minimum gap between two notifications for the
// same rule, independent of how fast the underlying condition flaps).
package alerting

import (
	"fmt"
	"os"
	"time"

	"gopkg.in/yaml.v3"
)

// RuleType selects how a rule's condition is evaluated.
type RuleType string

const (
	// RuleAnomaly fires from the anomaly detector's own hysteresis-adjusted
	// Firing/StateChanged state -- see internal/anomaly.
	RuleAnomaly RuleType = "anomaly"
	// RuleThreshold is a plain, fixed comparison against a value the caller
	// supplies (e.g. an error rate read straight from a RED rollup), for
	// conditions an SLO defines as an absolute number rather than "unusual
	// relative to history".
	RuleThreshold RuleType = "threshold"
)

// CompareOp is a threshold rule's comparison.
type CompareOp string

const (
	OpGT  CompareOp = ">"
	OpGTE CompareOp = ">="
	OpLT  CompareOp = "<"
	OpLTE CompareOp = "<="
)

// Rule is one alerting rule.
type Rule struct {
	Name      string
	Service   string
	Operation string
	Metric    string

	Type      RuleType
	Threshold float64
	Op        CompareOp

	// Cooldown bounds notification frequency for this rule independent of
	// how often its underlying condition flaps -- dedup (state-change-only
	// notification) already suppresses "still firing" repeats; Cooldown
	// additionally suppresses a fast clear-then-refire cycle from paging
	// twice in quick succession.
	Cooldown time.Duration

	Webhook      string
	SlackWebhook string
}

// Config is the top-level YAML shape.
type Config struct {
	Rules []Rule
}

type ruleFile struct {
	Rules []ruleEntry `yaml:"rules"`
}

type ruleEntry struct {
	Name         string  `yaml:"name"`
	Service      string  `yaml:"service"`
	Operation    string  `yaml:"operation"`
	Metric       string  `yaml:"metric"`
	Type         string  `yaml:"type"`
	Threshold    float64 `yaml:"threshold"`
	Op           string  `yaml:"op"`
	Cooldown     string  `yaml:"cooldown"`
	Webhook      string  `yaml:"webhook"`
	SlackWebhook string  `yaml:"slack_webhook"`
}

// LoadFile reads and validates a rules YAML file.
func LoadFile(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("alerting: read rules file: %w", err)
	}
	return Parse(data)
}

// Parse validates rawYAML and builds a Config. Every field is validated
// explicitly -- an unknown type or op is a load-time error, never a rule
// that silently never fires.
func Parse(rawYAML []byte) (Config, error) {
	var f ruleFile
	if err := yaml.Unmarshal(rawYAML, &f); err != nil {
		return Config{}, fmt.Errorf("alerting: parse rules yaml: %w", err)
	}

	seen := map[string]bool{}
	cfg := Config{Rules: make([]Rule, 0, len(f.Rules))}
	for i, e := range f.Rules {
		if e.Name == "" {
			return Config{}, fmt.Errorf("alerting: rule %d: name is required", i)
		}
		if seen[e.Name] {
			return Config{}, fmt.Errorf("alerting: duplicate rule name %q", e.Name)
		}
		seen[e.Name] = true
		if e.Service == "" {
			return Config{}, fmt.Errorf("alerting: rule %q: service is required", e.Name)
		}
		if e.Metric == "" {
			return Config{}, fmt.Errorf("alerting: rule %q: metric is required", e.Name)
		}
		if e.Webhook == "" && e.SlackWebhook == "" {
			return Config{}, fmt.Errorf("alerting: rule %q: at least one of webhook or slack_webhook is required", e.Name)
		}

		r := Rule{
			Name: e.Name, Service: e.Service, Operation: e.Operation, Metric: e.Metric,
			Webhook: e.Webhook, SlackWebhook: e.SlackWebhook,
		}

		switch RuleType(e.Type) {
		case RuleAnomaly:
			r.Type = RuleAnomaly
		case RuleThreshold:
			r.Type = RuleThreshold
			op := CompareOp(e.Op)
			switch op {
			case OpGT, OpGTE, OpLT, OpLTE:
				r.Op = op
			default:
				return Config{}, fmt.Errorf("alerting: rule %q: unknown threshold op %q", e.Name, e.Op)
			}
			r.Threshold = e.Threshold
		default:
			return Config{}, fmt.Errorf("alerting: rule %q: unknown type %q (want %q or %q)", e.Name, e.Type, RuleAnomaly, RuleThreshold)
		}

		if e.Cooldown == "" {
			r.Cooldown = 10 * time.Minute
		} else {
			d, err := time.ParseDuration(e.Cooldown)
			if err != nil {
				return Config{}, fmt.Errorf("alerting: rule %q: invalid cooldown %q: %w", e.Name, e.Cooldown, err)
			}
			r.Cooldown = d
		}

		cfg.Rules = append(cfg.Rules, r)
	}
	return cfg, nil
}

// Compare applies op to (value, threshold).
func Compare(value float64, op CompareOp, threshold float64) bool {
	switch op {
	case OpGT:
		return value > threshold
	case OpGTE:
		return value >= threshold
	case OpLT:
		return value < threshold
	case OpLTE:
		return value <= threshold
	default:
		return false
	}
}
