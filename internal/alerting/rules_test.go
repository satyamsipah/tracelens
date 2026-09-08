package alerting

import (
	"strings"
	"testing"
	"time"
)

func TestParseValidRulesFile(t *testing.T) {
	yaml := `
rules:
  - name: checkout-latency-anomaly
    service: checkout
    operation: charge
    metric: p95_latency_ms
    type: anomaly
    cooldown: 5m
    webhook: https://example.com/hook
  - name: checkout-error-rate
    service: checkout
    metric: error_rate
    type: threshold
    op: ">"
    threshold: 0.05
    slack_webhook: https://hooks.slack.com/services/x
`
	cfg, err := Parse([]byte(yaml))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(cfg.Rules) != 2 {
		t.Fatalf("got %d rules, want 2", len(cfg.Rules))
	}
	r0 := cfg.Rules[0]
	if r0.Type != RuleAnomaly || r0.Cooldown != 5*time.Minute || r0.Webhook == "" {
		t.Errorf("rule 0 = %+v", r0)
	}
	r1 := cfg.Rules[1]
	if r1.Type != RuleThreshold || r1.Op != OpGT || r1.Threshold != 0.05 {
		t.Errorf("rule 1 = %+v", r1)
	}
	if r1.Cooldown != 10*time.Minute {
		t.Errorf("rule 1 cooldown = %s, want the 10m default", r1.Cooldown)
	}
}

func TestParseRejectsInvalidRules(t *testing.T) {
	tests := []struct {
		name    string
		yaml    string
		wantErr string
	}{
		{
			name:    "missing name",
			yaml:    `rules: [{service: a, metric: m, type: anomaly, webhook: h}]`,
			wantErr: "name is required",
		},
		{
			name:    "duplicate name",
			yaml:    `rules: [{name: r, service: a, metric: m, type: anomaly, webhook: h}, {name: r, service: b, metric: m, type: anomaly, webhook: h}]`,
			wantErr: "duplicate rule name",
		},
		{
			name:    "missing service",
			yaml:    `rules: [{name: r, metric: m, type: anomaly, webhook: h}]`,
			wantErr: "service is required",
		},
		{
			name:    "no delivery target",
			yaml:    `rules: [{name: r, service: a, metric: m, type: anomaly}]`,
			wantErr: "webhook or slack_webhook is required",
		},
		{
			name:    "unknown type",
			yaml:    `rules: [{name: r, service: a, metric: m, type: bogus, webhook: h}]`,
			wantErr: "unknown type",
		},
		{
			name:    "threshold missing valid op",
			yaml:    `rules: [{name: r, service: a, metric: m, type: threshold, op: "~=", threshold: 1, webhook: h}]`,
			wantErr: "unknown threshold op",
		},
		{
			name:    "invalid cooldown",
			yaml:    `rules: [{name: r, service: a, metric: m, type: anomaly, cooldown: "not-a-duration", webhook: h}]`,
			wantErr: "invalid cooldown",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse([]byte(tt.yaml))
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("error = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestCompare(t *testing.T) {
	tests := []struct {
		value, threshold float64
		op               CompareOp
		want             bool
	}{
		{5, 3, OpGT, true}, {3, 3, OpGT, false},
		{3, 3, OpGTE, true}, {2, 3, OpGTE, false},
		{2, 3, OpLT, true}, {3, 3, OpLT, false},
		{3, 3, OpLTE, true}, {4, 3, OpLTE, false},
	}
	for _, tt := range tests {
		if got := Compare(tt.value, tt.op, tt.threshold); got != tt.want {
			t.Errorf("Compare(%v, %q, %v) = %v, want %v", tt.value, tt.op, tt.threshold, got, tt.want)
		}
	}
}
