package alerting

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"github.com/satyamsipah/tracelens/internal/anomaly"
)

// Fetcher returns the current value and observation time for one rule.
// Decoupled from ClickHouse so this package's own evaluation/dedup/cooldown
// logic is testable without a live connection -- cmd/query supplies the
// real fetcher, backed by QueryRED / the service graph.
type Fetcher func(ctx context.Context, r Rule) (value float64, at time.Time, err error)

type ruleState struct {
	firing       bool
	lastNotified time.Time
}

// Evaluator tracks per-rule firing state and applies dedup (only a state
// TRANSITION is notified) and cooldown (a minimum gap between two
// notifications for the same rule) before calling out to a Sink.
type Evaluator struct {
	mu      sync.Mutex
	states  map[string]*ruleState
	sinkFor func(Rule) Sink
	log     *slog.Logger
}

// NewEvaluator builds an evaluator. httpClient may be nil (http.DefaultClient).
func NewEvaluator(log *slog.Logger, httpClient *http.Client) *Evaluator {
	return &Evaluator{
		states:  make(map[string]*ruleState),
		sinkFor: func(r Rule) Sink { return SinksFor(r, httpClient) },
		log:     log,
	}
}

func (e *Evaluator) stateForLocked(name string) *ruleState {
	st, ok := e.states[name]
	if !ok {
		st = &ruleState{}
		e.states[name] = st
	}
	return st
}

// EvaluateAnomaly reacts to one anomaly.Result. Only a StateChanged result
// can produce a notification -- an anomaly.Result that is still firing (or
// still clear) from the previous tick is exactly the "dedup" case.
func (e *Evaluator) EvaluateAnomaly(ctx context.Context, r Rule, res anomaly.Result) {
	if !res.StateChanged {
		return
	}
	e.transition(ctx, r, res.Firing, res.Time,
		fmt.Sprintf("value=%.4f baseline=%.4f z=%.2f", res.Value, res.Baseline, res.Z))
}

// EvaluateThreshold reacts to a plain value/threshold comparison.
func (e *Evaluator) EvaluateThreshold(ctx context.Context, r Rule, value float64, at time.Time) {
	firing := Compare(value, r.Op, r.Threshold)
	e.transition(ctx, r, firing, at, fmt.Sprintf("value=%.4f %s %.4f", value, r.Op, r.Threshold))
}

// transition applies dedup and cooldown, then notifies if both pass.
func (e *Evaluator) transition(ctx context.Context, r Rule, firing bool, at time.Time, message string) {
	e.mu.Lock()
	st := e.stateForLocked(r.Name)
	if firing == st.firing {
		// Dedup: no actual state change (the anomaly path already checked
		// this via StateChanged; the threshold path relies on this check).
		e.mu.Unlock()
		return
	}
	withinCooldown := !st.lastNotified.IsZero() && at.Sub(st.lastNotified) < r.Cooldown
	// The true firing state always updates, even when the notification
	// itself is suppressed by cooldown -- otherwise a later observation
	// would compare against a stale state and could never detect the NEXT
	// real transition.
	st.firing = firing
	if !withinCooldown {
		st.lastNotified = at
	}
	e.mu.Unlock()

	if withinCooldown {
		return
	}

	sink := e.sinkFor(r)
	n := Notification{Rule: r.Name, Service: r.Service, Metric: r.Metric, Firing: firing, Message: message, Time: at}
	if err := sink.Notify(ctx, n); err != nil && e.log != nil {
		e.log.Error("alert notification failed", slog.String("rule", r.Name), slog.String("error", err.Error()))
	}
}

// RunScheduled evaluates every rule in cfg on each tick until ctx is done.
func (e *Evaluator) RunScheduled(ctx context.Context, cfg Config, interval time.Duration, detector *anomaly.Detector, fetch Fetcher) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			e.EvalOnce(ctx, cfg, detector, fetch)
		}
	}
}

// EvalOnce runs every rule once -- exported so a caller (or a test) can
// drive evaluation deterministically instead of waiting on a ticker.
func (e *Evaluator) EvalOnce(ctx context.Context, cfg Config, detector *anomaly.Detector, fetch Fetcher) {
	for _, r := range cfg.Rules {
		value, at, err := fetch(ctx, r)
		if err != nil {
			if e.log != nil {
				e.log.Error("alert rule fetch failed", slog.String("rule", r.Name), slog.String("error", err.Error()))
			}
			continue
		}
		switch r.Type {
		case RuleAnomaly:
			res := detector.Observe(anomaly.Observation{
				Series: anomaly.SeriesKey{Service: r.Service, Operation: r.Operation, Metric: r.Metric},
				Time:   at,
				Value:  value,
			})
			e.EvaluateAnomaly(ctx, r, res)
		case RuleThreshold:
			e.EvaluateThreshold(ctx, r, value, at)
		}
	}
}
