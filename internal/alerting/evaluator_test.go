package alerting

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/satyamsipah/tracelens/internal/anomaly"
)

// capturingServer is a real HTTP server (httptest), not a mock of this
// package's logic -- it records every delivered notification body.
type capturingServer struct {
	mu   sync.Mutex
	seen []Notification
	srv  *httptest.Server
}

func newCapturingServer(t *testing.T) *capturingServer {
	t.Helper()
	c := &capturingServer{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var n Notification
		if err := json.NewDecoder(r.Body).Decode(&n); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		c.mu.Lock()
		c.seen = append(c.seen, n)
		c.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(c.srv.Close)
	return c
}

func (c *capturingServer) notifications() []Notification {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]Notification(nil), c.seen...)
}

func newTestEvaluator(t *testing.T, srv *capturingServer) *Evaluator {
	t.Helper()
	e := NewEvaluator(nil, srv.srv.Client())
	e.sinkFor = func(r Rule) Sink { return &WebhookSink{URL: srv.srv.URL, Client: srv.srv.Client()} }
	return e
}

func TestEvaluatorThresholdNotifiesOnlyOnTransition(t *testing.T) {
	srv := newCapturingServer(t)
	e := newTestEvaluator(t, srv)
	rule := Rule{Name: "high-error-rate", Service: "checkout", Metric: "error_rate", Type: RuleThreshold, Op: OpGT, Threshold: 0.05, Cooldown: 0}

	base := time.Now()
	e.EvaluateThreshold(context.Background(), rule, 0.01, base)                    // below threshold: no transition (starts clear)
	e.EvaluateThreshold(context.Background(), rule, 0.10, base.Add(time.Second))   // crosses: fires
	e.EvaluateThreshold(context.Background(), rule, 0.12, base.Add(2*time.Second)) // still above: dedup, no repeat
	e.EvaluateThreshold(context.Background(), rule, 0.11, base.Add(3*time.Second)) // still above: dedup
	e.EvaluateThreshold(context.Background(), rule, 0.01, base.Add(4*time.Second)) // clears

	notes := srv.notifications()
	if len(notes) != 2 {
		t.Fatalf("got %d notifications, want 2 (one fire, one clear): %+v", len(notes), notes)
	}
	if !notes[0].Firing {
		t.Errorf("notification 0 should be the firing transition: %+v", notes[0])
	}
	if notes[1].Firing {
		t.Errorf("notification 1 should be the clear transition: %+v", notes[1])
	}
}

func TestEvaluatorCooldownSuppressesRapidReFire(t *testing.T) {
	srv := newCapturingServer(t)
	e := newTestEvaluator(t, srv)
	rule := Rule{Name: "flapping", Service: "checkout", Metric: "error_rate", Type: RuleThreshold, Op: OpGT, Threshold: 0.05, Cooldown: time.Minute}

	base := time.Now()
	e.EvaluateThreshold(context.Background(), rule, 0.10, base)                     // fires: notified
	e.EvaluateThreshold(context.Background(), rule, 0.01, base.Add(10*time.Second)) // clears WITHIN cooldown: suppressed
	e.EvaluateThreshold(context.Background(), rule, 0.10, base.Add(20*time.Second)) // fires again WITHIN cooldown: suppressed

	notes := srv.notifications()
	if len(notes) != 1 {
		t.Fatalf("got %d notifications during the cooldown window, want 1 (only the initial fire): %+v", len(notes), notes)
	}

	// After the cooldown elapses, a genuine transition notifies again.
	e.EvaluateThreshold(context.Background(), rule, 0.01, base.Add(2*time.Minute))
	notes = srv.notifications()
	if len(notes) != 2 {
		t.Fatalf("got %d notifications after cooldown elapsed, want 2", len(notes))
	}
}

func TestEvaluatorAnomalyOnlyNotifiesOnStateChanged(t *testing.T) {
	srv := newCapturingServer(t)
	e := newTestEvaluator(t, srv)
	rule := Rule{Name: "latency-anomaly", Service: "checkout", Metric: "p95_latency_ms", Type: RuleAnomaly, Cooldown: 0}

	base := time.Now()
	e.EvaluateAnomaly(context.Background(), rule, anomaly.Result{Firing: true, StateChanged: true, Time: base})
	e.EvaluateAnomaly(context.Background(), rule, anomaly.Result{Firing: true, StateChanged: false, Time: base.Add(time.Second)})
	e.EvaluateAnomaly(context.Background(), rule, anomaly.Result{Firing: true, StateChanged: false, Time: base.Add(2 * time.Second)})

	notes := srv.notifications()
	if len(notes) != 1 {
		t.Fatalf("got %d notifications, want 1 (StateChanged=false ticks must dedup)", len(notes))
	}
}

func TestEvalOnceDrivesAnomalyRuleThroughARealDetector(t *testing.T) {
	srv := newCapturingServer(t)
	e := newTestEvaluator(t, srv)
	rule := Rule{Name: "latency-anomaly", Service: "checkout", Operation: "charge", Metric: "p95_latency_ms", Type: RuleAnomaly, Cooldown: 0}
	cfg := Config{Rules: []Rule{rule}}
	detector := anomaly.NewDetector(anomaly.DefaultConfig())

	base := time.Date(2026, 1, 5, 10, 0, 0, 0, time.UTC)
	tick := base
	fetch := func(ctx context.Context, r Rule) (float64, time.Time, error) {
		return 100, tick, nil
	}
	// Warm the baseline across several weeks at the same hour/weekday.
	for i := 0; i < 9; i++ {
		e.EvalOnce(context.Background(), cfg, detector, fetch)
		tick = tick.Add(7 * 24 * time.Hour)
	}
	if len(srv.notifications()) != 0 {
		t.Fatalf("no anomaly should have fired during warmup: %+v", srv.notifications())
	}

	// A huge spike, twice (hysteresis needs 2 consecutive ticks).
	fetch = func(ctx context.Context, r Rule) (float64, time.Time, error) { return 100000, tick, nil }
	e.EvalOnce(context.Background(), cfg, detector, fetch)
	tick = tick.Add(time.Minute)
	e.EvalOnce(context.Background(), cfg, detector, fetch)

	notes := srv.notifications()
	if len(notes) != 1 || !notes[0].Firing {
		t.Fatalf("expected exactly one firing notification after the spike, got %+v", notes)
	}
}
