package alerting

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Notification is one alert state transition.
type Notification struct {
	Rule    string
	Service string
	Metric  string
	Firing  bool // true = just started firing, false = just cleared
	Message string
	Time    time.Time
}

// Sink delivers a Notification somewhere. Kept as an interface so tests
// never need a live external webhook -- an httptest.Server stands in for
// "somewhere", which is a real HTTP endpoint, not a mock of this package's
// own logic.
type Sink interface {
	Notify(ctx context.Context, n Notification) error
}

// WebhookSink POSTs a generic JSON body.
type WebhookSink struct {
	URL    string
	Client *http.Client
}

func (s *WebhookSink) Notify(ctx context.Context, n Notification) error {
	body, err := json.Marshal(n)
	if err != nil {
		return fmt.Errorf("alerting: marshal webhook notification: %w", err)
	}
	return postJSON(ctx, s.client(), s.URL, body)
}

func (s *WebhookSink) client() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	return http.DefaultClient
}

// SlackSink POSTs Slack's incoming-webhook {"text": ...} shape.
type SlackSink struct {
	URL    string
	Client *http.Client
}

func (s *SlackSink) Notify(ctx context.Context, n Notification) error {
	verb := "FIRING"
	if !n.Firing {
		verb = "RESOLVED"
	}
	text := fmt.Sprintf("[%s] %s (%s/%s): %s", verb, n.Rule, n.Service, n.Metric, n.Message)
	body, err := json.Marshal(map[string]string{"text": text})
	if err != nil {
		return fmt.Errorf("alerting: marshal slack notification: %w", err)
	}
	return postJSON(ctx, s.client(), s.URL, body)
}

func (s *SlackSink) client() *http.Client {
	if s.Client != nil {
		return s.Client
	}
	return http.DefaultClient
}

func postJSON(ctx context.Context, client *http.Client, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("alerting: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("alerting: deliver notification: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("alerting: notification endpoint returned %s", resp.Status)
	}
	return nil
}

// MultiSink fans a notification out to every sink, best-effort: one sink's
// failure doesn't stop delivery to the others, and every error is joined
// into the returned error rather than only the first.
type MultiSink []Sink

func (m MultiSink) Notify(ctx context.Context, n Notification) error {
	var errs []error
	for _, s := range m {
		if err := s.Notify(ctx, n); err != nil {
			errs = append(errs, err)
		}
	}
	if len(errs) == 0 {
		return nil
	}
	joined := errs[0]
	for _, e := range errs[1:] {
		joined = fmt.Errorf("%w; %v", joined, e)
	}
	return joined
}

// SinksFor builds the fan-out sink a rule's configured endpoints imply.
func SinksFor(r Rule, client *http.Client) Sink {
	var sinks MultiSink
	if r.Webhook != "" {
		sinks = append(sinks, &WebhookSink{URL: r.Webhook, Client: client})
	}
	if r.SlackWebhook != "" {
		sinks = append(sinks, &SlackSink{URL: r.SlackWebhook, Client: client})
	}
	return sinks
}
