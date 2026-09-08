package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"

	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// Receiver holds the admission logic shared by the gRPC and HTTP transports.
// Both transports do nothing but decode, call Accept*, and map the error onto
// their own status vocabulary -- so the two cannot drift on backpressure
// semantics, which is the one thing that would be invisible until production.
type Receiver struct {
	queues map[config.Signal]*Queue
	cfg    config.Collector
	m      *observability.Metrics
	log    *slog.Logger
}

// NewReceiver builds a receiver with one bounded queue per signal.
func NewReceiver(cfg config.Collector, m *observability.Metrics, log *slog.Logger) *Receiver {
	queues := make(map[config.Signal]*Queue, len(config.AllSignals))
	for _, s := range config.AllSignals {
		queues[s] = NewQueue(cfg.Queue, s, m)
	}
	return &Receiver{queues: queues, cfg: cfg, m: m, log: log}
}

// Queue exposes a signal's queue, for wiring batchers and for tests.
func (r *Receiver) Queue(s config.Signal) *Queue { return r.queues[s] }

// RetryAfter is the delay advertised to a rejected client.
func (r *Receiver) RetryAfter() time.Duration { return r.cfg.Queue.RetryAfter }

// AcceptTraces admits an OTLP trace export, or rejects the whole request.
//
// Rejection is WHOLE-REQUEST by construction: either every envelope derived
// from this payload is admitted or none is. Partial admission would hand the
// assembler a trace that is missing spans the client believes it delivered,
// and a truncated trace is far harder to detect downstream than an absent one.
func (r *Receiver) AcceptTraces(_ context.Context, resourceSpans []*tracepb.ResourceSpans) error {
	s := GetSplitter()
	defer s.Release()

	envs := envelopeBufGet()
	// Closure, not `defer envelopeBufPut(envs)`: Split* may grow and
	// reallocate the slice, and a direct defer would recycle the stale header.
	defer func() { envelopeBufPut(envs) }()

	envs, spans, err := s.SplitTraces(resourceSpans, envs[:0])
	if err != nil {
		r.m.SpansDroppedDecode.Add(float64(CountSpans(resourceSpans)))
		ReleaseAll(envs)
		return fmt.Errorf("split traces: %w", err)
	}

	r.m.SpansReceived.Add(float64(spans))

	if err := r.queues[config.SignalTraces].EnqueueBatch(envs); err != nil {
		ReleaseAll(envs)
		return err
	}
	return nil
}

// AcceptLogs admits an OTLP log export, or rejects the whole request.
func (r *Receiver) AcceptLogs(_ context.Context, resourceLogs []*logspb.ResourceLogs) error {
	s := GetSplitter()
	defer s.Release()

	envs := envelopeBufGet()
	// Closure, not `defer envelopeBufPut(envs)`: Split* may grow and
	// reallocate the slice, and a direct defer would recycle the stale header.
	defer func() { envelopeBufPut(envs) }()

	envs, records, err := s.SplitLogs(resourceLogs, envs[:0])
	if err != nil {
		r.m.DroppedFor("logs", observability.ReasonDecodeError).Add(float64(CountLogRecords(resourceLogs)))
		ReleaseAll(envs)
		return fmt.Errorf("split logs: %w", err)
	}

	r.m.LogsReceived.Add(float64(records))

	if err := r.queues[config.SignalLogs].EnqueueBatch(envs); err != nil {
		ReleaseAll(envs)
		return err
	}
	return nil
}

// AcceptMetrics admits an OTLP metric export, or rejects the whole request.
func (r *Receiver) AcceptMetrics(_ context.Context, resourceMetrics []*metricspb.ResourceMetrics) error {
	s := GetSplitter()
	defer s.Release()

	envs := envelopeBufGet()
	// Closure, not `defer envelopeBufPut(envs)`: Split* may grow and
	// reallocate the slice, and a direct defer would recycle the stale header.
	defer func() { envelopeBufPut(envs) }()

	envs, points, err := s.SplitMetrics(resourceMetrics, envs[:0])
	if err != nil {
		r.m.DroppedFor("metrics", observability.ReasonDecodeError).Add(float64(CountDataPoints(resourceMetrics)))
		ReleaseAll(envs)
		return fmt.Errorf("split metrics: %w", err)
	}

	r.m.PointsReceived.Add(float64(points))

	if err := r.queues[config.SignalMetrics].EnqueueBatch(envs); err != nil {
		ReleaseAll(envs)
		return err
	}
	return nil
}
