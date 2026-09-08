package ingest

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
)

// Sink is the downstream of the ingest queue. It is declared here, on the
// consuming side, so that internal/ingest depends on no broker package and
// the transport -> pipeline -> storage layering holds.
//
// Produce takes ownership of the envelopes on return, success or failure.
type Sink interface {
	Produce(ctx context.Context, signal config.Signal, envs []*Envelope) error
}

// Batcher drains one signal's queue and flushes to the sink whenever the
// record count, the byte size, or the flush interval trips -- whichever comes
// first. Byte size matters independently of record count because a broker
// rejects an oversized produce request outright.
type Batcher struct {
	queue  *Queue
	sink   Sink
	cfg    config.BatchConfig
	signal config.Signal
	log    *slog.Logger

	sizeHist prometheus.Observer
	dropped  prometheus.Counter
}

// NewBatcher builds a batcher for one signal.
func NewBatcher(q *Queue, sink Sink, cfg config.BatchConfig, signal config.Signal, m *observability.Metrics, log *slog.Logger) *Batcher {
	return &Batcher{
		queue:    q,
		sink:     sink,
		cfg:      cfg,
		signal:   signal,
		log:      log.With(slog.String("signal", string(signal))),
		sizeHist: m.BatchSize.WithLabelValues(string(signal)),
		dropped:  m.DroppedFor(string(signal), observability.ReasonProduceFailed),
	}
}

// Run drains the queue until the context is cancelled.
//
// On shutdown it drains whatever is still queued before returning. Those
// envelopes belong to clients we already answered OK, so discarding them here
// would be exactly the silent loss the design forbids.
func (b *Batcher) Run(ctx context.Context) error {
	buf := make([]*Envelope, 0, b.cfg.MaxRecords)
	bytes := 0

	ticker := time.NewTicker(b.cfg.FlushInterval)
	defer ticker.Stop()

	flush := func(ctx context.Context) {
		if len(buf) == 0 {
			return
		}
		b.sizeHist.Observe(float64(len(buf)))
		if err := b.sink.Produce(ctx, b.signal, buf); err != nil {
			items := 0
			for _, e := range buf {
				items += e.items
			}
			b.dropped.Add(float64(items))
			b.log.Error("produce batch failed",
				slog.Int("records", len(buf)),
				slog.Int("items", items),
				slog.String("error", err.Error()))
		}
		// Produce owns the envelopes either way.
		buf = buf[:0]
		bytes = 0
	}

	for {
		select {
		case <-ctx.Done():
			flush(context.WithoutCancel(ctx))
			b.drain()
			return nil

		case <-ticker.C:
			flush(ctx)

		default:
			e, ok := b.queue.Dequeue(ctx)
			if !ok {
				flush(context.WithoutCancel(ctx))
				b.drain()
				if err := ctx.Err(); err != nil && !errors.Is(err, context.Canceled) {
					return fmt.Errorf("batcher %s: %w", b.signal, err)
				}
				return nil
			}
			buf = append(buf, e)
			bytes += e.Size()

			if len(buf) >= b.cfg.MaxRecords || bytes >= b.cfg.MaxBytes {
				flush(ctx)
			}
		}
	}
}

// drain flushes anything left in the queue after the context is done, on a
// bounded best-effort deadline.
func (b *Batcher) drain() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	buf := make([]*Envelope, 0, b.cfg.MaxRecords)
	for {
		e, ok := b.queue.TryDequeue()
		if !ok {
			break
		}
		buf = append(buf, e)
		if len(buf) >= b.cfg.MaxRecords {
			if err := b.sink.Produce(ctx, b.signal, buf); err != nil {
				b.log.Error("drain produce failed", slog.String("error", err.Error()))
			}
			buf = buf[:0]
		}
	}
	if len(buf) > 0 {
		if err := b.sink.Produce(ctx, b.signal, buf); err != nil {
			b.log.Error("drain produce failed", slog.String("error", err.Error()))
		}
	}
}
