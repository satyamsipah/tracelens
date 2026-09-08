package pipeline

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
)

// Handler processes one poll's worth of records for a single topic.
// Returning an error means the offsets are NOT committed and the records will
// be redelivered, so handlers must be idempotent -- which is what the
// ReplacingMergeTree sort key and the insert dedup token provide.
type Handler func(ctx context.Context, topic string, records []*kgo.Record) error

// Consumer reads a consumer group and dispatches per topic.
type Consumer struct {
	client *kgo.Client
	cfg    config.Kafka
	m      *observability.Metrics
	log    *slog.Logger
}

// NewConsumer joins the consumer group.
//
// Auto-commit is disabled on purpose. Offsets advance only after the handler
// reports the batch durably stored, which makes delivery at-least-once. The
// alternative -- committing on poll -- would make it at-most-once and would
// lose a batch on any writer crash, silently.
func NewConsumer(cfg config.Kafka, m *observability.Metrics, log *slog.Logger) (*Consumer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.ConsumerGroup),
		kgo.ConsumeTopics(cfg.TopicSpans, cfg.TopicLogs, cfg.TopicMetrics),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(config.LoadKafka().ProduceTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka consumer: %w", err)
	}
	return &Consumer{client: client, cfg: cfg, m: m, log: log}, nil
}

// Run polls until the context is cancelled.
func (c *Consumer) Run(ctx context.Context, handle Handler) error {
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}

		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if ctx.Err() != nil {
					return nil
				}
				c.m.ConsumeErrors.WithLabelValues(e.Topic).Inc()
				c.log.Error("fetch error",
					slog.String("topic", e.Topic),
					slog.Int("partition", int(e.Partition)),
					slog.String("error", e.Err.Error()))
			}
			continue
		}

		byTopic := map[string][]*kgo.Record{}
		fetches.EachRecord(func(r *kgo.Record) {
			byTopic[r.Topic] = append(byTopic[r.Topic], r)
		})

		failed := false
		for topic, records := range byTopic {
			c.m.ConsumeRecords.WithLabelValues(topic).Add(float64(len(records)))
			if err := handle(ctx, topic, records); err != nil {
				failed = true
				c.m.ConsumeErrors.WithLabelValues(topic).Inc()
				c.log.Error("handler failed, offsets withheld for redelivery",
					slog.String("topic", topic),
					slog.Int("records", len(records)),
					slog.String("error", err.Error()))
			}
		}

		// Commit only when every topic in this poll succeeded. Committing a
		// partial poll would silently drop the failed topic's records.
		if !failed {
			if err := c.client.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
				c.log.Error("commit offsets failed", slog.String("error", err.Error()))
			}
		}
	}
}

// Close leaves the group cleanly so partitions rebalance promptly.
func (c *Consumer) Close() { c.client.Close() }

// Ping verifies broker reachability, for readiness probes.
func (c *Consumer) Ping(ctx context.Context) error { return c.client.Ping(ctx) }
