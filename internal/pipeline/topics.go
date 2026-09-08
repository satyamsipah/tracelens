package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/satyamsipah/tracelens/internal/config"
)

// EnsureTopics creates the three signal topics if they are absent.
//
// The partition count is fixed configuration rather than something we grow on
// demand, and that is load bearing: Kafka's partitioner is hash(key) % N, so
// CHANGING N rehashes every key. Any trace in flight across a repartition has
// its spans split between the old and the new partition, and therefore across
// two assembler instances -- the exact failure that keying on trace_id exists
// to prevent. Growing partition count is a planned migration, not a knob.
func EnsureTopics(ctx context.Context, cfg config.Kafka, log *slog.Logger) error {
	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	if err != nil {
		return fmt.Errorf("dial brokers for topic setup: %w", err)
	}
	defer client.Close()

	admin := kadm.NewClient(client)
	topics := []string{cfg.TopicSpans, cfg.TopicLogs, cfg.TopicMetrics}

	resp, err := admin.CreateTopics(ctx, cfg.Partitions, -1, nil, topics...)
	if err != nil {
		return fmt.Errorf("create topics: %w", err)
	}
	for _, t := range resp {
		if t.Err != nil && !errors.Is(t.Err, kerr.TopicAlreadyExists) {
			return fmt.Errorf("create topic %s: %w", t.Topic, t.Err)
		}
		log.Info("topic ready",
			slog.String("topic", t.Topic),
			slog.Int("partitions", int(cfg.Partitions)))
	}
	return nil
}
