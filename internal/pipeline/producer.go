// Package pipeline moves telemetry between the OTLP receiver and storage via
// Redpanda.
//
// Layering note: this package imports internal/ingest for the Envelope type,
// so the compile-time arrow points pipeline -> ingest while the runtime data
// flow is ingest -> pipeline. That inversion is deliberate: ingest declares
// the Sink interface it needs and pipeline satisfies it, which is what keeps
// the transport layer free of any broker dependency.
package pipeline

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/ingest"
	"github.com/satyamsipah/tracelens/internal/observability"
)

// ErrMissingPartitionKey is returned when a span-bearing record would be
// produced with no key.
//
// This is a hard error rather than a fallback, because the failure it prevents
// is invisible. A keyless record round-robins, the pipeline keeps working, and
// nothing looks wrong until the phase-2 tail sampler starts making decisions
// on fragments of traces it can no longer see whole.
var ErrMissingPartitionKey = errors.New("span record has no trace_id partition key")

// Producer writes envelopes to Redpanda.
type Producer struct {
	client *kgo.Client
	cfg    config.Kafka
	m      *observability.Metrics
	log    *slog.Logger
}

// NewProducer dials the broker and returns a ready producer.
func NewProducer(cfg config.Kafka, m *observability.Metrics, log *slog.Logger) (*Producer, error) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),

		// THE partitioning decision. StickyKeyPartitioner hashes the record
		// key when one is present, so an identical trace_id always resolves
		// to an identical partition. It falls back to sticky round-robin ONLY
		// for keyless records -- which is why Produce below refuses to emit a
		// keyless span record at all.
		kgo.RecordPartitioner(kgo.StickyKeyPartitioner(nil)),

		// Durability over latency: a span acknowledged to a client and then
		// lost to a leader failover is precisely the silent loss we forbid.
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DisableIdempotentWrite(),

		kgo.ProducerBatchCompression(kgo.ZstdCompression(), kgo.SnappyCompression()),
		kgo.ProduceRequestTimeout(cfg.ProduceTimeout),
		kgo.RecordDeliveryTimeout(cfg.ProduceTimeout),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka producer: %w", err)
	}
	return &Producer{client: client, cfg: cfg, m: m, log: log}, nil
}

// Produce writes a batch of envelopes and waits for broker acknowledgement.
//
// It takes ownership of the envelopes and releases them before returning, on
// both the success and the failure path.
func (p *Producer) Produce(ctx context.Context, signal config.Signal, envs []*ingest.Envelope) error {
	if len(envs) == 0 {
		return nil
	}
	defer ingest.ReleaseAll(envs)

	topic := p.cfg.TopicFor(signal)
	if topic == "" {
		return fmt.Errorf("no topic configured for signal %q", signal)
	}

	records := make([]*kgo.Record, 0, len(envs))
	for _, e := range envs {
		key := e.Key()
		if signal == config.SignalTraces && len(key) == 0 {
			p.m.ProduceErrors.WithLabelValues(topic).Inc()
			return ErrMissingPartitionKey
		}
		records = append(records, &kgo.Record{
			Topic: topic,
			Key:   key,
			Value: e.Payload(),
		})
	}

	start := time.Now()
	results := p.client.ProduceSync(ctx, records...)
	p.m.ProduceLatency.WithLabelValues(topic).Observe(time.Since(start).Seconds())

	var firstErr error
	ok := 0
	for _, res := range results {
		if res.Err != nil {
			p.m.ProduceErrors.WithLabelValues(topic).Inc()
			if firstErr == nil {
				firstErr = res.Err
			}
			continue
		}
		ok++
	}
	p.m.ProduceRecords.WithLabelValues(topic).Add(float64(ok))

	if firstErr != nil {
		return fmt.Errorf("produce to %s: %w", topic, firstErr)
	}
	return nil
}

// Close flushes and shuts down the producer.
func (p *Producer) Close(ctx context.Context) error {
	if err := p.client.Flush(ctx); err != nil {
		return fmt.Errorf("flush producer: %w", err)
	}
	p.client.Close()
	return nil
}

// Ping verifies broker reachability, for readiness probes.
func (p *Producer) Ping(ctx context.Context) error { return p.client.Ping(ctx) }
