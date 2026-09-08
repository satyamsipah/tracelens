package pipeline

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
	"github.com/twmb/franz-go/pkg/kgo"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/ingest"
	"github.com/satyamsipah/tracelens/internal/observability"
)

const redpandaImage = "redpandadata/redpanda:v24.2.7"

var (
	brokerOnce sync.Once
	sharedBrk  string
	brokerSkip string
	brokerErr  error
)

// dockerReachable reports whether a Docker daemon is actually usable.
//
// Skipping when Docker is ABSENT is reasonable; skipping when Docker is
// present but the container failed to start is not, because `go test` prints
// "ok" for a fully skipped package and the suite reports green while testing
// nothing.
func dockerReachable() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		return false
	}
	defer func() { _ = provider.Close() }()

	return provider.Health(ctx) == nil
}

func recordContainerFailure(what string, err error) {
	if !dockerReachable() {
		brokerSkip = "docker unavailable, skipping container-backed tests: " + err.Error()
		return
	}
	brokerErr = fmt.Errorf("%s failed with docker available: %w", what, err)
}

// startRedpanda boots one real Redpanda for the package.
func startRedpanda(t *testing.T) string {
	t.Helper()

	if testing.Short() {
		t.Skip("skipping container-backed test in -short mode")
	}

	brokerOnce.Do(func() {
		ctx := context.Background()
		container, err := redpanda.Run(ctx, redpandaImage,
			redpanda.WithAutoCreateTopics(),
		)
		if err != nil {
			recordContainerFailure("redpanda container", err)
			return
		}
		broker, err := container.KafkaSeedBroker(ctx)
		if err != nil {
			recordContainerFailure("redpanda broker resolution", err)
			return
		}
		sharedBrk = broker
	})

	if brokerSkip != "" {
		t.Skip(brokerSkip)
	}
	require.NoError(t, brokerErr)
	return sharedBrk
}

func testKafkaCfg(broker, suffix string) config.Kafka {
	return config.Kafka{
		Brokers:        []string{broker},
		TopicSpans:     "spans-" + suffix,
		TopicLogs:      "logs-" + suffix,
		TopicMetrics:   "metrics-" + suffix,
		Partitions:     8,
		ConsumerGroup:  "test-" + suffix,
		ProduceTimeout: 15 * time.Second,
	}
}

func traceID(b byte) []byte {
	id := make([]byte, 16)
	for i := range id {
		id[i] = b
	}
	return id
}

// buildEnvelopes uses the REAL splitter, so this test exercises the production
// keying path rather than a hand-built approximation of it.
func buildEnvelopes(t *testing.T, traceIDs [][]byte, spansPerTrace int) []*ingest.Envelope {
	t.Helper()

	spans := make([]*tracepb.Span, 0, len(traceIDs)*spansPerTrace)
	for _, tid := range traceIDs {
		for i := 0; i < spansPerTrace; i++ {
			sid := make([]byte, 8)
			sid[0] = byte(i + 1)
			spans = append(spans, &tracepb.Span{
				TraceId:           tid,
				SpanId:            sid,
				Name:              "GET /checkout",
				Kind:              tracepb.Span_SPAN_KIND_SERVER,
				StartTimeUnixNano: uint64(time.Now().UnixNano()),
				EndTimeUnixNano:   uint64(time.Now().Add(time.Millisecond).UnixNano()),
			})
		}
	}

	rs := []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
			Key:   "service.name",
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "checkout"}},
		}}},
		ScopeSpans: []*tracepb.ScopeSpans{{Spans: spans}},
	}}

	s := ingest.GetSplitter()
	defer s.Release()

	envs, _, err := s.SplitTraces(rs, nil)
	require.NoError(t, err)
	return envs
}

// consumePartitions reads n records and reports which partition each key
// landed on.
func consumePartitions(t *testing.T, broker, topic string, want int) map[string]map[int32]struct{} {
	t.Helper()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(broker),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	require.NoError(t, err)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	out := map[string]map[int32]struct{}{}
	seen := 0
	for seen < want {
		fetches := client.PollFetches(ctx)
		require.NoError(t, ctx.Err(), "timed out after %d of %d records", seen, want)
		if errs := fetches.Errors(); len(errs) > 0 {
			require.NoError(t, errs[0].Err)
		}
		fetches.EachRecord(func(r *kgo.Record) {
			key := string(r.Key)
			if out[key] == nil {
				out[key] = map[int32]struct{}{}
			}
			out[key][r.Partition] = struct{}{}
			seen++
		})
	}
	return out
}

// TestProducerRoutesTraceToSinglePartition is the test that principle 3 stands
// on. If it fails, tail sampling in phase 2 silently makes decisions on
// fragments of traces.
func TestProducerRoutesTraceToSinglePartition(t *testing.T) {
	broker := startRedpanda(t)
	cfg := testKafkaCfg(broker, "routing")
	m := observability.NewMetrics()
	log := observability.NewLogger("pipeline-test")
	ctx := context.Background()

	require.NoError(t, EnsureTopics(ctx, cfg, log))

	producer, err := NewProducer(cfg, m, log)
	require.NoError(t, err)
	defer func() { _ = producer.Close(ctx) }()

	t.Run("should send every span of one trace to one partition when produced in separate batches", func(t *testing.T) {
		const batches = 6
		const spansPerBatch = 3
		tid := traceID(0x7F)

		// Separate produce calls on purpose: a sticky partitioner would be
		// free to pick a different partition per batch if the key were absent.
		for i := 0; i < batches; i++ {
			envs := buildEnvelopes(t, [][]byte{tid}, spansPerBatch)
			require.Len(t, envs, 1)
			require.NoError(t, producer.Produce(ctx, config.SignalTraces, envs))
		}

		byKey := consumePartitions(t, broker, cfg.TopicSpans, batches)
		require.Len(t, byKey, 1, "exactly one distinct key was produced")

		partitions := byKey[string(tid)]
		require.NotNil(t, partitions)
		require.Len(t, partitions, 1,
			"all %d batches of one trace must land on ONE partition, got %v", batches, partitions)
	})
}

func TestProducerSpreadsDistinctTraces(t *testing.T) {
	broker := startRedpanda(t)
	cfg := testKafkaCfg(broker, "spread")
	m := observability.NewMetrics()
	log := observability.NewLogger("pipeline-test")
	ctx := context.Background()

	require.NoError(t, EnsureTopics(ctx, cfg, log))

	producer, err := NewProducer(cfg, m, log)
	require.NoError(t, err)
	defer func() { _ = producer.Close(ctx) }()

	t.Run("should distribute distinct traces across partitions when keys differ", func(t *testing.T) {
		const traces = 64
		ids := make([][]byte, 0, traces)
		for i := 0; i < traces; i++ {
			ids = append(ids, traceID(byte(i+1)))
		}

		envs := buildEnvelopes(t, ids, 2)
		require.Len(t, envs, traces)
		require.NoError(t, producer.Produce(ctx, config.SignalTraces, envs))

		byKey := consumePartitions(t, broker, cfg.TopicSpans, traces)
		require.Len(t, byKey, traces)

		used := map[int32]struct{}{}
		for key, partitions := range byKey {
			require.Len(t, partitions, 1, "key %x split across partitions", key)
			for p := range partitions {
				used[p] = struct{}{}
			}
		}
		require.Greater(t, len(used), 1,
			"hash partitioning must spread load; a single partition means the key is being ignored")
	})
}

func TestProducerRejectsKeylessSpanRecord(t *testing.T) {
	// No broker: the guard must trip before any network call is attempted,
	// which is also what makes this test fast and hermetic.
	newBareProducer := func() *Producer {
		return &Producer{
			cfg: config.Kafka{
				TopicSpans:     "spans",
				TopicMetrics:   "metrics",
				ProduceTimeout: time.Second,
			},
			m:   observability.NewMetrics(),
			log: observability.NewLogger("pipeline-test"),
		}
	}

	tests := []struct {
		name    string
		signal  config.Signal
		key     []byte
		wantErr error
	}{
		{
			name:    "should refuse the batch when a span record has no partition key",
			signal:  config.SignalTraces,
			key:     nil,
			wantErr: ErrMissingPartitionKey,
		},
		{
			name:    "should refuse the batch when a span key is empty rather than absent",
			signal:  config.SignalTraces,
			key:     []byte{},
			wantErr: ErrMissingPartitionKey,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := newBareProducer()
			env := ingest.NewEnvelope(tt.signal, tt.key, []byte{0x01}, 1)

			err := p.Produce(context.Background(), tt.signal, []*ingest.Envelope{env})
			require.ErrorIs(t, err, tt.wantErr,
				"a keyless span record would round-robin and silently split traces")
		})
	}

	t.Run("should treat an empty batch as a no-op when there is nothing to send", func(t *testing.T) {
		p := newBareProducer()
		require.NoError(t, p.Produce(context.Background(), config.SignalTraces, nil))
	})
}
