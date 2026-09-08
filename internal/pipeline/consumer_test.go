package pipeline

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
)

// TestConsumerNoTraceSplitAcrossThreeInstances is the test requirement 4
// explicitly asks for. Phase 1 (TestProducerRoutesTraceToSinglePartition)
// proved the PRODUCER side routes a trace's spans to one partition; this
// proves the CONSUMER side of that guarantee holds once the partitions are
// spread across multiple real, concurrently-running consumer-group members --
// which is the layer that actually matters for the sampler, since the
// in-flight trace buffer lives in one specific process's memory. If Kafka's
// consumer-group assignment ever let two members see the same partition
// simultaneously (it does not, by design, but this proves it empirically
// rather than by argument), two assembler replicas would each buffer a
// fragment of the same trace, each judge its fragment complete independently,
// and the tail sampler would silently make decisions on incomplete data.
func TestConsumerNoTraceSplitAcrossThreeInstances(t *testing.T) {
	broker := startRedpanda(t)
	suffix := "consumer-split"
	cfg := testKafkaCfg(broker, suffix)
	cfg.Partitions = 12
	cfg.ConsumerGroup = "test-3-instance-group"
	log := observability.NewLogger("pipeline-test")
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	require.NoError(t, EnsureTopics(ctx, cfg, log))

	m := observability.NewMetrics()
	producer, err := NewProducer(cfg, m, log)
	require.NoError(t, err)
	defer func() { _ = producer.Close(ctx) }()

	// Produce many distinct traces, several batches each, exactly as the
	// real collector -> assembler path does: one envelope per (trace_id)
	// grouping, keyed by the real splitter.
	const traces = 300
	const spansPerTrace = 4
	ids := make([][]byte, traces)
	for i := range ids {
		// A local 2-byte id, distinct from the shared single-byte traceID
		// helper: 300 distinct traces need more than one byte of entropy.
		id := make([]byte, 16)
		id[0] = byte(i)
		id[1] = byte(i >> 8)
		ids[i] = id
	}

	for _, id := range ids {
		envs := buildEnvelopes(t, [][]byte{id}, spansPerTrace)
		require.NoError(t, producer.Produce(ctx, config.SignalTraces, envs))
	}

	// Three REAL, concurrently-running consumer-group members, each with its
	// own client and its own Consumer, all in the SAME group -- exactly how
	// three assembler replica processes would be deployed.
	const instances = 3
	seenBy := make([]map[[16]byte]struct{}, instances)
	var mus [instances]sync.Mutex
	var wg sync.WaitGroup
	var consumers [instances]*Consumer

	runCtx, runCancel := context.WithTimeout(ctx, 30*time.Second)
	defer runCancel()

	for i := 0; i < instances; i++ {
		seenBy[i] = map[[16]byte]struct{}{}
		c, err := NewConsumer(cfg, m, log)
		require.NoError(t, err)
		consumers[i] = c

		idx := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = c.Run(runCtx, func(_ context.Context, topic string, records []*kgo.Record) error {
				if topic != cfg.TopicSpans {
					return nil
				}
				for _, r := range records {
					var tid [16]byte
					copy(tid[:], r.Key)
					mus[idx].Lock()
					seenBy[idx][tid] = struct{}{}
					mus[idx].Unlock()
				}
				return nil
			})
		}()
	}

	// Wait until every produced trace has been observed by SOME instance,
	// or the context times out.
	require.Eventually(t, func() bool {
		total := 0
		for i := range seenBy {
			mus[i].Lock()
			total += len(seenBy[i])
			mus[i].Unlock()
		}
		return total >= traces
	}, 25*time.Second, 200*time.Millisecond, "all produced traces must eventually be consumed by the group")

	runCancel()
	wg.Wait()
	for _, c := range consumers {
		c.Close()
	}

	// THE assertion: no trace_id may appear in more than one instance's set.
	seenCount := map[[16]byte]int{}
	for i := range seenBy {
		for id := range seenBy[i] {
			seenCount[id]++
		}
	}
	split := 0
	for id, count := range seenCount {
		if count > 1 {
			split++
			t.Logf("trace %x was observed by %d different consumer instances", id, count)
		}
	}
	require.Zero(t, split, "no trace's spans may be observed by more than one consumer-group member")

	// Sanity: the group actually spread work across more than one instance
	// (otherwise this test would trivially pass by only one member doing
	// anything, proving nothing about the multi-instance case).
	active := 0
	for i := range seenBy {
		if len(seenBy[i]) > 0 {
			active++
		}
	}
	require.Greater(t, active, 1, "the test must exercise more than one active consumer instance to be meaningful")
}

// TestConsumerCommitGateWithholdsAndReleases verifies RunWithCommitGate
// against a real broker: a ceiling that withholds a partition must prevent
// its offsets from advancing, and clearing the ceiling must let them proceed.
func TestConsumerCommitGateWithholdsAndReleases(t *testing.T) {
	broker := startRedpanda(t)
	cfg := testKafkaCfg(broker, "commit-gate")
	log := observability.NewLogger("pipeline-test")
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	require.NoError(t, EnsureTopics(ctx, cfg, log))

	m := observability.NewMetrics()
	producer, err := NewProducer(cfg, m, log)
	require.NoError(t, err)
	defer func() { _ = producer.Close(ctx) }()

	envs := buildEnvelopes(t, [][]byte{traceID(0xAA)}, 3)
	require.NoError(t, producer.Produce(ctx, config.SignalTraces, envs))

	// NewConsumer's default FetchMaxWait comes from
	// config.LoadKafka().ProduceTimeout (10s): once the first poll delivers
	// the already-produced records, a later poll with nothing new to fetch
	// would otherwise block for up to that long before retrying the ceiling
	// -- far longer than this test should need to wait for a retry cycle.
	t.Setenv("TRACELENS_PRODUCE_TIMEOUT", "200ms")
	consumer, err := NewConsumer(cfg, m, log)
	require.NoError(t, err)
	defer consumer.Close()

	var withhold atomic32
	withhold.set(1) // start withholding

	runCtx, runCancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = consumer.RunWithCommitGate(runCtx, func(context.Context, string, []*kgo.Record) error {
			return nil
		}, func(topic string, partition int32) (int64, bool) {
			if withhold.get() == 1 {
				return -1, true // hold everything back on every partition
			}
			return 0, false // no ceiling: commit whatever this poll saw
		})
	}()

	// Give it time to poll and attempt (and withhold) a commit.
	time.Sleep(2 * time.Second)

	uncommittedBefore := fetchCommittedOffset(t, ctx, cfg)

	withhold.set(0)
	time.Sleep(2 * time.Second)

	runCancel()
	<-done

	committedAfter := fetchCommittedOffset(t, ctx, cfg)
	require.Greater(t, committedAfter, uncommittedBefore,
		"clearing the ceiling must let the withheld offset finally commit")
}

// atomic32 is a tiny int flag, avoiding a sync/atomic import just for one
// test's start/stop toggle.
type atomic32 struct {
	mu sync.Mutex
	v  int
}

func (a *atomic32) set(v int) { a.mu.Lock(); a.v = v; a.mu.Unlock() }
func (a *atomic32) get() int  { a.mu.Lock(); defer a.mu.Unlock(); return a.v }

// fetchCommittedOffset reads the consumer group's committed offset for the
// spans topic via kadm -- a pure metadata query that does NOT join the
// group, so it cannot trigger a rebalance that would disturb the consumer
// under test.
func fetchCommittedOffset(t *testing.T, ctx context.Context, cfg config.Kafka) int64 {
	t.Helper()

	client, err := kgo.NewClient(kgo.SeedBrokers(cfg.Brokers...))
	require.NoError(t, err)
	defer client.Close()

	admin := kadm.NewClient(client)
	offsets, err := admin.FetchOffsets(ctx, cfg.ConsumerGroup)
	require.NoError(t, err)

	var max int64 = -1
	offsets.Each(func(o kadm.OffsetResponse) {
		if o.Topic == cfg.TopicSpans && o.At > max {
			max = o.At
		}
	})
	return max
}
