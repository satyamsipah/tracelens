package ingest

import (
	"context"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
)

// queueCfg builds a queue config with an exact, predictable high-water mark.
func queueCfg(capacity int, ratio float64, policy config.BackpressurePolicy) config.QueueConfig {
	return config.QueueConfig{
		Capacity:       capacity,
		HighWaterRatio: ratio,
		Policy:         map[config.Signal]config.BackpressurePolicy{config.SignalTraces: policy},
	}
}

// testEnvelope builds an envelope carrying a known item count, so drop
// counters can be asserted in spans rather than in envelopes.
func testEnvelope(items int) *Envelope {
	e := newEnvelope(config.SignalTraces)
	e.key = append(e.key, make([]byte, 16)...)
	e.payload = append(e.payload, 0x01, 0x02)
	e.items = items
	return e
}

func TestQueueBackpressure(t *testing.T) {
	tests := []struct {
		name string
		// capacity and ratio fix the high-water mark exactly.
		capacity    int
		ratio       float64
		itemsPer    int
		admitFirst  int
		wantDepth   int
		wantDropped float64
	}{
		{
			name:        "should admit up to the high water mark when below capacity",
			capacity:    10,
			ratio:       0.8, // high water = 8
			itemsPer:    1,
			admitFirst:  8,
			wantDepth:   8,
			wantDropped: 0,
		},
		{
			name:        "should reject before the hard cap when the high water mark is reached",
			capacity:    10,
			ratio:       0.8,
			itemsPer:    1,
			admitFirst:  9, // the 9th is refused with 2 slots still physically free
			wantDepth:   8,
			wantDropped: 1,
		},
		{
			name:        "should count dropped spans not dropped envelopes when rejecting",
			capacity:    4,
			ratio:       0.5, // high water = 2
			itemsPer:    25,
			admitFirst:  5, // 2 admitted, 3 rejected
			wantDepth:   2,
			wantDropped: 75, // exactly 3 envelopes x 25 spans
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := observability.NewMetrics()
			q := NewQueue(queueCfg(tt.capacity, tt.ratio, config.PolicyReject), config.SignalTraces, m)

			rejected := 0
			for i := 0; i < tt.admitFirst; i++ {
				if err := q.Enqueue(testEnvelope(tt.itemsPer)); err != nil {
					require.ErrorIs(t, err, ErrQueueFull)
					rejected++
				}
			}

			require.Equal(t, tt.wantDepth, q.Depth(), "queue depth")
			require.Equal(t, tt.wantDropped,
				testutil.ToFloat64(m.DroppedFor("traces", observability.ReasonRejected)),
				"otlp_spans_dropped_total must move by exactly the rejected span count")
		})
	}
}

func TestQueueEnqueueBatchIsAtomic(t *testing.T) {
	tests := []struct {
		name        string
		capacity    int
		ratio       float64
		batchSize   int
		itemsPer    int
		wantErr     bool
		wantDepth   int
		wantDropped float64
	}{
		{
			name:      "should admit the whole batch when it fits under the high water mark",
			capacity:  20,
			ratio:     0.8, // high water = 16
			batchSize: 16,
			itemsPer:  3,
			wantErr:   false,
			wantDepth: 16,
		},
		{
			name:        "should admit nothing when the batch would cross the high water mark",
			capacity:    20,
			ratio:       0.8,
			batchSize:   17,
			itemsPer:    3,
			wantErr:     true,
			wantDepth:   0, // partial admission would truncate a trace
			wantDropped: 51,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := observability.NewMetrics()
			q := NewQueue(queueCfg(tt.capacity, tt.ratio, config.PolicyReject), config.SignalTraces, m)

			batch := make([]*Envelope, 0, tt.batchSize)
			for i := 0; i < tt.batchSize; i++ {
				batch = append(batch, testEnvelope(tt.itemsPer))
			}

			err := q.EnqueueBatch(batch)
			if tt.wantErr {
				require.ErrorIs(t, err, ErrQueueFull)
			} else {
				require.NoError(t, err)
			}

			require.Equal(t, tt.wantDepth, q.Depth())
			require.Equal(t, tt.wantDropped,
				testutil.ToFloat64(m.DroppedFor("traces", observability.ReasonRejected)))

			// Whatever was refused is still owned by the caller and must be
			// releasable without double-freeing what the queue took.
			ReleaseAll(batch)
		})
	}
}

func TestQueueDropOldest(t *testing.T) {
	t.Run("should evict the oldest and count it when the buffer is full", func(t *testing.T) {
		m := observability.NewMetrics()
		q := NewQueue(queueCfg(4, 1.0, config.PolicyDropOldest), config.SignalTraces, m)

		const overshoot = 6
		for i := 0; i < overshoot; i++ {
			require.NoError(t, q.Enqueue(testEnvelope(1)),
				"drop-oldest never reports failure to the client -- that is its defining hazard")
		}

		require.Equal(t, 4, q.Depth())
		require.Equal(t, float64(overshoot-4),
			testutil.ToFloat64(m.DroppedFor("traces", observability.ReasonQueueFull)),
			"every eviction must be counted even though the client was told OK")
	})
}

func TestQueueDequeue(t *testing.T) {
	t.Run("should return envelopes in FIFO order when draining", func(t *testing.T) {
		m := observability.NewMetrics()
		q := NewQueue(queueCfg(8, 1.0, config.PolicyReject), config.SignalTraces, m)

		for i := 1; i <= 3; i++ {
			require.NoError(t, q.Enqueue(testEnvelope(i)))
		}

		ctx := context.Background()
		for i := 1; i <= 3; i++ {
			e, ok := q.Dequeue(ctx)
			require.True(t, ok)
			require.Equal(t, i, e.Items())
			e.Release()
		}
		require.Equal(t, 0, q.Depth())
	})

	t.Run("should report not-ok when the context is cancelled", func(t *testing.T) {
		m := observability.NewMetrics()
		q := NewQueue(queueCfg(8, 1.0, config.PolicyReject), config.SignalTraces, m)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, ok := q.Dequeue(ctx)
		require.False(t, ok)
	})
}
