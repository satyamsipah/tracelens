package ingest

import (
	"context"
	"errors"
	"sync/atomic"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
)

// ErrQueueFull is returned by Enqueue when the bounded queue has reached its
// high-water mark under the reject policy. The receiver translates it into
// gRPC RESOURCE_EXHAUSTED or HTTP 429, both of which the OTLP spec designates
// retryable.
var ErrQueueFull = errors.New("ingest queue saturated")

// Queue is the bounded buffer between the OTLP receiver and the Kafka
// producer. It is THE backpressure point of the collector: everything
// upstream of it is unbounded network input and everything downstream of it
// is bounded work.
type Queue struct {
	ch   chan *Envelope
	hard int
	high int

	policy config.BackpressurePolicy
	signal config.Signal

	depth atomic.Int64

	// Pre-resolved counters. These fire per rejected request, and in the
	// drop-oldest case per evicted envelope, so no label lookup here.
	dropped  prometheus.Counter
	depthGge prometheus.Gauge
}

// NewQueue builds the bounded queue for one signal.
func NewQueue(cfg config.QueueConfig, signal config.Signal, m *observability.Metrics) *Queue {
	policy := cfg.PolicyFor(signal)

	reason := observability.ReasonRejected
	if policy == config.PolicyDropOldest {
		reason = observability.ReasonQueueFull
	}

	q := &Queue{
		ch:       make(chan *Envelope, cfg.Capacity),
		hard:     cfg.Capacity,
		high:     cfg.HighWater(),
		policy:   policy,
		signal:   signal,
		dropped:  m.DroppedFor(string(signal), reason),
		depthGge: m.QueueDepth.WithLabelValues(string(signal)),
	}
	m.QueueCapacity.WithLabelValues(string(signal)).Set(float64(cfg.Capacity))
	return q
}

// Policy reports the configured saturation policy.
func (q *Queue) Policy() config.BackpressurePolicy { return q.policy }

// Depth reports the current queue depth.
func (q *Queue) Depth() int { return int(q.depth.Load()) }

// HighWater reports the depth at which the reject policy engages.
func (q *Queue) HighWater() int { return q.high }

// Enqueue admits one envelope, or reports that it could not be admitted.
//
// Under PolicyReject the queue refuses at the HIGH-WATER mark rather than at
// the hard cap. Rejecting early leaves headroom for batches already in flight
// to land, so the queue degrades smoothly instead of wedging at exactly full.
//
// Ownership: on success the queue owns the envelope. On error the caller
// still owns it and must Release it.
func (q *Queue) Enqueue(e *Envelope) error {
	if q.policy == config.PolicyDropOldest {
		return q.enqueueDropOldest(e)
	}

	if q.depth.Load() >= int64(q.high) {
		q.dropped.Add(float64(e.items))
		return ErrQueueFull
	}
	select {
	case q.ch <- e:
		q.depthGge.Set(float64(q.depth.Add(1)))
		return nil
	default:
		// Lost a race against other producers between the check and the send.
		q.dropped.Add(float64(e.items))
		return ErrQueueFull
	}
}

// EnqueueBatch admits every envelope derived from one export request, or
// none of them.
//
// This is the whole-batch atomicity the reject policy depends on. Admitting a
// prefix would leave the client believing it delivered a complete trace while
// the assembler sees a truncated one -- and a truncated trace is invisible
// downstream in a way a missing trace is not.
//
// Ownership: on success the queue owns every envelope. On error the caller
// still owns all of them.
func (q *Queue) EnqueueBatch(envs []*Envelope) error {
	if len(envs) == 0 {
		return nil
	}
	if q.policy == config.PolicyDropOldest {
		for i, e := range envs {
			_ = q.enqueueDropOldest(e)
			envs[i] = nil // ownership transferred
		}
		return nil
	}

	// One admission decision for the whole request. The 20% of capacity above
	// the high-water mark is deliberate slack: it absorbs races between
	// concurrent requests passing this check simultaneously, so the
	// non-blocking sends below cannot fail in practice.
	if q.depth.Load()+int64(len(envs)) > int64(q.high) {
		items := 0
		for _, e := range envs {
			items += e.items
		}
		q.dropped.Add(float64(items))
		return ErrQueueFull
	}

	for i, e := range envs {
		select {
		case q.ch <- e:
			q.depth.Add(1)
			envs[i] = nil // ownership transferred to the queue
		default:
			// Slack exhausted by concurrent producers. Count only what did
			// not make it in; the prefix is already owned by the queue.
			items := 0
			for _, rest := range envs[i:] {
				items += rest.items
			}
			q.dropped.Add(float64(items))
			q.depthGge.Set(float64(q.depth.Load()))
			return ErrQueueFull
		}
	}
	q.depthGge.Set(float64(q.depth.Load()))
	return nil
}

// enqueueDropOldest evicts the head to make room and always reports success.
//
// The eviction is counted, but the CLIENT is not told: it receives OK and can
// never retry what was discarded. That asymmetry is the entire reason this is
// not the default for traces -- and note that evicting the OLDEST entry
// preferentially discards the earliest spans of in-flight traces, so the
// damage lands as truncated traces rather than as missing ones.
func (q *Queue) enqueueDropOldest(e *Envelope) error {
	for {
		select {
		case q.ch <- e:
			q.depthGge.Set(float64(q.depth.Add(1)))
			return nil
		default:
		}

		select {
		case old := <-q.ch:
			q.depth.Add(-1)
			q.dropped.Add(float64(old.items))
			old.Release()
		default:
			// Drained by a consumer in between; loop and retry the send.
		}
	}
}

// Dequeue blocks until an envelope is available, the queue is closed, or the
// context is cancelled. The second return value is false when the queue is
// drained and closed.
func (q *Queue) Dequeue(ctx context.Context) (*Envelope, bool) {
	select {
	case e, ok := <-q.ch:
		if !ok {
			return nil, false
		}
		q.depthGge.Set(float64(q.depth.Add(-1)))
		return e, true
	case <-ctx.Done():
		return nil, false
	}
}

// TryDequeue takes an envelope without blocking.
func (q *Queue) TryDequeue() (*Envelope, bool) {
	select {
	case e, ok := <-q.ch:
		if !ok {
			return nil, false
		}
		q.depthGge.Set(float64(q.depth.Add(-1)))
		return e, true
	default:
		return nil, false
	}
}

// Close stops further admission. Drain the queue after closing to avoid
// discarding data that was already accepted from a client -- we told them OK,
// so losing it here would be exactly the silent loss the design forbids.
func (q *Queue) Close() { close(q.ch) }
