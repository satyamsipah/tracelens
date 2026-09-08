package ingest

import (
	"sync"

	"github.com/satyamsipah/tracelens/internal/config"
)

// Envelope is one Kafka-bound unit: a marshalled OTLP payload plus the
// partition key it must be routed by.
//
// Envelopes are pooled. Key and Payload are append-reused rather than
// reallocated, so after warm-up a steady-state ingest loop allocates nothing
// per span beyond growth of these two backing arrays.
type Envelope struct {
	Signal config.Signal

	// key is the Kafka partition key. For spans this is the raw 16-byte
	// trace_id and NOTHING ELSE -- see pipeline.Producer for why a nil key is
	// treated as a bug rather than a default.
	key []byte

	// payload is a marshalled otlp *Data message (TracesData, LogsData or
	// MetricsData), which keeps the wire format identical to what the client
	// sent us and avoids inventing a private encoding.
	payload []byte

	// items is the span / log record / data point count, for counters.
	items int
}

// Key returns the partition key. The slice is owned by the envelope and is
// invalid after Release.
func (e *Envelope) Key() []byte { return e.key }

// Payload returns the marshalled OTLP payload. The slice is owned by the
// envelope and is invalid after Release.
func (e *Envelope) Payload() []byte { return e.payload }

// Items reports how many spans, log records or data points this envelope
// carries, so that a drop can be counted in the units the operator cares
// about rather than in envelopes.
func (e *Envelope) Items() int { return e.items }

// Size reports the approximate broker-side cost of this envelope.
func (e *Envelope) Size() int { return len(e.key) + len(e.payload) }

// NewEnvelope builds an envelope from an explicit key and payload.
//
// The splitters are the normal producers of envelopes; this exists for
// callers that already hold an encoded payload, and for exercising downstream
// invariants -- notably the producer's refusal to emit a keyless span record,
// which cannot otherwise be reached because the splitter zero-pads every key.
func NewEnvelope(signal config.Signal, key, payload []byte, items int) *Envelope {
	e := newEnvelope(signal)
	e.key = append(e.key, key...)
	e.payload = append(e.payload, payload...)
	e.items = items
	return e
}

var envelopePool = sync.Pool{
	New: func() any { return &Envelope{} },
}

func newEnvelope(signal config.Signal) *Envelope {
	e, ok := envelopePool.Get().(*Envelope)
	if !ok {
		e = &Envelope{}
	}
	e.Signal = signal
	e.key = e.key[:0]
	e.payload = e.payload[:0]
	e.items = 0
	return e
}

// Release returns the envelope to the pool. Callers must not touch Key or
// Payload afterwards. Releasing twice is a use-after-free and will corrupt
// another goroutine's envelope, so ownership transfers with the pointer.
func (e *Envelope) Release() {
	// Keep the backing arrays; that reuse is the whole point of the pool.
	// Drop pathologically large ones so a single 16MB request does not pin
	// that much memory in the pool forever.
	const maxRetained = 1 << 20
	if cap(e.payload) > maxRetained {
		e.payload = nil
	}
	envelopePool.Put(e)
}

// ReleaseAll releases every envelope the caller still owns.
//
// nil entries are skipped: Queue.EnqueueBatch nils out the envelopes it has
// taken ownership of, so a caller can unconditionally ReleaseAll after a
// partial admission and free exactly the ones that were not accepted.
func ReleaseAll(envs []*Envelope) {
	for i, e := range envs {
		if e == nil {
			continue
		}
		e.Release()
		envs[i] = nil
	}
}
