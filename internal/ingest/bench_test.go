package ingest

import (
	"context"
	"fmt"
	"testing"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"

	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

// benchBatch builds a request shaped like a real OTLP export: one resource,
// several traces, a handful of spans each.
func benchBatch(traces, spansPerTrace int) []*tracepb.ResourceSpans {
	spans := make([]*tracepb.Span, 0, traces*spansPerTrace)
	for tr := 0; tr < traces; tr++ {
		tid := traceID(byte(tr + 1))
		for s := 0; s < spansPerTrace; s++ {
			spans = append(spans, makeSpan(tid, spanID(byte(s+1)), "GET /checkout"))
		}
	}
	return resourceSpans("checkout", spans...)
}

// BenchmarkSplitTraces measures the per-span cost of the grouping and marshal
// step, which runs once per span on the ingest hot path.
//
// Report allocs/op and B/op: the pooling in envelope.go and split.go exists
// specifically to hold these flat as the batch grows.
func BenchmarkSplitTraces(b *testing.B) {
	cases := []struct{ traces, spansPerTrace int }{
		{1, 10},
		{10, 10},
		{50, 4},
	}

	for _, c := range cases {
		b.Run(fmt.Sprintf("traces=%d/spans=%d", c.traces, c.spansPerTrace), func(b *testing.B) {
			input := benchBatch(c.traces, c.spansPerTrace)
			totalSpans := c.traces * c.spansPerTrace

			b.ReportAllocs()
			b.ResetTimer()

			for i := 0; i < b.N; i++ {
				s := GetSplitter()
				envs := envelopeBufGet()
				envs, _, err := s.SplitTraces(input, envs)
				if err != nil {
					b.Fatal(err)
				}
				ReleaseAll(envs)
				envelopeBufPut(envs)
				s.Release()
			}

			b.ReportMetric(float64(totalSpans), "spans/op")
		})
	}
}

// BenchmarkEnqueueBatch measures the admission path in isolation.
func BenchmarkEnqueueBatch(b *testing.B) {
	m := observability.NewMetrics()
	q := NewQueue(queueCfg(1<<16, 1.0, config.PolicyReject), config.SignalTraces, m)

	batch := make([]*Envelope, 8)

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := range batch {
			batch[j] = testEnvelope(1)
		}
		if err := q.EnqueueBatch(batch); err != nil {
			// Drain so the benchmark keeps measuring admission, not rejection.
			b.StopTimer()
			for {
				e, ok := q.TryDequeue()
				if !ok {
					break
				}
				e.Release()
			}
			b.StartTimer()
		}
	}
}

// BenchmarkSplitAndEnqueue measures the full receiver hot path end to end.
func BenchmarkSplitAndEnqueue(b *testing.B) {
	m := observability.NewMetrics()
	cfg := config.Collector{
		Queue: queueCfg(1<<16, 1.0, config.PolicyReject),
	}
	r := NewReceiver(cfg, m, observability.NewLogger("bench"))
	input := benchBatch(10, 10)

	// Drain continuously so the queue never saturates and the benchmark keeps
	// measuring admission rather than rejection.
	stop := make(chan struct{})
	drained := make(chan struct{})
	go func() {
		defer close(drained)
		q := r.Queue(config.SignalTraces)
		for {
			select {
			case <-stop:
				return
			default:
			}
			if e, ok := q.TryDequeue(); ok {
				e.Release()
			}
		}
	}()
	b.Cleanup(func() {
		close(stop)
		<-drained
	})

	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := r.AcceptTraces(ctx, input); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
}
