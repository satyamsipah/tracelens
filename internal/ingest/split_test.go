package ingest

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
)

func traceID(b byte) []byte {
	id := make([]byte, 16)
	for i := range id {
		id[i] = b
	}
	return id
}

func spanID(b byte) []byte {
	id := make([]byte, 8)
	for i := range id {
		id[i] = b
	}
	return id
}

func makeSpan(tid, sid []byte, name string) *tracepb.Span {
	return &tracepb.Span{
		TraceId:           tid,
		SpanId:            sid,
		Name:              name,
		Kind:              tracepb.Span_SPAN_KIND_SERVER,
		StartTimeUnixNano: 1_700_000_000_000_000_000,
		EndTimeUnixNano:   1_700_000_000_500_000_000,
		Status:            &tracepb.Status{Code: tracepb.Status_STATUS_CODE_OK},
	}
}

func resourceSpans(service string, spans ...*tracepb.Span) []*tracepb.ResourceSpans {
	return []*tracepb.ResourceSpans{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{{
			Key:   ServiceNameKey,
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: service}},
		}}},
		ScopeSpans: []*tracepb.ScopeSpans{{
			Scope: &commonpb.InstrumentationScope{Name: "test", Version: "1.0.0"},
			Spans: spans,
		}},
	}}
}

func TestSplitTraces(t *testing.T) {
	tests := []struct {
		name          string
		input         []*tracepb.ResourceSpans
		wantEnvelopes int
		wantSpans     int
		wantKeys      [][]byte
	}{
		{
			name: "should emit one envelope per trace when a batch carries several traces",
			input: resourceSpans("checkout",
				makeSpan(traceID(0xAA), spanID(0x01), "a"),
				makeSpan(traceID(0xBB), spanID(0x02), "b"),
				makeSpan(traceID(0xAA), spanID(0x03), "c"),
			),
			wantEnvelopes: 2,
			wantSpans:     3,
			wantKeys:      [][]byte{traceID(0xAA), traceID(0xBB)},
		},
		{
			name: "should keep every span of one trace in a single envelope when spans are interleaved",
			input: resourceSpans("checkout",
				makeSpan(traceID(0xAA), spanID(0x01), "a"),
				makeSpan(traceID(0xBB), spanID(0x02), "b"),
				makeSpan(traceID(0xAA), spanID(0x03), "c"),
				makeSpan(traceID(0xBB), spanID(0x04), "d"),
				makeSpan(traceID(0xAA), spanID(0x05), "e"),
			),
			wantEnvelopes: 2,
			wantSpans:     5,
			wantKeys:      [][]byte{traceID(0xAA), traceID(0xBB)},
		},
		{
			name: "should group duplicate spans together when the same span arrives twice",
			input: resourceSpans("checkout",
				makeSpan(traceID(0xCC), spanID(0x01), "a"),
				makeSpan(traceID(0xCC), spanID(0x01), "a"),
			),
			wantEnvelopes: 1,
			wantSpans:     2, // dedup is storage's job, not the splitter's
			wantKeys:      [][]byte{traceID(0xCC)},
		},
		{
			name: "should zero-pad the key when a span carries a malformed short trace id",
			input: resourceSpans("checkout",
				makeSpan([]byte{0x01, 0x02}, spanID(0x01), "a"),
			),
			wantEnvelopes: 1,
			wantSpans:     1,
			wantKeys:      [][]byte{append([]byte{0x01, 0x02}, make([]byte, 14)...)},
		},
		{
			name:          "should emit nothing when the request carries no spans",
			input:         resourceSpans("checkout"),
			wantEnvelopes: 0,
			wantSpans:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := GetSplitter()
			defer s.Release()

			envs, spans, err := s.SplitTraces(tt.input, nil)
			require.NoError(t, err)
			defer ReleaseAll(envs)

			require.Len(t, envs, tt.wantEnvelopes)
			require.Equal(t, tt.wantSpans, spans)

			for _, want := range tt.wantKeys {
				found := false
				for _, e := range envs {
					if bytes.Equal(e.Key(), want) {
						found = true
					}
				}
				require.True(t, found, "expected an envelope keyed %x", want)
			}

			// Item counts across envelopes must total the span count, or a
			// rejection would be attributed to the wrong number of spans.
			total := 0
			for _, e := range envs {
				total += e.Items()
			}
			require.Equal(t, tt.wantSpans, total)
		})
	}
}

func TestSplitTracesPreservesPayload(t *testing.T) {
	t.Run("should preserve resource and scope on every envelope when splitting", func(t *testing.T) {
		s := GetSplitter()
		defer s.Release()

		input := resourceSpans("checkout",
			makeSpan(traceID(0xAA), spanID(0x01), "first"),
			makeSpan(traceID(0xBB), spanID(0x02), "second"),
		)

		envs, _, err := s.SplitTraces(input, nil)
		require.NoError(t, err)
		defer ReleaseAll(envs)
		require.Len(t, envs, 2)

		for _, e := range envs {
			var td tracepb.TracesData
			require.NoError(t, proto.Unmarshal(e.Payload(), &td))
			require.Len(t, td.GetResourceSpans(), 1)

			rs := td.GetResourceSpans()[0]
			require.Equal(t, "checkout", ServiceName(rs.GetResource()),
				"resource must survive the split or the row loses its service")
			require.Len(t, rs.GetScopeSpans(), 1)
			require.Equal(t, "test", rs.GetScopeSpans()[0].GetScope().GetName())

			// Every span in one envelope must share the envelope's key.
			for _, span := range rs.GetScopeSpans()[0].GetSpans() {
				require.True(t, bytes.Equal(span.GetTraceId(), e.Key()),
					"a span routed under another trace's key would reach the wrong assembler")
			}
		}
	})
}

func TestSplitTracesRoutingInvariant(t *testing.T) {
	t.Run("should give identical keys when one trace spans two resources", func(t *testing.T) {
		s := GetSplitter()
		defer s.Release()

		shared := traceID(0xDD)
		input := []*tracepb.ResourceSpans{
			resourceSpans("gateway", makeSpan(shared, spanID(0x01), "in"))[0],
			resourceSpans("checkout", makeSpan(shared, spanID(0x02), "out"))[0],
		}

		envs, spans, err := s.SplitTraces(input, nil)
		require.NoError(t, err)
		defer ReleaseAll(envs)

		require.Len(t, envs, 2, "one envelope per resource")
		require.Equal(t, 2, spans)
		require.True(t, bytes.Equal(envs[0].Key(), envs[1].Key()),
			"identical keys hash to identical partitions, so the trace still converges on one assembler")
	})
}

func TestSplitMetrics(t *testing.T) {
	t.Run("should key every metric of one service deterministically", func(t *testing.T) {
		a := appendSeriesKey(nil, "checkout", "http.server.duration")
		b := appendSeriesKey(nil, "checkout", "http.server.duration")
		c := appendSeriesKey(nil, "checkout", "http.server.requests")

		require.True(t, bytes.Equal(a, b), "the same metric must always route to the same partition")
		require.False(t, bytes.Equal(a, c))
		require.Len(t, a, 8)
	})
}
