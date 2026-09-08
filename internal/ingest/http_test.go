package ingest

import (
	"bytes"
	"compress/gzip"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
	rpcstatus "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/proto"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"

	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
)

func newTestHTTPServer(t *testing.T, capacity int, ratio float64) (http.Handler, *Receiver, *observability.Metrics) {
	t.Helper()

	m := observability.NewMetrics()
	cfg := config.Collector{
		MaxRecvBytes: 4 << 20,
		Queue: config.QueueConfig{
			Capacity:       capacity,
			HighWaterRatio: ratio,
			RetryAfter:     2_000_000_000, // 2s
			Policy: map[config.Signal]config.BackpressurePolicy{
				config.SignalTraces: config.PolicyReject,
			},
		},
	}
	r := NewReceiver(cfg, m, observability.NewLogger("test"))
	srv := NewHTTPServer(cfg, r, m, observability.NewLogger("test"))
	return srv.Handler(), r, m
}

func traceRequestBody(t *testing.T, traces int) []byte {
	t.Helper()
	req := &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: resourceSpans("checkout"),
	}
	for i := 0; i < traces; i++ {
		req.ResourceSpans[0].ScopeSpans[0].Spans = append(
			req.ResourceSpans[0].ScopeSpans[0].Spans,
			makeSpan(traceID(byte(i+1)), spanID(0x01), "GET /checkout"),
		)
	}
	body, err := proto.Marshal(req)
	require.NoError(t, err)
	return body
}

func gzipBytes(t *testing.T, in []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	_, err := zw.Write(in)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func zstdBytes(t *testing.T, in []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	zw, err := zstd.NewWriter(&buf)
	require.NoError(t, err)
	_, err = zw.Write(in)
	require.NoError(t, err)
	require.NoError(t, zw.Close())
	return buf.Bytes()
}

func TestHTTPContentEncoding(t *testing.T) {
	raw := traceRequestBody(t, 3)

	tests := []struct {
		name     string
		encoding string
		body     func(*testing.T, []byte) []byte
		wantCode int
	}{
		{
			name:     "should accept an uncompressed protobuf body when no encoding is set",
			encoding: "",
			body:     func(_ *testing.T, b []byte) []byte { return b },
			wantCode: http.StatusOK,
		},
		{
			name:     "should accept a gzip body when Content-Encoding is gzip",
			encoding: "gzip",
			body:     gzipBytes,
			wantCode: http.StatusOK,
		},
		{
			name:     "should accept a zstd body when Content-Encoding is zstd",
			encoding: "zstd",
			body:     zstdBytes,
			wantCode: http.StatusOK,
		},
		{
			name:     "should reject an unknown encoding when Content-Encoding is unsupported",
			encoding: "br",
			body:     func(_ *testing.T, b []byte) []byte { return b },
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler, _, _ := newTestHTTPServer(t, 1024, 1.0)

			req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(tt.body(t, raw)))
			req.Header.Set("Content-Type", protobufContentType)
			if tt.encoding != "" {
				req.Header.Set("Content-Encoding", tt.encoding)
			}

			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, req)
			require.Equal(t, tt.wantCode, rec.Code, "body: %s", rec.Body.String())
		})
	}
}

func TestHTTPContentType(t *testing.T) {
	t.Run("should reject JSON when only protobuf is implemented", func(t *testing.T) {
		handler, _, _ := newTestHTTPServer(t, 1024, 1.0)

		req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader([]byte("{}")))
		req.Header.Set("Content-Type", "application/json")

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)
		require.Equal(t, http.StatusUnsupportedMediaType, rec.Code)
	})
}

func TestHTTPBackpressure(t *testing.T) {
	t.Run("should answer 429 with Retry-After when the queue is saturated", func(t *testing.T) {
		// Capacity 2 at ratio 1.0 means the third distinct trace is refused.
		handler, _, m := newTestHTTPServer(t, 2, 1.0)
		body := traceRequestBody(t, 5) // 5 traces -> 5 envelopes, over the cap

		req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(body))
		req.Header.Set("Content-Type", protobufContentType)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusTooManyRequests, rec.Code)
		require.NotEmpty(t, rec.Header().Get("Retry-After"),
			"a retryable rejection without Retry-After leaves the client guessing")

		// The body must be a google.rpc.Status carrying RESOURCE_EXHAUSTED.
		var st rpcstatus.Status
		raw, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		require.NoError(t, proto.Unmarshal(raw, &st))
		require.Equal(t, int32(codes.ResourceExhausted), st.GetCode())
		require.NotEmpty(t, st.GetDetails(), "RetryInfo detail must be attached")

		// Whole-request rejection: all 5 spans counted, none admitted.
		require.Equal(t, float64(5),
			promValue(t, m, "traces", observability.ReasonRejected),
			"the drop counter must move by the full request, not a partial prefix")
	})

	t.Run("should admit the request when the queue has room", func(t *testing.T) {
		handler, r, m := newTestHTTPServer(t, 64, 1.0)
		body := traceRequestBody(t, 5)

		req := httptest.NewRequest(http.MethodPost, "/v1/traces", bytes.NewReader(body))
		req.Header.Set("Content-Type", protobufContentType)

		rec := httptest.NewRecorder()
		handler.ServeHTTP(rec, req)

		require.Equal(t, http.StatusOK, rec.Code)
		require.Equal(t, 5, r.Queue(config.SignalTraces).Depth())
		require.Equal(t, float64(0), promValue(t, m, "traces", observability.ReasonRejected))

		// The response must be a valid, empty ExportTraceServiceResponse.
		var resp coltracepb.ExportTraceServiceResponse
		raw, err := io.ReadAll(rec.Body)
		require.NoError(t, err)
		require.NoError(t, proto.Unmarshal(raw, &resp))
		require.Nil(t, resp.GetPartialSuccess())
	})
}
