package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/query"
	"github.com/satyamsipah/tracelens/internal/storage"
)

func newTestMux(h *handler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /api/query", h.handleQuery)
	mux.HandleFunc("POST /api/explain", h.handleExplain)
	mux.HandleFunc("GET /api/trace/{id}", h.handleTrace)
	mux.HandleFunc("GET /api/services", h.handleServices)
	mux.HandleFunc("GET /api/services/graph", h.handleServiceGraph)
	return mux
}

func traceIDHex(n byte) string {
	id := make([]byte, 16)
	id[15] = n
	const hexdigits = "0123456789abcdef"
	buf := make([]byte, 32)
	for i, b := range id {
		buf[i*2] = hexdigits[b>>4]
		buf[i*2+1] = hexdigits[b&0x0f]
	}
	return string(buf)
}

func testSpan(service, operation, status string, durationNS uint64, weight float64, traceN byte, ts time.Time) storage.SpanRow {
	traceID := make([]byte, 16)
	traceID[15] = traceN
	spanID := make([]byte, 8)
	spanID[7] = 1
	return storage.SpanRow{
		Timestamp: ts, TraceID: traceID, SpanID: spanID, ParentSpanID: make([]byte, 8),
		ServiceName: service, SpanName: operation, SpanKind: "server",
		DurationNS: durationNS, StatusCode: status, SamplingWeight: weight,
		ResourceAttributes: map[string]string{}, SpanAttributes: map[string]string{},
	}
}

func TestHandleQueryExecutesWeightedAggregation(t *testing.T) {
	conn := startClickHouse(t)
	now := time.Now().UTC()
	insertTestSpans(t, conn, []storage.SpanRow{
		testSpan("cmdq-checkout", "charge", "error", 600_000_000, 1.0, 1, now),
		testSpan("cmdq-checkout", "charge", "ok", 100_000_000, 20.0, 2, now),
	})

	h := &handler{conn: conn, opts: query.DefaultOptions()}
	mux := newTestMux(h)

	body, _ := json.Marshal(queryRequest{Query: `{service="cmdq-checkout"} | duration > 500ms | count by (operation)`})
	req := httptest.NewRequest(http.MethodPost, "/api/query", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var res query.Result
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &res))
	require.Contains(t, res.Columns, "count")
}

func TestHandleQueryReturns400OnParseError(t *testing.T) {
	conn := startClickHouse(t)
	h := &handler{conn: conn, opts: query.DefaultOptions()}
	mux := newTestMux(h)

	body, _ := json.Marshal(queryRequest{Query: `{not valid`})
	req := httptest.NewRequest(http.MethodPost, "/api/query", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandleExplainReturnsLogicalAndPhysicalPlan(t *testing.T) {
	conn := startClickHouse(t)
	h := &handler{conn: conn, opts: query.DefaultOptions()}
	mux := newTestMux(h)

	body, _ := json.Marshal(queryRequest{Query: `{service="cmdq-checkout"} | count by (operation)`})
	req := httptest.NewRequest(http.MethodPost, "/api/explain", bytes.NewReader(body))
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var res query.ExplainResult
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &res))
	require.Contains(t, res.OptimizedPlan, "Aggregate")
	require.Contains(t, res.SQL, "sum(sampling_weight)")
}

func TestHandleTraceReturnsSpansForThatTraceOnly(t *testing.T) {
	conn := startClickHouse(t)
	now := time.Now().UTC()
	insertTestSpans(t, conn, []storage.SpanRow{
		testSpan("cmdq-trace-svc", "root", "ok", 10_000_000, 1.0, 77, now),
		testSpan("cmdq-trace-svc", "unrelated", "ok", 5_000_000, 1.0, 78, now),
	})

	h := &handler{conn: conn, opts: query.DefaultOptions()}
	mux := newTestMux(h)

	req := httptest.NewRequest(http.MethodGet, "/api/trace/"+traceIDHex(77), nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var res query.Result
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &res))
	require.Len(t, res.Rows, 1)
}

func TestHandleServiceGraphReturnsNodesAndEdges(t *testing.T) {
	conn := startClickHouse(t)
	now := time.Now().UTC()
	insertServiceEdgesForTest(t, conn, []storage.ServiceEdgeRow{
		{Timestamp: now, CallerService: "cmdq-graph-gateway", CalleeService: "cmdq-graph-checkout", DurationNS: 1_000_000, IsError: false, SamplingWeight: 5},
	})

	h := &handler{conn: conn, opts: query.DefaultOptions()}
	mux := newTestMux(h)

	req := httptest.NewRequest(http.MethodGet, "/api/services/graph?window=1h", nil)
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())
	var graph query.ServiceGraph
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &graph))
	require.Contains(t, graph.Nodes, "cmdq-graph-gateway")
	require.Contains(t, graph.Nodes, "cmdq-graph-checkout")
}
