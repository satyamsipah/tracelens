package query

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/storage"
)

func traceIDN(n byte) []byte {
	id := make([]byte, 16)
	id[15] = n
	return id
}

func spanIDN(n byte) []byte {
	id := make([]byte, 8)
	id[7] = n
	return id
}

func testSpan(service, operation, status string, durationNS uint64, weight float64, traceN, spanN byte, ts time.Time) storage.SpanRow {
	return storage.SpanRow{
		Timestamp:          ts,
		TraceID:            traceIDN(traceN),
		SpanID:             spanIDN(spanN),
		ParentSpanID:       spanIDN(0),
		ServiceName:        service,
		SpanName:           operation,
		SpanKind:           "server",
		DurationNS:         durationNS,
		StatusCode:         status,
		SamplingWeight:     weight,
		ResourceAttributes: map[string]string{},
		SpanAttributes:     map[string]string{"http.route": "/pay"},
	}
}

func TestExecuteShouldReturnWeightedCountAcrossRealClickHouse(t *testing.T) {
	conn := startClickHouse(t)
	now := time.Now().UTC()

	spans := []storage.SpanRow{
		testSpan("query-it-checkout", "charge", "error", 600_000_000, 1.0, 1, 1, now.Add(-time.Minute)),
		testSpan("query-it-checkout", "charge", "ok", 100_000_000, 20.0, 2, 1, now.Add(-time.Minute)),
		testSpan("query-it-checkout", "refund", "ok", 700_000_000, 20.0, 3, 1, now.Add(-time.Minute)),
	}
	insertTestSpans(t, conn, spans)

	dsl := `{service="query-it-checkout"} | duration > 500ms | count by (operation)`
	res, err := Execute(context.Background(), conn, dsl, DefaultOptions())
	require.NoError(t, err)

	got := map[string]float64{}
	opIdx, countIdx := colIndex(t, res, "operation"), colIndex(t, res, "count")
	for _, row := range res.Rows {
		got[toString(row[opIdx])] += toFloat64(row[countIdx])
	}
	// "charge">500ms only matches the ERROR span (weight 1); "refund" matches
	// the one OK span (weight 20) -- proving count is sum(sampling_weight),
	// not a raw row count.
	require.InDelta(t, 1.0, got["charge"], 0.001)
	require.InDelta(t, 20.0, got["refund"], 0.001)
}

func TestExecuteTraceLookupReturnsAllSpansForThatTraceOnly(t *testing.T) {
	conn := startClickHouse(t)
	now := time.Now().UTC()
	traceN := byte(200)

	spans := []storage.SpanRow{
		testSpan("query-it-trace-svc", "root", "ok", 10_000_000, 1.0, traceN, 1, now),
		testSpan("query-it-trace-svc", "child", "ok", 5_000_000, 1.0, traceN, 2, now),
		testSpan("query-it-trace-svc", "unrelated", "ok", 5_000_000, 1.0, 201, 1, now),
	}
	insertTestSpans(t, conn, spans)

	id := traceIDHex(traceN)
	res, err := Execute(context.Background(), conn, `trace("`+id+`")`, DefaultOptions())
	require.NoError(t, err)
	require.Len(t, res.Rows, 2)
}

func TestExecuteRejectsMalformedTraceID(t *testing.T) {
	conn := startClickHouse(t)
	_, err := Execute(context.Background(), conn, `trace("not-hex!")`, DefaultOptions())
	require.Error(t, err)
}

func TestExplainReportsEstimatedRowsAndOptimizedPlan(t *testing.T) {
	conn := startClickHouse(t)
	now := time.Now().UTC()
	insertTestSpans(t, conn, []storage.SpanRow{
		testSpan("query-it-explain", "op", "ok", 1_000_000, 1.0, 50, 1, now),
	})

	res, err := Explain(context.Background(), conn, `{service="query-it-explain"} | count by (operation)`)
	require.NoError(t, err)
	require.Contains(t, res.OptimizedPlan, "Aggregate")
	require.Contains(t, res.SQL, "sum(sampling_weight)")
	require.NotEmpty(t, res.ClickHouseExplain)
}

func TestExecuteMaxRowsScannedGuardRejectsRunawayQuery(t *testing.T) {
	conn := startClickHouse(t)
	now := time.Now().UTC()
	// Insert its own rows rather than relying on other tests having run
	// first -- Go doesn't guarantee cross-test ordering across files.
	spans := make([]storage.SpanRow, 5)
	for i := range spans {
		spans[i] = testSpan("query-it-guard", "op", "ok", 1_000_000, 1.0, byte(100+i), 1, now)
	}
	insertTestSpans(t, conn, spans)

	opts := Options{Timeout: 10 * time.Second, MaxRowsScanned: 1}
	_, err := Execute(context.Background(), conn, `{service="query-it-guard"}`, opts)
	require.Error(t, err)
	require.Contains(t, err.Error(), "row guard")
}

func colIndex(t *testing.T, res *Result, name string) int {
	t.Helper()
	for i, c := range res.Columns {
		if c == name {
			return i
		}
	}
	t.Fatalf("column %q not found in %v", name, res.Columns)
	return -1
}

func toFloat64(v any) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	default:
		return 0
	}
}

func toString(v any) string {
	s, _ := v.(string)
	return s
}

func traceIDHex(n byte) string {
	id := traceIDN(n)
	const hexdigits = "0123456789abcdef"
	buf := make([]byte, 32)
	for i, b := range id {
		buf[i*2] = hexdigits[b>>4]
		buf[i*2+1] = hexdigits[b&0x0f]
	}
	return string(buf)
}
