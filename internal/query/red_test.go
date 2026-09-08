package query

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/tracelens/internal/storage"
)

func TestQueryREDReadsFromRollupNotRawSpans(t *testing.T) {
	conn := startClickHouse(t)
	now := time.Now().UTC()

	spans := []storage.SpanRow{
		testSpan("query-red-svc", "checkout", "error", 600_000_000, 1.0, 1, 1, now),
		testSpan("query-red-svc", "checkout", "ok", 100_000_000, 20.0, 2, 1, now),
	}
	insertTestSpans(t, conn, spans)

	stats, err := QueryRED(context.Background(), conn, Rollup1m, "query-red-svc", "checkout",
		now.Add(-time.Hour), now.Add(time.Hour))
	require.NoError(t, err)
	require.NotEmpty(t, stats, "the 1m rollup materialized view should have populated from the spans insert")

	var totalCalls, totalErrors float64
	for _, s := range stats {
		totalCalls += s.Calls
		totalErrors += s.Calls * s.ErrorRate
	}
	require.InDelta(t, 21.0, totalCalls, 0.001, "weighted call count: 1.0 (error) + 20.0 (ok)")
	require.InDelta(t, 1.0, totalErrors, 0.001, "only the error span's weight should count as errors")
}

func TestQueryREDRejectsUnknownLevel(t *testing.T) {
	conn := startClickHouse(t)
	_, err := QueryRED(context.Background(), conn, RollupLevel("bogus"), "svc", "", time.Now().Add(-time.Hour), time.Now())
	require.Error(t, err)
}
