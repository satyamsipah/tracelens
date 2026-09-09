package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
)

func spanRow(traceID, spanID byte, service, name string, ts time.Time, duration uint64) SpanRow {
	tid := make([]byte, 16)
	sid := make([]byte, 8)
	for i := range tid {
		tid[i] = traceID
	}
	for i := range sid {
		sid[i] = spanID
	}
	return SpanRow{
		Timestamp:          ts,
		TraceID:            tid,
		SpanID:             sid,
		ParentSpanID:       make([]byte, 8),
		ServiceName:        service,
		SpanName:           name,
		SpanKind:           "server",
		DurationNS:         duration,
		StatusCode:         "ok",
		StatusMessage:      "",
		ResourceAttributes: map[string]string{"service.name": service, "host.name": "test-0"},
		SpanAttributes:     map[string]string{"http.request.method": "GET"},
		SamplingWeight:     1,
		EventTimestamps:    []time.Time{},
		EventNames:         []string{},
		EventAttributes:    []map[string]string{},
		LinkTraceIDs:       [][]byte{},
		LinkSpanIDs:        [][]byte{},
		LinkAttributes:     []map[string]string{},
	}
}

// TestSchemaDeclaresCodecOnEveryColumn enforces principle 5 as a test rather
// than as a review habit: a column added later without a CODEC fails CI.
func TestSchemaDeclaresCodecOnEveryColumn(t *testing.T) {
	conn, _ := startClickHouse(t)
	ctx := context.Background()

	for _, table := range []string{"spans", "logs", "metrics", "metrics_rollup_1m"} {
		t.Run(fmt.Sprintf("should declare an explicit codec on every column of %s", table), func(t *testing.T) {
			rows, err := conn.Query(ctx, `
				SELECT name
				FROM system.columns
				WHERE database = 'tracelens' AND table = ? AND compression_codec = ''
				ORDER BY name`, table)
			require.NoError(t, err)
			defer func() { _ = rows.Close() }()

			var missing []string
			for rows.Next() {
				var name string
				require.NoError(t, rows.Scan(&name))
				missing = append(missing, name)
			}
			require.NoError(t, rows.Err())
			require.Empty(t, missing, "columns without an explicit CODEC: %v", missing)
		})
	}
}

func TestSchemaHasTTL(t *testing.T) {
	conn, _ := startClickHouse(t)
	ctx := context.Background()

	t.Run("should define a TTL on every table when the schema is applied", func(t *testing.T) {
		for _, table := range []string{"spans", "logs", "metrics", "metrics_rollup_1m"} {
			var expr string
			require.NoError(t, conn.QueryRow(ctx, `
				SELECT engine_full
				FROM system.tables
				WHERE database = 'tracelens' AND name = ?`, table).Scan(&expr))
			require.Contains(t, expr, "TTL", "table %s has no TTL", table)
		}
	})
}

func TestWriterInsertsSpans(t *testing.T) {
	conn, cfg := startClickHouse(t)
	w, m := newTestWriter(t, conn, cfg)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Second)

	t.Run("should store and read back spans when a batch is written", func(t *testing.T) {
		f := NewFlush("batch-insert-1")
		f.Spans = []SpanRow{
			spanRow(0xA1, 0x01, "checkout", "GET /checkout", base, 1_500_000),
			spanRow(0xA1, 0x02, "inventory", "SELECT stock", base.Add(time.Millisecond), 800_000),
		}

		require.NoError(t, w.Submit(ctx, f))
		require.NoError(t, f.Wait(ctx))

		require.Equal(t, uint64(2),
			countSpans(t, conn, "A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1", false))

		var service, name, kind, status string
		var duration uint64
		var weight float64
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT service_name, span_name, span_kind, status_code, duration_ns, sampling_weight
			FROM tracelens.spans
			WHERE trace_id = unhex(?) AND service_name = 'checkout'
			LIMIT 1`, "A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1A1").
			Scan(&service, &name, &kind, &status, &duration, &weight))

		require.Equal(t, "checkout", service)
		require.Equal(t, "GET /checkout", name)
		require.Equal(t, "server", kind)
		require.Equal(t, "ok", status)
		require.Equal(t, uint64(1_500_000), duration)
		require.Equal(t, float64(1), weight,
			"phase 1 admits everything, so every span must weigh exactly 1")

		require.Equal(t, float64(2),
			testutil.ToFloat64(m.RowsInserted.WithLabelValues(TableSpans)),
			"insert throughput must be observable, not inferred")
	})
}

func TestWriterHandlesOutOfOrderInput(t *testing.T) {
	conn, cfg := startClickHouse(t)
	w, _ := newTestWriter(t, conn, cfg)
	ctx := context.Background()

	t.Run("should store every span when timestamps arrive out of order", func(t *testing.T) {
		base := time.Now().UTC().Truncate(time.Second)

		// Deliberately regressing timestamps: a late-arriving span from a slow
		// service is the normal case, not the exceptional one.
		f := NewFlush("out-of-order-1")
		f.Spans = []SpanRow{
			spanRow(0xB1, 0x03, "payments", "authorize", base.Add(5*time.Second), 100),
			spanRow(0xB1, 0x01, "gateway", "GET /", base, 100),
			spanRow(0xB1, 0x02, "checkout", "POST /orders", base.Add(2*time.Second), 100),
		}

		require.NoError(t, w.Submit(ctx, f))
		require.NoError(t, f.Wait(ctx))

		require.Equal(t, uint64(3),
			countSpans(t, conn, "B1B1B1B1B1B1B1B1B1B1B1B1B1B1B1B1", false),
			"out-of-order arrival must not lose spans; the sort key reorders on merge")
	})
}

func TestWriterDeduplicatesDuplicateInput(t *testing.T) {
	conn, cfg := startClickHouse(t)
	w, m := newTestWriter(t, conn, cfg)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Second)

	t.Run("should ignore the second insert when the same batch replays with the same token", func(t *testing.T) {
		rows := []SpanRow{
			spanRow(0xC1, 0x01, "gateway", "GET /", base, 100),
			spanRow(0xC1, 0x02, "checkout", "POST /orders", base.Add(time.Millisecond), 200),
		}

		// Same token twice: this is exactly a Kafka redelivery after a crash
		// between the insert and the offset commit.
		for i := 0; i < 2; i++ {
			f := NewFlush("replayed-kafka-batch")
			f.Spans = append([]SpanRow(nil), rows...)
			require.NoError(t, w.Submit(ctx, f))
			require.NoError(t, f.Wait(ctx))
		}

		require.Equal(t, uint64(2),
			countSpans(t, conn, "C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1C1", false),
			"insert_deduplication_token must make a replayed batch a no-op")
	})

	t.Run("should collapse duplicate spans under FINAL when tokens differ", func(t *testing.T) {
		row := spanRow(0xD1, 0x01, "gateway", "GET /dup", base, 100)

		// Different tokens defeat server-side insert dedup, which is what
		// happens when the same span reaches us via two different Kafka
		// batches. ReplacingMergeTree is the second line of defence.
		before := testutil.ToFloat64(m.RowsInserted.WithLabelValues(TableSpans))
		for i := 0; i < 3; i++ {
			f := NewFlush(fmt.Sprintf("distinct-token-%d", i))
			f.Spans = []SpanRow{row}
			require.NoError(t, w.Submit(ctx, f))
			require.NoError(t, f.Wait(ctx))
		}

		// Assert that all three inserts actually reached the server, rather
		// than that three rows are still individually visible.
		//
		// The raw pre-merge count was the original assertion here and it is
		// genuinely flaky: it asserts on ClickHouse's background merge
		// SCHEDULING, not on our code. On an idle, fast runner the three
		// single-row parts merge before the count runs, and the test failed
		// in CI with 1 where it passed locally with 3. Merge timing is the
		// scheduler's business; what this test actually cares about is that
		// distinct tokens defeat server-side insert dedup (below) and that
		// ReplacingMergeTree still collapses the result (after).
		require.Equal(t, float64(3),
			testutil.ToFloat64(m.RowsInserted.WithLabelValues(TableSpans))-before,
			"distinct tokens must defeat insert dedup, so all three rows are inserted")

		require.Equal(t, uint64(1),
			countSpans(t, conn, "D1D1D1D1D1D1D1D1D1D1D1D1D1D1D1D1", true),
			"ReplacingMergeTree collapses on (service, name, ts, trace_id, span_id)")
	})
}

func TestWriterInsertsLogsAndMetrics(t *testing.T) {
	conn, cfg := startClickHouse(t)
	w, _ := newTestWriter(t, conn, cfg)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Second)

	t.Run("should store logs and metrics when written in one flush", func(t *testing.T) {
		f := NewFlush("logs-metrics-1")
		const metricName = "test.logs_and_metrics.duration"
		labels := map[string]string{"route": "/orders"}

		f.Logs = []LogRow{{
			Timestamp:      base,
			TraceID:        make([]byte, 16),
			SpanID:         make([]byte, 8),
			ServiceName:    "logs-metrics-svc",
			SeverityNumber: 17,
			SeverityText:   "ERROR",
			Body:           "payment authorisation failed",
			TemplateID:     0,
			Params:         []string{},
			LogAttributes:  map[string]string{"code": "AUTH_DECLINED"},
		}}
		f.Metrics = []MetricRow{{
			Timestamp:   base,
			ServiceName: "logs-metrics-svc",
			MetricName:  metricName,
			MetricType:  "histogram",
			Value:       42.5,
			Labels:      labels,
			LabelsHash:  seriesHash("logs-metrics-svc", metricName, labels),
		}}

		require.NoError(t, w.Submit(ctx, f))
		require.NoError(t, f.Wait(ctx))

		var logCount, metricCount uint64
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT count() FROM tracelens.logs WHERE service_name = 'logs-metrics-svc'`).Scan(&logCount))
		require.NoError(t, conn.QueryRow(ctx,
			`SELECT count() FROM tracelens.metrics WHERE metric_name = ?`, metricName).Scan(&metricCount))
		require.Equal(t, uint64(1), logCount)
		require.Equal(t, uint64(1), metricCount)

		var severity uint8
		var body string
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT severity_number, body FROM tracelens.logs
			WHERE service_name = 'logs-metrics-svc' LIMIT 1`).Scan(&severity, &body))
		require.Equal(t, uint8(17), severity)
		require.Equal(t, "payment authorisation failed", body)

		// The materialized view must have rolled the point up on insert.
		var rollupCount, rollupSamples uint64
		require.NoError(t, conn.QueryRow(ctx, `
			SELECT count(), sum(count) FROM tracelens.metrics_rollup_1m
			WHERE metric_name = ?`, metricName).Scan(&rollupCount, &rollupSamples))
		require.Equal(t, uint64(1), rollupCount,
			"the 1m rollup MV must fire on insert, not on a schedule")
		require.Equal(t, uint64(1), rollupSamples)
	})
}

// TestCompressionRatioIsMeasured records the achieved ratio rather than
// asserting a threshold. Principle 5 asks for the ratio to be MEASURED; a
// hard threshold on synthetic data would be a fake assertion, so this reports
// the number for the record and only guards against a nonsensical result.
func TestCompressionRatioIsMeasured(t *testing.T) {
	conn, cfg := startClickHouse(t)
	w, _ := newTestWriter(t, conn, cfg)
	ctx := context.Background()

	base := time.Now().UTC().Truncate(time.Second)
	const rows = 20000

	batch := make([]SpanRow, 0, rows)
	for i := 0; i < rows; i++ {
		batch = append(batch, spanRow(
			byte(i%251), byte(i%253),
			[]string{"gateway", "checkout", "inventory", "payments"}[i%4],
			[]string{"GET /checkout", "POST /orders", "SELECT stock", "authorize"}[i%4],
			base.Add(time.Duration(i)*time.Millisecond),
			uint64(1_000_000+(i%50_000)),
		))
	}

	f := NewFlush("compression-sample")
	f.Spans = batch
	require.NoError(t, w.Submit(ctx, f))
	require.NoError(t, f.Wait(ctx))

	require.NoError(t, conn.Exec(ctx, "OPTIMIZE TABLE tracelens.spans FINAL"))

	// Part-level accounting, deliberately not per-column. This ClickHouse
	// build reports column_data_compressed_bytes as 0 in both
	// system.parts_columns and system.columns, so a per-column assertion
	// would silently measure nothing. system.parts is populated, so that is
	// what the number comes from; `make compression` attempts the per-column
	// breakdown for humans, where an unpopulated result is obvious.
	var totalRaw, totalCompressed uint64
	require.NoError(t, conn.QueryRow(ctx, `
		SELECT sum(data_uncompressed_bytes), sum(data_compressed_bytes)
		FROM system.parts
		WHERE database = 'tracelens' AND table = 'spans' AND active`).
		Scan(&totalRaw, &totalCompressed))

	require.Positive(t, totalRaw, "the sample rows must actually be on disk")
	require.Positive(t, totalCompressed)

	ratio := float64(totalRaw) / float64(totalCompressed)
	t.Logf("MEASURED compression over %d spans: %.2fx (%d -> %d bytes, %.1f bytes/span)",
		rows, ratio, totalRaw, totalCompressed, float64(totalCompressed)/float64(rows))

	require.Greater(t, ratio, 1.0,
		"a columnar store that inflates its input has a codec configured backwards")

	_ = cfg
}
