// Command querybench measures each query-optimiser pass's effect (bytes
// scanned, wall-clock latency) with the pass on vs off, at a real 10M+ row
// scale. Bytes/rows read come from ClickHouse's own native-protocol
// progress callback -- an actual execution measurement, not an ESTIMATE.
//
// Usage: go run ./cmd/querybench [-rows 10000000] [-iters 5]
//
// Requires a running, migrated ClickHouse (docker compose up). Bulk-loads
// synthetic spans directly (bypassing OTLP/Kafka -- this benchmarks the
// query engine, not ingestion) up to -rows if the table doesn't already
// have that many.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sort"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/observability"
	"github.com/satyamsipah/tracelens/internal/query"
	"github.com/satyamsipah/tracelens/internal/storage"
)

const (
	numServices   = 10
	numOperations = 5
	batchSize     = 100_000
)

func main() {
	// os.Exit skips deferred functions, so every early exit lives in run()
	// and main() only reports. Without this the deferred conn.Close() below
	// would never run on any error path, leaving the ClickHouse session to
	// time out instead of closing.
	if err := run(); err != nil {
		log.Printf("querybench: %v", err)
		os.Exit(1)
	}
}

func run() error {
	rows := flag.Int("rows", 10_000_000, "target row count in tracelens.spans")
	iters := flag.Int("iters", 5, "iterations per (pass, on/off) measurement, median reported")
	flag.Parse()

	ctx := context.Background()
	cfg := config.LoadClickHouse()
	conn, err := storage.WaitForClickHouse(ctx, cfg, 30*time.Second)
	if err != nil {
		return fmt.Errorf("connect: %w (is `docker compose up` running?)", err)
	}
	defer func() { _ = conn.Close() }()

	if err := ensureRows(ctx, conn, cfg, *rows); err != nil {
		return fmt.Errorf("seed data: %w", err)
	}

	cases := []struct {
		name string
		pass string
		dsl  string
	}{
		{"constant fold (redundant duration bounds)", "constant_fold", `{} | duration > 500ms | duration > 300ms`},
		{"predicate pushdown (service + duration filter)", "predicate_pushdown", `{service="svc-3"} | duration > 500ms`},
		{"partition pruning (1h of 7d)", "partition_pruning", `{service="svc-3"} since 1h`},
		{"limit pushdown (limit 100, no filter)", "limit_pushdown", `{} | limit 100`},
		{"projection pushdown (aggregate query)", "projection_pushdown", `{service="svc-3"} | count by (operation)`},
	}

	fmt.Printf("| Pass | Query | Bytes read (off) | Bytes read (on) | Reduction | Latency (off) | Latency (on) | Speedup |\n")
	fmt.Printf("|---|---|---|---|---|---|---|---|\n")

	for _, c := range cases {
		off, err := measure(ctx, conn, c.dsl, *iters, c.pass)
		if err != nil {
			return fmt.Errorf("%s (off): %w", c.name, err)
		}
		on, err := measure(ctx, conn, c.dsl, *iters)
		if err != nil {
			return fmt.Errorf("%s (on): %w", c.name, err)
		}

		bytesReduction := "-"
		if off.bytes > 0 {
			bytesReduction = fmt.Sprintf("%.1fx", float64(off.bytes)/float64(max64(on.bytes, 1)))
		}
		speedup := "-"
		if on.latency > 0 {
			speedup = fmt.Sprintf("%.2fx", off.latency.Seconds()/on.latency.Seconds())
		}

		fmt.Printf("| %s | `%s` | %s | %s | %s | %s | %s | %s |\n",
			c.name, c.dsl,
			formatBytes(off.bytes), formatBytes(on.bytes), bytesReduction,
			off.latency.Round(time.Millisecond), on.latency.Round(time.Millisecond), speedup)
	}
	return nil
}

type measurement struct {
	bytes   uint64
	latency time.Duration
}

// measure compiles dsl with the given passes skipped (none = fully
// optimised), runs it `iters` times, and reports the MEDIAN bytes read and
// latency -- median rather than mean/first because ClickHouse's page cache
// makes the very first run of a query noticeably slower than the rest, and a
// mean would be dragged around by that outlier.
func measure(ctx context.Context, conn driver.Conn, dsl string, iters int, skip ...string) (measurement, error) {
	q, err := query.Parse(dsl)
	if err != nil {
		return measurement{}, err
	}

	plan := query.Build(mustSelectQuery(q))
	optimized := query.OptimizeExcept(plan, skip...)
	phys, err := query.Compile(optimized)
	if err != nil {
		return measurement{}, err
	}

	var bytesSamples []uint64
	var latSamples []time.Duration
	for i := 0; i < iters; i++ {
		var totalBytes uint64
		pctx := clickhouse.Context(ctx, clickhouse.WithProgress(func(p *clickhouse.Progress) {
			totalBytes += p.Bytes
		}))
		start := time.Now()
		rows, err := conn.Query(pctx, phys.SQL, phys.Args...)
		if err != nil {
			return measurement{}, fmt.Errorf("query: %w (sql=%s)", err, phys.SQL)
		}
		for rows.Next() {
		}
		err = rows.Err()
		_ = rows.Close()
		if err != nil {
			return measurement{}, err
		}
		latSamples = append(latSamples, time.Since(start))
		bytesSamples = append(bytesSamples, totalBytes)
	}

	sort.Slice(bytesSamples, func(i, j int) bool { return bytesSamples[i] < bytesSamples[j] })
	sort.Slice(latSamples, func(i, j int) bool { return latSamples[i] < latSamples[j] })
	return measurement{bytes: bytesSamples[len(bytesSamples)/2], latency: latSamples[len(latSamples)/2]}, nil
}

func mustSelectQuery(q query.Query) query.SelectQuery {
	sq, ok := q.(query.SelectQuery)
	if !ok {
		panic(fmt.Sprintf("querybench: expected a SelectQuery, got %T", q))
	}
	return sq
}

func max64(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func formatBytes(b uint64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := uint64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %ciB", float64(b)/float64(div), "KMGTPE"[exp])
}

// ensureRows tops tracelens.spans up to target rows with synthetic data,
// bulk-inserted directly through storage.Writer (bypassing OTLP/Kafka
// entirely -- this benchmarks the query engine, not ingestion throughput).
func ensureRows(ctx context.Context, conn driver.Conn, chCfg config.ClickHouse, target int) error {
	var current uint64
	row := conn.QueryRow(ctx, "SELECT count() FROM tracelens.spans")
	if err := row.Scan(&current); err != nil {
		return fmt.Errorf("count existing rows: %w", err)
	}
	if current >= uint64(target) {
		log.Printf("tracelens.spans already has %d rows (>= target %d), skipping seed", current, target)
		return nil
	}
	toInsert := uint64(target) - current
	log.Printf("seeding %d synthetic spans (have %d, want %d)...", toInsert, current, target)

	m := observability.NewMetrics()
	writerCfg := chCfg
	writerCfg.BatchSize = batchSize
	writerCfg.FlushInterval = time.Second
	w := storage.NewWriter(conn, writerCfg, m, observability.NewLogger("querybench"))
	w.Start(ctx)
	defer w.Close()

	rng := rand.New(rand.NewSource(1))
	now := time.Now().UTC()
	const spread = 7 * 24 * time.Hour // spread across a week, so a 1h window is a small, meaningful slice

	statuses := []string{"ok", "ok", "ok", "ok", "error"} // 20% error rate

	var inserted uint64
	for inserted < toInsert {
		n := batchSize
		if remaining := toInsert - inserted; remaining < uint64(n) {
			n = int(remaining)
		}
		spans := make([]storage.SpanRow, n)
		for i := 0; i < n; i++ {
			svc := fmt.Sprintf("svc-%d", rng.Intn(numServices))
			op := fmt.Sprintf("op-%d", rng.Intn(numOperations))
			traceID := make([]byte, 16)
			spanID := make([]byte, 8)
			rng.Read(traceID)
			rng.Read(spanID)
			ts := now.Add(-time.Duration(rng.Int63n(int64(spread))))
			durationNS := uint64(rng.ExpFloat64() * float64(200*time.Millisecond))

			spans[i] = storage.SpanRow{
				Timestamp:    ts,
				TraceID:      traceID,
				SpanID:       spanID,
				ParentSpanID: make([]byte, 8),
				ServiceName:  svc,
				SpanName:     op,
				SpanKind:     "server",
				DurationNS:   durationNS,
				StatusCode:   statuses[rng.Intn(len(statuses))],
				ResourceAttributes: map[string]string{
					"deployment.environment": "benchmark",
				},
				SpanAttributes: map[string]string{
					"http.method":      "GET",
					"http.route":       "/" + op,
					"http.status_code": "200",
				},
				SamplingWeight: 1.0,
			}
		}

		f := storage.NewFlush(fmt.Sprintf("querybench-seed-%d", inserted))
		f.Spans = spans
		if err := w.Submit(ctx, f); err != nil {
			return fmt.Errorf("submit batch: %w", err)
		}
		if err := f.Wait(ctx); err != nil {
			return fmt.Errorf("write batch: %w", err)
		}
		inserted += uint64(n)
		if inserted%1_000_000 < uint64(batchSize) {
			log.Printf("seeded %d / %d rows", inserted, toInsert)
		}
	}

	log.Printf("seed complete: %d rows inserted", inserted)
	_ = os.Stdout.Sync()
	return nil
}
