// Command storagebench measures the storage-side numbers requirement 7
// (Part B) asks for beyond the whole-table compression ratio `make
// compression` already reports: the top columns by compressed size, a
// direct codec comparison (DoubleDelta vs Delta vs none on timestamps,
// LowCardinality on/off on service_name), and insert throughput at
// different batch sizes.
//
// Everything runs in a throwaway `tracelens_bench` database, dropped and
// recreated on each run, so this never touches the real tracelens schema
// or its data.
//
// Usage: go run ./cmd/storagebench
// Requires a running, migrated ClickHouse (docker compose up).
package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/satyamsipah/tracelens/internal/config"
	"github.com/satyamsipah/tracelens/internal/query"
	"github.com/satyamsipah/tracelens/internal/storage"
)

const (
	codecExperimentRows = 2_000_000
	batchSizeRows       = 2_000_000
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()
	cfg := config.LoadClickHouse()
	conn, err := storage.WaitForClickHouse(ctx, cfg, 2*time.Minute)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()

	fmt.Println("## Whole-table compression (measured)")
	if err := reportCompression(ctx, conn); err != nil {
		return err
	}

	fmt.Println("\n## Top 5 columns by compressed size (tracelens.spans)")
	if err := reportTopColumns(ctx, conn); err != nil {
		return err
	}

	fmt.Println("\n## Codec comparison: DoubleDelta vs Delta vs none (timestamp)")
	if err := codecComparisonTimestamp(ctx, conn); err != nil {
		return err
	}

	fmt.Println("\n## Codec comparison: LowCardinality on vs off (service_name)")
	if err := codecComparisonLowCardinality(ctx, conn); err != nil {
		return err
	}

	fmt.Println("\n## Insert throughput at different batch sizes")
	if err := batchSizeThroughput(ctx, conn); err != nil {
		return err
	}

	return nil
}

func reportCompression(ctx context.Context, conn driver.Conn) error {
	stats, err := query.QueryCompressionStats(ctx, conn)
	if err != nil {
		return err
	}
	fmt.Println("| Table | Raw | Compressed | Ratio |")
	fmt.Println("|---|---|---|---|")
	for _, s := range stats {
		fmt.Printf("| %s | %s | %s | %.2fx |\n", s.Table, formatBytes(s.RawBytes), formatBytes(s.CompressedBytes), s.Ratio)
	}
	return nil
}

func reportTopColumns(ctx context.Context, conn driver.Conn) error {
	rows, err := conn.Query(ctx, `
		SELECT column, sum(column_data_compressed_bytes) AS compressed, sum(column_data_uncompressed_bytes) AS raw
		FROM system.parts_columns
		WHERE active AND database = 'tracelens' AND table = 'spans'
		GROUP BY column
		ORDER BY compressed DESC
		LIMIT 5`)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	fmt.Println("| Column | Compressed | Raw |")
	fmt.Println("|---|---|---|")
	any := false
	for rows.Next() {
		var col string
		var compressed, raw uint64
		if err := rows.Scan(&col, &compressed, &raw); err != nil {
			return err
		}
		any = true
		fmt.Printf("| %s | %s | %s |\n", col, formatBytes(compressed), formatBytes(raw))
	}
	if !any {
		fmt.Println("(system.parts_columns reported no rows -- see docs/DECISIONS.md Sec 3a: this ClickHouse build reports column-level bytes as 0 until the table holds real volume)")
	}
	return rows.Err()
}

// codecComparisonTimestamp inserts the SAME near-monotonic timestamp
// sequence into three tables differing only by codec, then measures
// compressed size -- an apples-to-apples comparison, not three separately
// reasoned-about numbers.
func codecComparisonTimestamp(ctx context.Context, conn driver.Conn) error {
	if err := conn.Exec(ctx, "CREATE DATABASE IF NOT EXISTS tracelens_bench"); err != nil {
		return err
	}
	variants := map[string]string{
		"ts_doubledelta": "DateTime64(9) CODEC(DoubleDelta, ZSTD(1))",
		"ts_delta":       "DateTime64(9) CODEC(Delta, ZSTD(1))",
		"ts_none":        "DateTime64(9) CODEC(ZSTD(1))",
	}
	for table, colDef := range variants {
		if err := conn.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS tracelens_bench.%s", table)); err != nil {
			return err
		}
		if err := conn.Exec(ctx, fmt.Sprintf(
			"CREATE TABLE tracelens_bench.%s (ts %s) ENGINE = MergeTree ORDER BY ts", table, colDef)); err != nil {
			return err
		}
	}

	now := time.Now()
	for table := range variants {
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO tracelens_bench.%s", table))
		if err != nil {
			return err
		}
		for i := 0; i < codecExperimentRows; i++ {
			// Sorted, near-regular arrival -- the shape DoubleDelta is
			// designed for, matching real spans within one granule.
			ts := now.Add(time.Duration(i) * time.Millisecond)
			if err := batch.Append(ts); err != nil {
				return err
			}
		}
		if err := batch.Send(); err != nil {
			return err
		}
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE tracelens_bench.ts_doubledelta FINAL"); err != nil {
		return err
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE tracelens_bench.ts_delta FINAL"); err != nil {
		return err
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE tracelens_bench.ts_none FINAL"); err != nil {
		return err
	}

	return reportBenchTableSizes(ctx, conn, []string{"ts_doubledelta", "ts_delta", "ts_none"})
}

// codecComparisonLowCardinality inserts the same 10-distinct-value service
// name distribution as LowCardinality(String) vs plain String.
func codecComparisonLowCardinality(ctx context.Context, conn driver.Conn) error {
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS tracelens_bench.svc_lowcard"); err != nil {
		return err
	}
	if err := conn.Exec(ctx, "DROP TABLE IF EXISTS tracelens_bench.svc_plain"); err != nil {
		return err
	}
	if err := conn.Exec(ctx, "CREATE TABLE tracelens_bench.svc_lowcard (svc LowCardinality(String) CODEC(ZSTD(1))) ENGINE = MergeTree ORDER BY svc"); err != nil {
		return err
	}
	if err := conn.Exec(ctx, "CREATE TABLE tracelens_bench.svc_plain (svc String CODEC(ZSTD(1))) ENGINE = MergeTree ORDER BY svc"); err != nil {
		return err
	}

	rng := rand.New(rand.NewSource(1))
	services := make([]string, codecExperimentRows)
	for i := range services {
		services[i] = fmt.Sprintf("service-%d", rng.Intn(10))
	}

	for _, table := range []string{"svc_lowcard", "svc_plain"} {
		batch, err := conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO tracelens_bench.%s", table))
		if err != nil {
			return err
		}
		for _, s := range services {
			if err := batch.Append(s); err != nil {
				return err
			}
		}
		if err := batch.Send(); err != nil {
			return err
		}
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE tracelens_bench.svc_lowcard FINAL"); err != nil {
		return err
	}
	if err := conn.Exec(ctx, "OPTIMIZE TABLE tracelens_bench.svc_plain FINAL"); err != nil {
		return err
	}

	return reportBenchTableSizes(ctx, conn, []string{"svc_lowcard", "svc_plain"})
}

func reportBenchTableSizes(ctx context.Context, conn driver.Conn, tables []string) error {
	fmt.Println("| Table | Compressed size |")
	fmt.Println("|---|---|")
	for _, t := range tables {
		var bytesOnDisk uint64
		row := conn.QueryRow(ctx, "SELECT sum(bytes_on_disk) FROM system.parts WHERE active AND database = 'tracelens_bench' AND table = ?", t)
		if err := row.Scan(&bytesOnDisk); err != nil {
			return err
		}
		fmt.Printf("| %s | %s |\n", t, formatBytes(bytesOnDisk))
	}
	return nil
}

// batchSizeThroughput inserts the identical row count through
// storage.Writer at several BatchSize settings, measuring wall-clock
// insert throughput -- "find the optimum" per the brief, not just "bigger
// is better" asserted without a number.
func batchSizeThroughput(ctx context.Context, conn driver.Conn) error {
	if err := conn.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS tracelens_bench.throughput_probe
		(ts DateTime64(9) CODEC(DoubleDelta, ZSTD(1)), svc LowCardinality(String), n UInt64 CODEC(T64, ZSTD(1)))
		ENGINE = MergeTree ORDER BY (svc, ts)`); err != nil {
		return err
	}

	batchSizes := []int{1_000, 10_000, 50_000, 100_000, 200_000}
	fmt.Println("| Batch size | Rows | Wall time | Rows/sec |")
	fmt.Println("|---|---|---|---|")

	for _, bs := range batchSizes {
		if err := conn.Exec(ctx, "TRUNCATE TABLE tracelens_bench.throughput_probe"); err != nil {
			return err
		}
		start := time.Now()
		inserted := 0
		for inserted < batchSizeRows {
			n := bs
			if remaining := batchSizeRows - inserted; remaining < n {
				n = remaining
			}
			batch, err := conn.PrepareBatch(ctx, "INSERT INTO tracelens_bench.throughput_probe")
			if err != nil {
				return err
			}
			now := time.Now()
			for i := 0; i < n; i++ {
				if err := batch.Append(now, "svc-bench", uint64(i)); err != nil {
					return err
				}
			}
			if err := batch.Send(); err != nil {
				return err
			}
			inserted += n
		}
		elapsed := time.Since(start)
		rowsPerSec := float64(inserted) / elapsed.Seconds()
		fmt.Printf("| %d | %d | %s | %.0f |\n", bs, inserted, elapsed.Round(time.Millisecond), rowsPerSec)
	}
	return nil
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
