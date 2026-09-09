package query

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// StorageStat is one table's on-disk footprint, read from active parts --
// the same query the self-monitoring Grafana dashboard uses, reused here so
// the UI's health page and the dashboard never quietly disagree.
type StorageStat struct {
	Table      string
	SizeOnDisk string
	Rows       uint64
}

// QueryStorageStats reports active-part size and row count per table.
func QueryStorageStats(ctx context.Context, conn driver.Conn) ([]StorageStat, error) {
	rows, err := conn.Query(ctx, `
		SELECT table, formatReadableSize(sum(bytes_on_disk)) AS size_on_disk, sum(rows) AS rows
		FROM system.parts
		WHERE active AND database = 'tracelens'
		GROUP BY table
		ORDER BY sum(bytes_on_disk) DESC`)
	if err != nil {
		return nil, fmt.Errorf("query: storage stats: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []StorageStat
	for rows.Next() {
		var s StorageStat
		if err := rows.Scan(&s.Table, &s.SizeOnDisk, &s.Rows); err != nil {
			return nil, fmt.Errorf("query: scan storage stat: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

// CompressionStat is one table's measured (not assumed -- CLAUDE.md
// principle 5) compression ratio.
type CompressionStat struct {
	Table           string
	RawBytes        uint64
	CompressedBytes uint64
	Ratio           float64
}

// QueryCompressionStats reports raw vs on-disk bytes and the resulting ratio
// per table.
func QueryCompressionStats(ctx context.Context, conn driver.Conn) ([]CompressionStat, error) {
	rows, err := conn.Query(ctx, `
		SELECT table,
		       sum(data_uncompressed_bytes) AS raw_bytes,
		       sum(bytes_on_disk) AS compressed_bytes,
		       round(sum(data_uncompressed_bytes) / greatest(sum(bytes_on_disk), 1), 2) AS ratio
		FROM system.parts
		WHERE active AND database = 'tracelens'
		GROUP BY table
		ORDER BY raw_bytes DESC`)
	if err != nil {
		return nil, fmt.Errorf("query: compression stats: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []CompressionStat
	for rows.Next() {
		var s CompressionStat
		if err := rows.Scan(&s.Table, &s.RawBytes, &s.CompressedBytes, &s.Ratio); err != nil {
			return nil, fmt.Errorf("query: scan compression stat: %w", err)
		}
		out = append(out, s)
	}
	return out, rows.Err()
}
