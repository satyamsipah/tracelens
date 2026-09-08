package query

import (
	"context"
	"fmt"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// RollupLevel selects which pre-aggregated RED rollup to read. Requirement 9
// asks for 1m/5m/1h; see docs/DECISIONS.md for the storage-vs-query-cost
// trade-off between them.
type RollupLevel string

const (
	Rollup1m RollupLevel = "1m"
	Rollup5m RollupLevel = "5m"
	Rollup1h RollupLevel = "1h"
)

// rollupTables is a fixed allow-list: RollupLevel is exactly the kind of
// value that ends up driven by an HTTP query parameter, so the string is
// mapped through this table rather than ever formatted directly into SQL.
var rollupTables = map[RollupLevel]string{
	Rollup1m: "tracelens.red_rollup_1m",
	Rollup5m: "tracelens.red_rollup_5m",
	Rollup1h: "tracelens.red_rollup_1h",
}

// REDStat is one (service, operation, time bucket) RED observation.
type REDStat struct {
	Bucket             time.Time
	Service, Operation string
	Calls              float64 // sum of sampling_weight
	ErrorRate          float64 // in [0,1]
	P50, P95, P99      time.Duration
}

// QueryRED reads rate/error/duration for a service (optionally narrowed to
// one operation) from the requested rollup level over [since, until).
// Never scans tracelens.spans -- that is the entire point of pre-aggregating.
func QueryRED(ctx context.Context, conn driver.Conn, level RollupLevel, service, operation string, since, until time.Time) ([]REDStat, error) {
	table, ok := rollupTables[level]
	if !ok {
		return nil, fmt.Errorf("query: unknown rollup level %q", level)
	}

	sql := fmt.Sprintf(`
		SELECT
			bucket, service_name, operation,
			sumMerge(call_weight)  AS calls,
			sumMerge(error_weight) AS errors,
			quantilesTDigestWeightedMerge(0.5, 0.95, 0.99)(duration_quantiles) AS q
		FROM %s
		WHERE service_name = ? AND bucket >= ? AND bucket < ?%s
		GROUP BY bucket, service_name, operation
		ORDER BY bucket
	`, table, operationFilter(operation))

	args := []any{service, since.UTC(), until.UTC()}
	if operation != "" {
		args = append(args, operation)
	}

	rows, err := conn.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("query: red rollup: %w", err)
	}
	defer rows.Close()

	var out []REDStat
	for rows.Next() {
		var s REDStat
		var calls, errs float64
		var q []uint64
		if err := rows.Scan(&s.Bucket, &s.Service, &s.Operation, &calls, &errs, &q); err != nil {
			return nil, fmt.Errorf("query: scan red rollup row: %w", err)
		}
		s.Calls = calls
		if calls > 0 {
			s.ErrorRate = errs / calls
		}
		if len(q) == 3 {
			s.P50, s.P95, s.P99 = time.Duration(q[0]), time.Duration(q[1]), time.Duration(q[2])
		}
		out = append(out, s)
	}
	return out, rows.Err()
}

func operationFilter(operation string) string {
	if operation == "" {
		return ""
	}
	return " AND operation = ?"
}
