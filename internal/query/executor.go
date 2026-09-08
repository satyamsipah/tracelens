package query

import (
	"context"
	"encoding/hex"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// Result is the generic, JSON-friendly output of a query: column names in
// order, and one []any per row in the same order.
type Result struct {
	Columns []string
	Rows    [][]any
}

// Options bounds a single query's execution cost. CLAUDE.md's ingestion-side
// discipline -- bounded memory, an explicit cap, never "trust the caller" --
// applies just as much to the read path: an unbounded query against a
// multi-billion-row table is a self-inflicted denial of service.
type Options struct {
	// Timeout bounds wall-clock execution.
	Timeout time.Duration
	// MaxRowsScanned, if non-zero, rejects a query whose ClickHouse
	// EXPLAIN ESTIMATE reports more matching rows than this -- checked
	// BEFORE the real query runs, so a runaway scan is refused cheaply
	// rather than merely cut off partway through by the timeout.
	MaxRowsScanned uint64
}

// DefaultOptions is what the query API applies when a caller doesn't
// override it.
func DefaultOptions() Options {
	return Options{Timeout: 30 * time.Second, MaxRowsScanned: 50_000_000}
}

// Execute parses, plans, optimises and runs dsl against conn.
func Execute(ctx context.Context, conn driver.Conn, dsl string, opts Options) (*Result, error) {
	q, err := Parse(dsl)
	if err != nil {
		return nil, err
	}
	switch v := q.(type) {
	case TraceQuery:
		return executeTrace(ctx, conn, v, opts)
	case SelectQuery:
		return executeSelect(ctx, conn, v, opts)
	default:
		return nil, fmt.Errorf("query: unrecognised query type %T", q)
	}
}

func executeSelect(ctx context.Context, conn driver.Conn, sq SelectQuery, opts Options) (*Result, error) {
	phys, err := Compile(Optimize(Build(sq)))
	if err != nil {
		return nil, err
	}

	if opts.MaxRowsScanned > 0 {
		est, estErr := EstimateRows(ctx, conn, phys)
		// A failed estimate is not fatal -- EXPLAIN ESTIMATE is a
		// best-effort preflight guard, not a correctness requirement, and
		// refusing every query because the guard itself errored would be
		// worse than the risk it exists to catch.
		if estErr == nil && est > opts.MaxRowsScanned {
			return nil, fmt.Errorf("query: estimated %d rows exceeds the %d row guard; narrow the time range or add a more selective filter", est, opts.MaxRowsScanned)
		}
	}

	qctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	rows, err := conn.Query(qctx, phys.SQL, phys.Args...)
	if err != nil {
		return nil, fmt.Errorf("query: execute: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

// executeTrace serves a trace-by-id lookup directly off the trace_id bloom
// filter (DECISIONS.md Sec 5), bypassing the whole filter/aggregate plan --
// a point lookup has nothing to optimise.
func executeTrace(ctx context.Context, conn driver.Conn, tq TraceQuery, opts Options) (*Result, error) {
	idBytes, err := decodeTraceID(tq.TraceID)
	if err != nil {
		return nil, err
	}
	qctx, cancel := context.WithTimeout(ctx, opts.Timeout)
	defer cancel()
	// Bound as a string, not []byte: clickhouse-go's positional-parameter
	// binding (as opposed to a typed batch Append) infers []byte as
	// Array(UInt8), which has no common supertype with the FixedString(16)
	// column -- a string parameter compares against FixedString natively.
	rows, err := conn.Query(qctx, `SELECT * FROM tracelens.spans WHERE trace_id = ? ORDER BY timestamp`, string(idBytes))
	if err != nil {
		return nil, fmt.Errorf("query: trace lookup: %w", err)
	}
	defer rows.Close()
	return scanRows(rows)
}

func decodeTraceID(id string) ([]byte, error) {
	b, err := hex.DecodeString(id)
	if err != nil {
		return nil, fmt.Errorf("query: invalid trace id %q: must be hex-encoded: %w", id, err)
	}
	if len(b) != 16 {
		return nil, fmt.Errorf("query: invalid trace id %q: want 16 bytes (32 hex chars), got %d", id, len(b))
	}
	return b, nil
}

// scanRows drains a result set generically -- Execute serves arbitrary
// aggregations and raw column lists, so the column set isn't known until
// query time. ColumnType.ScanType gives the exact Go type ClickHouse would
// hand back for a typed Scan, so this reproduces that without hand-listing
// every possible ClickHouse type here.
func scanRows(rows driver.Rows) (*Result, error) {
	cols := rows.Columns()
	types := rows.ColumnTypes()
	result := &Result{Columns: cols}

	for rows.Next() {
		dest := make([]any, len(types))
		for i, ct := range types {
			dest[i] = reflect.New(ct.ScanType()).Interface()
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, fmt.Errorf("query: scan row: %w", err)
		}
		row := make([]any, len(dest))
		for i, d := range dest {
			row[i] = reflect.ValueOf(d).Elem().Interface()
		}
		result.Rows = append(result.Rows, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("query: row iteration: %w", err)
	}
	return result, nil
}

// EstimateRows runs ClickHouse's own EXPLAIN ESTIMATE against phys and sums
// the per-part row estimate -- the same mechanism DECISIONS.md's storage
// review already relies on to reason about scan cost, reused here as a
// pre-execution guard rather than a purely diagnostic tool.
//
// MEASURED: EXPLAIN ESTIMATE against a query using genuine "?" wire-protocol
// parameters reported 0 rows on freshly inserted data that a plain query
// against the identical predicate matched correctly -- ClickHouse's index
// range estimation needs the predicate's values available at plan time, and
// a bound parameter isn't substituted until execution. Fixed by inlining
// phys.Args as literal SQL text for this diagnostic call only; the real
// query in executeSelect/executeTrace still uses genuine bound parameters.
// This is safe specifically because every value in phys.Args was produced by
// Compile() from already-parsed, already-typed Go values -- never raw user
// text passed through unescaped.
func EstimateRows(ctx context.Context, conn driver.Conn, phys Physical) (uint64, error) {
	inlined, err := inlineForExplain(phys.SQL, phys.Args)
	if err != nil {
		return 0, err
	}
	rows, err := conn.Query(ctx, "EXPLAIN ESTIMATE "+inlined)
	if err != nil {
		return 0, fmt.Errorf("query: explain estimate: %w", err)
	}
	defer rows.Close()

	var total uint64
	for rows.Next() {
		var database, table string
		var parts, rowsEst, marks uint64
		if err := rows.Scan(&database, &table, &parts, &rowsEst, &marks); err != nil {
			return 0, fmt.Errorf("query: scan explain estimate: %w", err)
		}
		total += rowsEst
	}
	return total, rows.Err()
}

// inlineForExplain substitutes each "?" in sql with a literal rendering of
// the corresponding bound argument, in order. It is only ever used for the
// EXPLAIN/EXPLAIN ESTIMATE diagnostic path (see EstimateRows) -- the actual
// query always executes with real bound parameters.
func inlineForExplain(sql string, args []any) (string, error) {
	var sb strings.Builder
	argi := 0
	for i := 0; i < len(sql); i++ {
		if sql[i] != '?' {
			sb.WriteByte(sql[i])
			continue
		}
		if argi >= len(args) {
			return "", fmt.Errorf("query: more placeholders than bound arguments in %q", sql)
		}
		lit, err := explainLiteral(args[argi])
		if err != nil {
			return "", err
		}
		sb.WriteString(lit)
		argi++
	}
	return sb.String(), nil
}

func explainLiteral(v any) (string, error) {
	switch t := v.(type) {
	case string:
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(t) + "'", nil
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64), nil
	case int64:
		return strconv.FormatInt(t, 10), nil
	case bool:
		if t {
			return "1", nil
		}
		return "0", nil
	case time.Time:
		return "'" + t.UTC().Format("2006-01-02 15:04:05.999999999") + "'", nil
	default:
		return "", fmt.Errorf("query: cannot render a %T literal for EXPLAIN", v)
	}
}
