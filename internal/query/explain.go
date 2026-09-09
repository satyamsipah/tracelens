package query

import (
	"context"
	"fmt"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

// ExplainResult is the full "show your work" view of a query: the plan
// before and after optimisation, the compiled SQL and its bound arguments,
// and (when a live connection is supplied) ClickHouse's own EXPLAIN and row
// estimate.
type ExplainResult struct {
	DSL           string
	LogicalPlan   string
	OptimizedPlan string
	SQL           string
	Args          []any

	// ClickHouseExplain and EstimatedRows are populated only when conn is
	// non-nil -- Explain works standalone (plan/SQL only) for tests and
	// tooling that don't have a ClickHouse connection at hand.
	ClickHouseExplain []string
	EstimatedRows     uint64
}

// Explain builds a query's logical and optimised plan and compiles it to
// SQL without executing it, optionally enriched with ClickHouse's own
// EXPLAIN and EXPLAIN ESTIMATE against a live connection.
func Explain(ctx context.Context, conn driver.Conn, dsl string) (*ExplainResult, error) {
	q, err := Parse(dsl)
	if err != nil {
		return nil, err
	}
	sq, ok := q.(SelectQuery)
	if !ok {
		return nil, fmt.Errorf("query: EXPLAIN applies to filter/aggregate queries, not trace(...) lookups")
	}

	logical := Build(sq)
	optimized := Optimize(logical)
	phys, err := Compile(optimized)
	if err != nil {
		return nil, err
	}

	res := &ExplainResult{
		DSL:           dsl,
		LogicalPlan:   PlanString(logical),
		OptimizedPlan: PlanString(optimized),
		SQL:           phys.SQL,
		Args:          phys.Args,
	}
	if conn == nil {
		return res, nil
	}

	// Inlined for the same reason as EstimateRows: EXPLAIN's index analysis
	// needs literal predicate values at plan time, which a bound "?"
	// parameter does not provide.
	if inlined, ierr := inlineForExplain(phys.SQL, phys.Args); ierr == nil {
		if rows, err := conn.Query(ctx, "EXPLAIN indexes=1, actions=1 "+inlined); err == nil {
			defer func() { _ = rows.Close() }()
			for rows.Next() {
				var line string
				if rows.Scan(&line) == nil {
					res.ClickHouseExplain = append(res.ClickHouseExplain, line)
				}
			}
		}
	}
	if est, err := EstimateRows(ctx, conn, phys); err == nil {
		res.EstimatedRows = est
	}
	return res, nil
}
