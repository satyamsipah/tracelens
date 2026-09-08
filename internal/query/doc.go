// Package query implements the TraceLens query engine end to end: a
// hand-written lexer and recursive-descent parser producing an AST, a
// logical planner (Scan -> Filter -> Project -> Aggregate -> Sort ->
// Limit), five independently-testable optimiser passes, and a physical
// compiler to parameterised ClickHouse SQL. No off-the-shelf query
// language library is used anywhere in this package -- the planner is the
// point of the exercise.
//
// Two facts about the storage layer shape the planner throughout:
//
//   - Aggregates weight by sampling_weight (CLAUDE.md principle 6):
//     count() over tracelens.spans is not the number of spans that
//     happened once anything samples; sum(sampling_weight) is. See
//     aggExprSQL in physical.go.
//
//   - Attribute values are stored as String, having been flattened from
//     OTLP's AnyValue. A numeric predicate or aggregation over an
//     attribute gets an explicit toFloat64OrNull cast, pushed into
//     ClickHouse rather than applied after the scan.
//
// Beyond the DSL pipeline, this package also reads two ClickHouse-side
// projections that are populated by other packages, not computed here:
// BuildServiceGraph (servicegraph.go) reads the pre-aggregated
// service_edges rollup -- the cross-service join it depends on happens in
// internal/sampling.ExtractServiceEdges, at trace-decision time, not in
// this package or in ClickHouse -- and QueryRED (red.go) reads the
// red_rollup_1m/5m/1h chain. Neither ever scans raw spans.
package query
