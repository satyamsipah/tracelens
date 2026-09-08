// Package query will hold the query engine (phase 3):
// DSL -> lexer -> parser -> AST -> logical plan -> physical plan.
//
// Empty by design in phase 1. Written by hand, with no off-the-shelf query
// language library -- the planner is the point of the exercise.
//
// Two facts about the storage layer constrain the planner from the start:
//
//   - Aggregates MUST weight by sampling_weight. Once phase 2 samples,
//     count() over tracelens.spans is not the number of spans that happened;
//     sum(sampling_weight) is. A planner that emits a bare count() produces
//     an answer that looks right and is wrong.
//
//   - Attribute values are stored as String, having been flattened from
//     OTLP's AnyValue. Numeric predicates over attributes therefore need an
//     explicit cast, and the planner should push those into ClickHouse rather
//     than filtering after the scan.
//
// Physical planning should aim at the spans table's sort key
// (service_name, span_name, timestamp): a predicate on service and time
// prunes granules, while one on duration alone does not.
package query
