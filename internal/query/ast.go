package query

import "time"

// Query is the root of a parsed DSL query. Exactly two shapes exist:
// TraceQuery, a point lookup that bypasses the whole plan/optimise/execute
// path, and SelectQuery, the filter/aggregate pipeline.
type Query interface{ queryNode() }

// TraceQuery is "trace(<id>)" -- served directly off the trace_id bloom
// filter (DECISIONS.md Sec 5), never through the logical planner.
type TraceQuery struct {
	TraceID string
}

func (TraceQuery) queryNode() {}

// SelectQuery is "{...} [since|range ...] { | stage }".
type SelectQuery struct {
	Selector []LabelMatcher
	Range    TimeRange
	Stages   []Stage
}

func (SelectQuery) queryNode() {}

// LabelOp is a label-selector comparison operator.
type LabelOp int

const (
	OpEq LabelOp = iota
	OpNeq
	OpRegexMatch
	OpRegexNotMatch
)

func (op LabelOp) String() string {
	switch op {
	case OpEq:
		return "="
	case OpNeq:
		return "!="
	case OpRegexMatch:
		return "=~"
	case OpRegexNotMatch:
		return "!~"
	default:
		return "?"
	}
}

// LabelMatcher is one "name OP value" term inside a {...} selector.
type LabelMatcher struct {
	Name  string
	Op    LabelOp
	Value string
}

// TimeRange is the query's time bound, always resolved to absolute
// [Start, End) by the time parsing finishes -- Explicit records whether the
// query text stated one, or the approved 1h default was applied.
type TimeRange struct {
	Start, End time.Time
	Explicit   bool
}

// Stage is one "| ..." pipeline element.
type Stage interface{ stageNode() }

// CmpOp is a numeric comparison operator.
type CmpOp int

const (
	CmpGT CmpOp = iota
	CmpGTE
	CmpLT
	CmpLTE
	CmpEQ
	CmpNEQ
)

func (op CmpOp) String() string {
	switch op {
	case CmpGT:
		return ">"
	case CmpGTE:
		return ">="
	case CmpLT:
		return "<"
	case CmpLTE:
		return "<="
	case CmpEQ:
		return "=="
	case CmpNEQ:
		return "!="
	default:
		return "?"
	}
}

// NumericFilter is "<field> <op> <number>[unit]", e.g. "duration > 500ms".
// Unit is empty for a bare number.
type NumericFilter struct {
	Field string
	Op    CmpOp
	Value float64
	Unit  string
}

func (NumericFilter) stageNode() {}

// AggFunc names a supported aggregation function.
type AggFunc string

const (
	AggCount AggFunc = "count"
	AggSum   AggFunc = "sum"
	AggAvg   AggFunc = "avg"
	AggMin   AggFunc = "min"
	AggMax   AggFunc = "max"
	AggP50   AggFunc = "p50"
	AggP95   AggFunc = "p95"
	AggP99   AggFunc = "p99"
)

// AggExpr is one aggregation expression, e.g. "p99(duration)" or "count".
// Field is empty for count.
type AggExpr struct {
	Func  AggFunc
	Field string
}

// Alias is the output column name for this aggregation, e.g. "p99_duration"
// or "count".
func (e AggExpr) Alias() string {
	if e.Field == "" {
		return string(e.Func)
	}
	return string(e.Func) + "_" + e.Field
}

// Aggregation is "<AggExpr>, ... [by (<field>, ...)]".
type Aggregation struct {
	Exprs   []AggExpr
	GroupBy []string
}

func (Aggregation) stageNode() {}

// SortField is one "<field> [asc|desc]" term.
type SortField struct {
	Field string
	Desc  bool
}

// SortStage is "sort by (<field> [asc|desc], ...)".
type SortStage struct {
	Fields []SortField
}

func (SortStage) stageNode() {}

// LimitStage is "limit <n>".
type LimitStage struct {
	N int
}

func (LimitStage) stageNode() {}
