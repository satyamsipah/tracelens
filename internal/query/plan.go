package query

import (
	"fmt"
	"strings"
)

// Expr is a predicate expression tree. It exists as its own type (rather
// than reusing AST nodes directly) because the optimiser rewrites it --
// folding, pushing, and simplifying -- independently of the immutable parsed
// AST.
type Expr interface {
	exprNode()
	String() string
}

// ColumnRef names a resolved field. Resolve() has already run by the time
// one of these exists in a plan, so Name is either a known physical column
// ("service_name", "duration_ns", ...) or an AttrRef.
type ColumnRef struct {
	Name string
}

func (ColumnRef) exprNode()        {}
func (c ColumnRef) String() string { return c.Name }

// AttrRef is an attribute-map lookup that was not one of the known columns.
// It carries its own resolution rule (span_attributes, falling back to
// resource_attributes) rather than being flattened to a plain column name,
// because physical planning needs to know it is a map lookup, not a column.
type AttrRef struct {
	Key string
}

func (AttrRef) exprNode()        {}
func (a AttrRef) String() string { return "attr(" + a.Key + ")" }

// Literal is a constant value: string, float64, or bool.
type Literal struct {
	Value any
}

func (Literal) exprNode() {}
func (l Literal) String() string {
	return fmt.Sprintf("%v", l.Value)
}

// BinaryExpr is "Left Op Right". Op is one of the LabelOp/CmpOp string forms
// ("=", "!=", "=~", "!~", ">", ">=", "<", "<=", "==") or the boolean
// combinator "AND".
type BinaryExpr struct {
	Op          string
	Left, Right Expr
}

func (BinaryExpr) exprNode() {}
func (b BinaryExpr) String() string {
	return fmt.Sprintf("(%s %s %s)", b.Left, b.Op, b.Right)
}

// AndAll builds a left-associative AND chain from terms, skipping nils and
// collapsing to a single term (or nil) when there is nothing to combine.
func AndAll(terms ...Expr) Expr {
	var out Expr
	for _, t := range terms {
		if t == nil {
			continue
		}
		if out == nil {
			out = t
			continue
		}
		out = BinaryExpr{Op: "AND", Left: out, Right: t}
	}
	return out
}

// PlanNode is one operator in the logical (and, after physical planning,
// SQL-bound) plan tree.
type PlanNode interface {
	planNode() string // human-readable op name, used by String()
	Inputs() []PlanNode
}

// ScanNode reads from a table. It starts with no predicate and no column
// restriction; the optimiser fills both in.
type ScanNode struct {
	Table     string
	Range     TimeRange
	Predicate Expr     // nil until predicate/partition pushdown runs
	Columns   []string // nil = all columns; set by projection pushdown
	Limit     int      // 0 = none; set by limit pushdown when safe
}

func (ScanNode) planNode() string   { return "Scan" }
func (ScanNode) Inputs() []PlanNode { return nil }

// FilterNode keeps rows matching Predicate. An empty/nil Predicate after
// pushdown means the filter was fully absorbed into the Scan and this node
// is a pass-through kept only for shape -- Optimize elides pass-throughs.
type FilterNode struct {
	Input     PlanNode
	Predicate Expr
}

func (FilterNode) planNode() string     { return "Filter" }
func (f FilterNode) Inputs() []PlanNode { return []PlanNode{f.Input} }

// ProjectNode restricts the row shape to Columns. Empty Columns before
// projection pushdown means "not yet computed", not "zero columns".
type ProjectNode struct {
	Input   PlanNode
	Columns []string
}

func (ProjectNode) planNode() string     { return "Project" }
func (p ProjectNode) Inputs() []PlanNode { return []PlanNode{p.Input} }

// AggregateNode computes Exprs grouped by GroupBy. Every AggExpr is already
// weight-aware by construction (see physical.go) -- CLAUDE.md principle 6.
type AggregateNode struct {
	Input   PlanNode
	Exprs   []AggExpr
	GroupBy []string
}

func (AggregateNode) planNode() string     { return "Aggregate" }
func (a AggregateNode) Inputs() []PlanNode { return []PlanNode{a.Input} }

// SortNode orders rows.
type SortNode struct {
	Input  PlanNode
	Fields []SortField
}

func (SortNode) planNode() string     { return "Sort" }
func (s SortNode) Inputs() []PlanNode { return []PlanNode{s.Input} }

// LimitNode truncates the result to N rows (or N groups, if it sits above an
// Aggregate).
type LimitNode struct {
	Input PlanNode
	N     int
}

func (LimitNode) planNode() string     { return "Limit" }
func (l LimitNode) Inputs() []PlanNode { return []PlanNode{l.Input} }

// PlanString renders a plan tree, indented by depth -- used by EXPLAIN and by
// test assertions that want a stable, readable shape rather than reflecting
// on struct fields.
func PlanString(n PlanNode) string {
	var sb strings.Builder
	writePlan(&sb, n, 0)
	return sb.String()
}

func writePlan(sb *strings.Builder, n PlanNode, depth int) {
	indent := strings.Repeat("  ", depth)
	switch v := n.(type) {
	case ScanNode:
		fmt.Fprintf(sb, "%sScan(%s", indent, v.Table)
		if v.Predicate != nil {
			fmt.Fprintf(sb, ", predicate=%s", v.Predicate)
		}
		if v.Columns != nil {
			fmt.Fprintf(sb, ", columns=%v", v.Columns)
		}
		if v.Limit > 0 {
			fmt.Fprintf(sb, ", limit=%d", v.Limit)
		}
		fmt.Fprintf(sb, ", range=[%s,%s))\n", v.Range.Start.Format("15:04:05"), v.Range.End.Format("15:04:05"))
	case FilterNode:
		fmt.Fprintf(sb, "%sFilter(%s)\n", indent, v.Predicate)
	case ProjectNode:
		fmt.Fprintf(sb, "%sProject(%v)\n", indent, v.Columns)
	case AggregateNode:
		fmt.Fprintf(sb, "%sAggregate(exprs=%v, by=%v)\n", indent, v.Exprs, v.GroupBy)
	case SortNode:
		fmt.Fprintf(sb, "%sSort(%v)\n", indent, v.Fields)
	case LimitNode:
		fmt.Fprintf(sb, "%sLimit(%d)\n", indent, v.N)
	default:
		fmt.Fprintf(sb, "%s%s\n", indent, n.planNode())
	}
	for _, in := range n.Inputs() {
		writePlan(sb, in, depth+1)
	}
}

// Build lowers a parsed SelectQuery into an unoptimised logical plan, in
// exactly the fixed shape Scan -> Filter -> Project -> [Aggregate] ->
// [Sort] -> [Limit]. Build never fails on a well-formed AST; unknown field
// names are still resolved (as attribute lookups) rather than rejected here
// -- a query against an attribute that happens not to exist on any span is
// a valid query that returns nothing, not a parse-time error.
func Build(q SelectQuery) PlanNode {
	var node PlanNode = ScanNode{Table: "spans", Range: q.Range}

	var predicateTerms []Expr
	for _, m := range q.Selector {
		predicateTerms = append(predicateTerms, BinaryExpr{
			Op:    m.Op.String(),
			Left:  resolveField(m.Name),
			Right: Literal{Value: m.Value},
		})
	}

	var agg *Aggregation
	var sort *SortStage
	var limit *LimitStage
	for _, stage := range q.Stages {
		switch s := stage.(type) {
		case NumericFilter:
			predicateTerms = append(predicateTerms, BinaryExpr{
				Op:    s.Op.String(),
				Left:  resolveField(s.Field),
				Right: Literal{Value: numericFilterValue(s)},
			})
		case Aggregation:
			a := s
			agg = &a
		case SortStage:
			s2 := s
			sort = &s2
		case LimitStage:
			l2 := s
			limit = &l2
		}
	}

	if pred := AndAll(predicateTerms...); pred != nil {
		node = FilterNode{Input: node, Predicate: pred}
	} else {
		node = FilterNode{Input: node, Predicate: nil}
	}

	node = ProjectNode{Input: node, Columns: nil}

	if agg != nil {
		node = AggregateNode{Input: node, Exprs: agg.Exprs, GroupBy: agg.GroupBy}
	}
	if sort != nil {
		node = SortNode{Input: node, Fields: sort.Fields}
	}
	if limit != nil {
		node = LimitNode{Input: node, N: limit.N}
	}
	return node
}

// numericFilterValue converts a duration-unit value to nanoseconds (the
// native unit of duration_ns) and leaves a bare number untouched.
func numericFilterValue(f NumericFilter) float64 {
	if f.Unit == "" {
		return f.Value
	}
	mult, ok := durationUnitNanos[f.Unit]
	if !ok {
		return f.Value
	}
	return f.Value * mult
}

var durationUnitNanos = map[string]float64{
	"ns": 1,
	"us": 1e3,
	"ms": 1e6,
	"s":  1e9,
	"m":  60e9,
	"h":  3600e9,
}

// knownColumns maps a DSL field name to its physical spans-table column.
// Anything not in this map resolves as an attribute lookup instead.
var knownColumns = map[string]string{
	"service":         "service_name",
	"operation":       "span_name",
	"status":          "status_code",
	"kind":            "span_kind",
	"trace_id":        "trace_id",
	"span_id":         "span_id",
	"parent_span_id":  "parent_span_id",
	"duration":        "duration_ns",
	"sampling_weight": "sampling_weight",
}

func resolveField(name string) Expr {
	if col, ok := knownColumns[name]; ok {
		return ColumnRef{Name: col}
	}
	return AttrRef{Key: name}
}
