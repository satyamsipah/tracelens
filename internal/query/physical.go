package query

import (
	"fmt"
	"strings"
)

// Physical is a compiled, ready-to-run ClickHouse query. SQL never contains
// an inlined value: every literal is a "?" placeholder bound positionally in
// Args, and every identifier that reaches the string comes either from a
// fixed allow-list (allowedColumns) or from DSL text the lexer already
// restricts to [A-Za-z0-9_.] -- so even a hand-built PlanNode with an
// attacker-chosen AttrRef.Key is safe, because the key travels as a bound
// parameter, never as SQL text.
type Physical struct {
	SQL  string
	Args []any
}

// allowedColumns is the complete set of physical column names Compile will
// ever emit unescaped. Anything else is a bug in an optimiser pass or a
// hand-built plan, not user input -- it fails compilation rather than
// silently emitting an unvalidated identifier.
var allowedColumns = func() map[string]bool {
	m := map[string]bool{"timestamp": true, "span_attributes": true, "resource_attributes": true}
	for _, c := range knownColumns {
		m[c] = true
	}
	return m
}()

// allowedTables mirrors allowedColumns for ScanNode.Table: Build() hard-
// codes "spans" today, so nothing currently exercises the reject path, but
// the same "never trust an identifier that reaches SQL text" rule this file
// applies to every column should apply here too -- before Table ever
// becomes data-driven (a future logs/metrics query target), not after.
var allowedTables = map[string]bool{"spans": true}

type paramBuilder struct{ args []any }

func (p *paramBuilder) bind(v any) string {
	p.args = append(p.args, v)
	return "?"
}

// compiled is one node's SQL text plus the arguments its "?" placeholders
// need, in the same left-to-right order they appear in sql.
type compiled struct {
	sql  string
	args []any
}

// Compile lowers an optimised logical plan to one ClickHouse statement,
// compiling bottom-up: each node wraps its input in a subquery ONLY when it
// carries a real (unpushed) transformation, so a Filter whose predicate
// pushdown already absorbed into Scan is a pure pass-through, while an
// unpushed Filter genuinely produces a wrapping "SELECT * FROM (...) WHERE
// ..." rather than the same text either way. This is what makes every
// optimiser pass's effect real and measurable rather than something the
// compiler quietly does regardless -- see docs/BENCHMARKS.md for what each
// pass's presence/absence actually costs at 10M rows.
func Compile(root PlanNode) (Physical, error) {
	c, err := compileNode(root)
	if err != nil {
		return Physical{}, err
	}
	return Physical{SQL: c.sql, Args: c.args}, nil
}

func compileNode(n PlanNode) (compiled, error) {
	switch v := n.(type) {
	case ScanNode:
		return compileScan(v)
	case FilterNode:
		return compileFilter(v)
	case ProjectNode:
		// Project carries no SQL of its own in v1 (there is no explicit
		// "select these columns" DSL stage) -- the column restriction it
		// would apply already lives on Scan by the time ProjectionPushdown
		// has run, so this node is always a pure pass-through.
		return compileNode(v.Input)
	case AggregateNode:
		return compileAggregate(v)
	case SortNode:
		return compileSort(v)
	case LimitNode:
		return compileLimit(v)
	default:
		return compiled{}, fmt.Errorf("query: unsupported plan node %T", n)
	}
}

func compileScan(s ScanNode) (compiled, error) {
	if !allowedTables[s.Table] {
		return compiled{}, fmt.Errorf("query: table %q is not in the physical schema", s.Table)
	}
	pb := &paramBuilder{}

	var cols string
	if len(s.Columns) > 0 {
		list := make([]string, len(s.Columns))
		for i, c := range s.Columns {
			if !allowedColumns[c] {
				return compiled{}, fmt.Errorf("query: column %q is not in the physical schema", c)
			}
			list[i] = c
		}
		cols = strings.Join(list, ", ")
	} else {
		cols = "*"
	}

	var sb strings.Builder
	fmt.Fprintf(&sb, "SELECT %s FROM tracelens.%s", cols, s.Table)
	if s.Predicate != nil {
		where, err := exprToSQL(s.Predicate, pb)
		if err != nil {
			return compiled{}, err
		}
		fmt.Fprintf(&sb, " WHERE %s", where)
	}
	if s.Limit > 0 {
		fmt.Fprintf(&sb, " LIMIT %s", pb.bind(int64(s.Limit)))
	}
	return compiled{sql: sb.String(), args: pb.args}, nil
}

func compileFilter(f FilterNode) (compiled, error) {
	child, err := compileNode(f.Input)
	if err != nil {
		return compiled{}, err
	}
	if f.Predicate == nil {
		return child, nil
	}
	pb := &paramBuilder{args: child.args}
	where, err := exprToSQL(f.Predicate, pb)
	if err != nil {
		return compiled{}, err
	}
	sql := fmt.Sprintf("SELECT * FROM (%s) WHERE %s", child.sql, where)
	return compiled{sql: sql, args: pb.args}, nil
}

func compileAggregate(a AggregateNode) (compiled, error) {
	child, err := compileNode(a.Input)
	if err != nil {
		return compiled{}, err
	}
	pb := &paramBuilder{args: child.args}

	var selectList, groupBySQL []string
	for _, f := range a.GroupBy {
		expr, err := fieldSQL(f, pb)
		if err != nil {
			return compiled{}, err
		}
		groupBySQL = append(groupBySQL, expr)
		selectList = append(selectList, fmt.Sprintf("%s AS `%s`", expr, f))
	}
	for _, e := range a.Exprs {
		aggSQL, err := aggExprSQL(e, pb)
		if err != nil {
			return compiled{}, err
		}
		selectList = append(selectList, fmt.Sprintf("%s AS `%s`", aggSQL, e.Alias()))
	}

	sql := fmt.Sprintf("SELECT %s FROM (%s)", strings.Join(selectList, ", "), child.sql)
	if len(groupBySQL) > 0 {
		sql += " GROUP BY " + strings.Join(groupBySQL, ", ")
	}
	return compiled{sql: sql, args: pb.args}, nil
}

func compileSort(s SortNode) (compiled, error) {
	child, err := compileNode(s.Input)
	if err != nil {
		return compiled{}, err
	}
	agg := findAggregate(s.Input)

	var orderBy []string
	for _, f := range s.Fields {
		expr, err := sortFieldSQL(f.Field, agg)
		if err != nil {
			return compiled{}, err
		}
		dir := "ASC"
		if f.Desc {
			dir = "DESC"
		}
		orderBy = append(orderBy, fmt.Sprintf("%s %s", expr, dir))
	}
	sql := fmt.Sprintf("SELECT * FROM (%s) ORDER BY %s", child.sql, strings.Join(orderBy, ", "))
	return compiled{sql: sql, args: child.args}, nil
}

func compileLimit(l LimitNode) (compiled, error) {
	child, err := compileNode(l.Input)
	if err != nil {
		return compiled{}, err
	}
	pb := &paramBuilder{args: child.args}
	sql := fmt.Sprintf("SELECT * FROM (%s) LIMIT %s", child.sql, pb.bind(int64(l.N)))
	return compiled{sql: sql, args: pb.args}, nil
}

// findAggregate walks down a single-child chain looking for an Aggregate
// node, stopping at Scan. Used to decide whether a Sort field name resolves
// against aggregation output aliases or plain physical fields.
func findAggregate(n PlanNode) *AggregateNode {
	for {
		switch v := n.(type) {
		case AggregateNode:
			return &v
		case ScanNode:
			return nil
		default:
			ins := n.Inputs()
			if len(ins) != 1 {
				return nil
			}
			n = ins[0]
		}
	}
}

// sortFieldSQL resolves a "sort by (field ...)" name against the
// aggregation's own output aliases first (an agg expr alias or a group-by
// field), falling back to a plain field/attribute resolution for a
// non-aggregate (raw span listing) query.
func sortFieldSQL(name string, agg *AggregateNode) (string, error) {
	if agg != nil {
		for _, e := range agg.Exprs {
			if e.Alias() == name {
				return "`" + name + "`", nil
			}
		}
		for _, g := range agg.GroupBy {
			if g == name {
				return "`" + name + "`", nil
			}
		}
		return "", fmt.Errorf("query: sort field %q is not an aggregation output or group-by field", name)
	}
	pb := &paramBuilder{} // sort fields never need bound params (columns only)
	return fieldSQL(name, pb)
}

// fieldSQL resolves a DSL field name to its SQL expression: a known column
// resolves plainly, anything else resolves as a coalesced attribute lookup.
func fieldSQL(name string, pb *paramBuilder) (string, error) {
	if col, ok := knownColumns[name]; ok {
		if !allowedColumns[col] {
			return "", fmt.Errorf("query: column %q is not in the physical schema", col)
		}
		return col, nil
	}
	return attrSQL(name, pb), nil
}

func attrSQL(key string, pb *paramBuilder) string {
	return fmt.Sprintf("coalesce(span_attributes[%s], resource_attributes[%s])", pb.bind(key), pb.bind(key))
}

// numericFieldSQL is fieldSQL but casts an attribute lookup to Float64,
// since attribute values are stored as String (DECISIONS.md: "Attribute
// values flatten to String... numeric predicates must cast").
func numericFieldSQL(name string, pb *paramBuilder) string {
	if col, ok := knownColumns[name]; ok {
		return col
	}
	return "toFloat64OrNull(" + attrSQL(name, pb) + ")"
}

func aggExprSQL(e AggExpr, pb *paramBuilder) (string, error) {
	switch e.Func {
	case AggCount:
		return "sum(sampling_weight)", nil
	case AggSum:
		return fmt.Sprintf("sum(%s * sampling_weight)", numericFieldSQL(e.Field, pb)), nil
	case AggAvg:
		f := numericFieldSQL(e.Field, pb)
		return fmt.Sprintf("sum(%s * sampling_weight) / sum(sampling_weight)", f), nil
	case AggMin:
		return fmt.Sprintf("min(%s)", numericFieldSQL(e.Field, pb)), nil
	case AggMax:
		return fmt.Sprintf("max(%s)", numericFieldSQL(e.Field, pb)), nil
	case AggP50:
		return fmt.Sprintf("quantileTDigestWeighted(0.5)(%s, toUInt64(round(sampling_weight)))", numericFieldSQL(e.Field, pb)), nil
	case AggP95:
		return fmt.Sprintf("quantileTDigestWeighted(0.95)(%s, toUInt64(round(sampling_weight)))", numericFieldSQL(e.Field, pb)), nil
	case AggP99:
		return fmt.Sprintf("quantileTDigestWeighted(0.99)(%s, toUInt64(round(sampling_weight)))", numericFieldSQL(e.Field, pb)), nil
	default:
		return "", fmt.Errorf("query: unknown aggregation function %q", e.Func)
	}
}

// exprToSQL renders a predicate tree. Every column identifier is checked
// against allowedColumns; every value, including an attribute map key,
// is bound as a "?" parameter.
func exprToSQL(e Expr, pb *paramBuilder) (string, error) {
	switch v := e.(type) {
	case nil:
		return "1", nil
	case Literal:
		if b, ok := v.Value.(bool); ok {
			if b {
				return "1", nil
			}
			return "0", nil
		}
		return pb.bind(v.Value), nil
	case BinaryExpr:
		if v.Op == "AND" {
			l, err := exprToSQL(v.Left, pb)
			if err != nil {
				return "", err
			}
			r, err := exprToSQL(v.Right, pb)
			if err != nil {
				return "", err
			}
			return fmt.Sprintf("(%s AND %s)", l, r), nil
		}
		return binaryComparisonSQL(v, pb)
	default:
		return "", fmt.Errorf("query: unsupported expression %T in predicate", e)
	}
}

func binaryComparisonSQL(b BinaryExpr, pb *paramBuilder) (string, error) {
	lit, ok := b.Right.(Literal)
	if !ok {
		return "", fmt.Errorf("query: comparison right-hand side must be a literal, got %T", b.Right)
	}

	numericOp := b.Op == ">" || b.Op == ">=" || b.Op == "<" || b.Op == "<="
	var operand string
	switch left := b.Left.(type) {
	case ColumnRef:
		if !allowedColumns[left.Name] {
			return "", fmt.Errorf("query: column %q is not in the physical schema", left.Name)
		}
		operand = left.Name
	case AttrRef:
		if numericOp {
			operand = "toFloat64OrNull(" + attrSQL(left.Key, pb) + ")"
		} else {
			operand = attrSQL(left.Key, pb)
		}
	default:
		return "", fmt.Errorf("query: unsupported comparison operand %T", b.Left)
	}

	switch b.Op {
	case "=", "==":
		return fmt.Sprintf("%s = %s", operand, pb.bind(lit.Value)), nil
	case "!=":
		return fmt.Sprintf("%s != %s", operand, pb.bind(lit.Value)), nil
	case "=~":
		return fmt.Sprintf("match(%s, %s)", operand, pb.bind(lit.Value)), nil
	case "!~":
		return fmt.Sprintf("NOT match(%s, %s)", operand, pb.bind(lit.Value)), nil
	case ">", ">=", "<", "<=":
		return fmt.Sprintf("%s %s %s", operand, b.Op, pb.bind(lit.Value)), nil
	default:
		return "", fmt.Errorf("query: unsupported operator %q", b.Op)
	}
}
