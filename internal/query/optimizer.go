package query

import (
	"sort"
	"strconv"
)

// NamedPass pairs a pass with the name docs/BENCHMARKS.md and the on/off
// ablation harness (cmd/querybench) identify it by.
type NamedPass struct {
	Name string
	Fn   func(PlanNode) PlanNode
}

// Passes is the fixed, approved pipeline order: constant folding first (so
// every later pass reasons about already-simplified predicates), then
// predicate pushdown, then the time-range-specific partition pruning, then
// limit pushdown, then projection pushdown last (it needs the fully settled
// plan to compute the referenced-column closure).
var Passes = []NamedPass{
	{"constant_fold", ConstantFold},
	{"predicate_pushdown", PredicatePushdown},
	{"partition_pruning", PartitionPruning},
	{"limit_pushdown", LimitPushdown},
	{"projection_pushdown", ProjectionPushdown},
}

// Optimize runs every pass in Passes, in order.
func Optimize(root PlanNode) PlanNode {
	return OptimizeExcept(root)
}

// OptimizeExcept runs every pass except those named in skip -- the on/off
// ablation benchmark's "off" case for pass X is OptimizeExcept(plan, X),
// isolating exactly that pass's effect while every other pass still runs
// normally (an unrealistic "nothing at all optimised" baseline would
// conflate multiple passes' effects into one number).
func OptimizeExcept(root PlanNode, skip ...string) PlanNode {
	skipSet := make(map[string]bool, len(skip))
	for _, s := range skip {
		skipSet[s] = true
	}
	for _, p := range Passes {
		if skipSet[p.Name] {
			continue
		}
		root = p.Fn(root)
	}
	return root
}

// ---- generic tree-rewrite helpers, shared by the passes below -------------

// withInput reconstructs a node of the same type with its single input
// replaced. Every node in this plan is unary (Inputs() returns 0 or 1
// element), which is what makes a single generic helper sufficient.
func withInput(n PlanNode, in PlanNode) PlanNode {
	switch v := n.(type) {
	case FilterNode:
		v.Input = in
		return v
	case ProjectNode:
		v.Input = in
		return v
	case AggregateNode:
		v.Input = in
		return v
	case SortNode:
		v.Input = in
		return v
	case LimitNode:
		v.Input = in
		return v
	default:
		return n
	}
}

// mapPost applies fn to every node, children before parents.
func mapPost(n PlanNode, fn func(PlanNode) PlanNode) PlanNode {
	if ins := n.Inputs(); len(ins) == 1 {
		n = withInput(n, mapPost(ins[0], fn))
	}
	return fn(n)
}

func hasAggregate(n PlanNode) bool {
	if _, ok := n.(AggregateNode); ok {
		return true
	}
	for _, in := range n.Inputs() {
		if hasAggregate(in) {
			return true
		}
	}
	return false
}

// ---- pass 1: constant folding + predicate simplification ------------------

// ConstantFold simplifies the AND-chain of comparison terms built by Build:
// duplicate terms collapse to one, range comparisons on the same field
// tighten to their intersection (duration>500ms AND duration>300ms becomes
// duration>500ms), and two contradictory equalities on the same field
// (status=error AND status=ok) fold the whole filter to a constant "no
// rows" -- caught here instead of silently scanning for an impossible
// result. The DSL has no explicit AND/OR/NOT keywords (every selector term
// and every filter stage implicitly ANDs), so this is the pass's whole
// scope; there is no arithmetic to fold.
func ConstantFold(root PlanNode) PlanNode {
	return mapPost(root, func(n PlanNode) PlanNode {
		f, ok := n.(FilterNode)
		if !ok || f.Predicate == nil {
			return n
		}
		f.Predicate = AndAll(simplifyAnd(flattenAnd(f.Predicate))...)
		return f
	})
}

func flattenAnd(e Expr) []Expr {
	if e == nil {
		return nil
	}
	if b, ok := e.(BinaryExpr); ok && b.Op == "AND" {
		return append(flattenAnd(b.Left), flattenAnd(b.Right)...)
	}
	return []Expr{e}
}

// simplifyAnd tightens and deduplicates a flat AND-chain. Terms that are not
// "field op literal" comparisons pass through untouched, in original order
// after the simplified numeric/equality terms.
func simplifyAnd(terms []Expr) []Expr {
	type bound struct {
		val       float64
		inclusive bool
		set       bool
	}
	type fieldState struct {
		left         Expr // the original ColumnRef/AttrRef, preserved verbatim for rebuilding
		lower, upper bound
		eq           map[string]any // dedup key -> original typed literal value
		neq          map[string]any
	}
	fields := map[string]*fieldState{}
	var order []string
	var passthrough []Expr
	seen := map[string]bool{} // exact-duplicate dedup, keyed by String()

	for _, t := range terms {
		b, ok := t.(BinaryExpr)
		if !ok {
			passthrough = append(passthrough, t)
			continue
		}
		key := fieldKey(b.Left)
		lit, isLit := b.Right.(Literal)
		if key == "" || !isLit {
			if !seen[t.String()] {
				seen[t.String()] = true
				passthrough = append(passthrough, t)
			}
			continue
		}
		fs, exists := fields[key]
		if !exists {
			fs = &fieldState{left: b.Left, eq: map[string]any{}, neq: map[string]any{}}
			fields[key] = fs
			order = append(order, key)
		}

		switch b.Op {
		case "=", "==":
			fs.eq[valueString(lit.Value)] = lit.Value
		case "!=":
			fs.neq[valueString(lit.Value)] = lit.Value
		case "=~", "!~":
			if !seen[t.String()] {
				seen[t.String()] = true
				passthrough = append(passthrough, t)
			}
		case ">", ">=":
			if v, ok := numericLiteral(lit.Value); ok {
				incl := b.Op == ">="
				if !fs.lower.set || v > fs.lower.val || (v == fs.lower.val && !incl) {
					fs.lower = bound{val: v, inclusive: incl, set: true}
				}
			}
		case "<", "<=":
			if v, ok := numericLiteral(lit.Value); ok {
				incl := b.Op == "<="
				if !fs.upper.set || v < fs.upper.val || (v == fs.upper.val && !incl) {
					fs.upper = bound{val: v, inclusive: incl, set: true}
				}
			}
		default:
			if !seen[t.String()] {
				seen[t.String()] = true
				passthrough = append(passthrough, t)
			}
		}
	}

	var out []Expr
	for _, key := range order {
		fs := fields[key]
		left := fs.left

		// Contradictory equalities on the same field: the whole predicate
		// can never match. Short-circuit the entire simplification.
		if len(fs.eq) > 1 {
			return []Expr{Literal{Value: false}}
		}
		for k, v := range fs.eq {
			// A value asserted both equal and not-equal to itself is also
			// a contradiction.
			if _, contradict := fs.neq[k]; contradict {
				return []Expr{Literal{Value: false}}
			}
			out = append(out, BinaryExpr{Op: "=", Left: left, Right: Literal{Value: v}})
		}
		for _, v := range fs.neq {
			if len(fs.eq) == 0 {
				out = append(out, BinaryExpr{Op: "!=", Left: left, Right: Literal{Value: v}})
			}
		}
		if fs.lower.set {
			op := ">"
			if fs.lower.inclusive {
				op = ">="
			}
			out = append(out, BinaryExpr{Op: op, Left: left, Right: Literal{Value: fs.lower.val}})
		}
		if fs.upper.set {
			// An impossible range (lower > upper, or equal with either
			// exclusive) also folds to "no rows".
			if fs.lower.set && (fs.lower.val > fs.upper.val ||
				(fs.lower.val == fs.upper.val && !(fs.lower.inclusive && fs.upper.inclusive))) {
				return []Expr{Literal{Value: false}}
			}
			op := "<"
			if fs.upper.inclusive {
				op = "<="
			}
			out = append(out, BinaryExpr{Op: op, Left: left, Right: Literal{Value: fs.upper.val}})
		}
	}
	out = append(out, passthrough...)

	// Stable output order makes this pass's tests deterministic regardless
	// of map iteration order.
	sort.Slice(out, func(i, j int) bool { return out[i].String() < out[j].String() })
	return out
}

func fieldKey(e Expr) string {
	switch v := e.(type) {
	case ColumnRef:
		return "col:" + v.Name
	case AttrRef:
		return "attr:" + v.Key
	default:
		return ""
	}
}

func numericLiteral(v any) (float64, bool) {
	f, ok := v.(float64)
	return f, ok
}

func valueString(v any) string {
	switch t := v.(type) {
	case string:
		return t
	case float64:
		return strconv.FormatFloat(t, 'g', -1, 64)
	case bool:
		if t {
			return "true"
		}
		return "false"
	default:
		return ""
	}
}

// ---- pass 2: predicate pushdown into Scan -----------------------------

// PredicatePushdown moves a Filter's predicate into its Scan's WHERE clause
// when the Filter sits directly above a Scan. It refuses to push a Filter
// across an Aggregate: a predicate there might reference the aggregate's
// OUTPUT (a HAVING-style filter), which does not exist until the Aggregate
// has actually run, so pushing it below would be evaluating a filter on
// values that are not computed yet. Sort and Limit are likewise left
// alone -- there is nothing to gain by commuting a row filter past either,
// and Aggregate is the only boundary that pushing would silently break.
func PredicatePushdown(root PlanNode) PlanNode {
	f, ok := root.(FilterNode)
	if !ok {
		if ins := root.Inputs(); len(ins) == 1 {
			return withInput(root, PredicatePushdown(ins[0]))
		}
		return root
	}
	newInput := PredicatePushdown(f.Input)
	if f.Predicate == nil {
		return FilterNode{Input: newInput, Predicate: nil}
	}
	if scan, ok := newInput.(ScanNode); ok {
		scan.Predicate = AndAll(scan.Predicate, f.Predicate)
		return FilterNode{Input: scan, Predicate: nil}
	}
	// newInput is Aggregate, Sort, Limit, or anything else a predicate
	// cannot safely commute past -- left in place.
	return FilterNode{Input: newInput, Predicate: f.Predicate}
}

// ---- pass 3: partition pruning from the time range -------------------

// PartitionPruning rewrites the Scan's time bound into the exact half-open
// form (timestamp >= start AND timestamp < end) that both the partition
// index (toDate(timestamp)) and the (service_name, span_name, timestamp)
// sort key can use to eliminate whole parts and granules. The predicate is
// deliberately never wrapped in a function (e.g. toYear(timestamp)=2026),
// which would defeat both indexes even though it is logically equivalent.
// Idempotent: running it twice does not duplicate the range term.
func PartitionPruning(root PlanNode) PlanNode {
	return mapPost(root, func(n PlanNode) PlanNode {
		s, ok := n.(ScanNode)
		if !ok {
			return n
		}
		lower := BinaryExpr{Op: ">=", Left: ColumnRef{Name: "timestamp"}, Right: Literal{Value: s.Range.Start}}
		upper := BinaryExpr{Op: "<", Left: ColumnRef{Name: "timestamp"}, Right: Literal{Value: s.Range.End}}
		existing := flattenAnd(s.Predicate)
		has := func(term Expr) bool {
			for _, e := range existing {
				if e.String() == term.String() {
					return true
				}
			}
			return false
		}
		var add []Expr
		if !has(lower) {
			add = append(add, lower)
		}
		if !has(upper) {
			add = append(add, upper)
		}
		if len(add) == 0 {
			return s
		}
		s.Predicate = AndAll(append(existing, add...)...)
		return s
	})
}

// ---- pass 4: limit pushdown, never across Aggregate ------------------

// LimitPushdown records, on the Scan node, whether a terminal Limit can rely
// on the scan itself stopping early. It can when nothing between Limit and
// Scan changes which rows count toward "the first N" in a way the scan
// doesn't already know about -- concretely, no Sort (row order isn't fixed
// until Sort runs, so "first N" is meaningless before it) and no Aggregate
// (limiting raw rows before grouping would silently corrupt every group's
// aggregate over an arbitrary N-row prefix). Filter and Project commute
// freely: neither changes which of the scan's own rows would have been
// emitted first. The Limit node itself is left in the tree either way --
// this pass only ever adds information, never removes the final LIMIT.
func LimitPushdown(root PlanNode) PlanNode {
	lim, ok := root.(LimitNode)
	if !ok {
		if ins := root.Inputs(); len(ins) == 1 {
			return withInput(root, LimitPushdown(ins[0]))
		}
		return root
	}
	if pushed, ok := tryPushLimit(lim.Input, lim.N); ok {
		lim.Input = pushed
	}
	return lim
}

func tryPushLimit(n PlanNode, limitN int) (PlanNode, bool) {
	switch v := n.(type) {
	case ScanNode:
		v.Limit = limitN
		return v, true
	case FilterNode:
		in, ok := tryPushLimit(v.Input, limitN)
		if !ok {
			return n, false
		}
		v.Input = in
		return v, true
	case ProjectNode:
		in, ok := tryPushLimit(v.Input, limitN)
		if !ok {
			return n, false
		}
		v.Input = in
		return v, true
	default: // AggregateNode, SortNode: refuse
		return n, false
	}
}

// ---- pass 5: projection pushdown ---------------------------------------

// ProjectionPushdown computes the transitive closure of columns actually
// referenced by the settled plan and restricts Scan (and the pass-through
// Project) to exactly that set. It runs last because it needs to see the
// final Filter predicate, GroupBy, aggregation fields and Sort fields to
// know what's needed.
//
// A plan with no Aggregate is left with Columns == nil (meaning "all
// columns"): a raw span listing has no explicit column list in the DSL, so
// narrowing it would silently drop data the caller didn't ask to exclude.
// The real payoff is aggregate queries, which by definition only ever look
// at a handful of fields out of a wide row -- exactly columnar storage's
// point, and the case the benchmark measures.
func ProjectionPushdown(root PlanNode) PlanNode {
	if !hasAggregate(root) {
		return root
	}
	cols := neededColumns(root)
	return mapPost(root, func(n PlanNode) PlanNode {
		switch v := n.(type) {
		case ScanNode:
			v.Columns = cols
			return v
		case ProjectNode:
			v.Columns = cols
			return v
		default:
			return n
		}
	})
}

func neededColumns(root PlanNode) []string {
	set := map[string]bool{"timestamp": true, "sampling_weight": true}
	add := func(name string) {
		if col, ok := knownColumns[name]; ok {
			set[col] = true
			return
		}
		set["span_attributes"] = true
		set["resource_attributes"] = true
	}
	var addExpr func(Expr)
	addExpr = func(e Expr) {
		switch v := e.(type) {
		case ColumnRef:
			set[v.Name] = true
		case AttrRef:
			set["span_attributes"] = true
			set["resource_attributes"] = true
		case BinaryExpr:
			addExpr(v.Left)
			addExpr(v.Right)
		}
	}

	var walk func(PlanNode)
	walk = func(n PlanNode) {
		switch v := n.(type) {
		case FilterNode:
			if v.Predicate != nil {
				addExpr(v.Predicate)
			}
		case ScanNode:
			if v.Predicate != nil {
				addExpr(v.Predicate)
			}
		case AggregateNode:
			for _, f := range v.GroupBy {
				add(f)
			}
			for _, e := range v.Exprs {
				if e.Field != "" {
					add(e.Field)
				}
			}
		case SortNode:
			for _, f := range v.Fields {
				add(f.Field)
			}
		}
		for _, in := range n.Inputs() {
			walk(in)
		}
	}
	walk(root)

	out := make([]string, 0, len(set))
	for c := range set {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}
