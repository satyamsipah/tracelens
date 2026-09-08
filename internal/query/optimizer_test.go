package query

import (
	"strings"
	"testing"
	"time"
)

// ---- pass 1: constant folding -------------------------------------------

func TestConstantFoldTightensRangeOnSameField(t *testing.T) {
	plan := buildFrom(t, `{} | duration > 500ms | duration > 300ms`)
	folded := ConstantFold(plan)
	filter := folded.(ProjectNode).Input.(FilterNode)
	terms := flattenAnd(filter.Predicate)
	if len(terms) != 1 {
		t.Fatalf("got %d terms after folding, want 1 (tightest bound only): %v", len(terms), terms)
	}
	b := terms[0].(BinaryExpr)
	if b.Op != ">" || b.Right.(Literal).Value.(float64) != 500*1e6 {
		t.Errorf("tightened term = %s, want duration > 500ms in ns", terms[0])
	}
}

func TestConstantFoldDeduplicatesIdenticalTerms(t *testing.T) {
	plan := buildFrom(t, `{service="checkout", service="checkout"}`)
	folded := ConstantFold(plan)
	filter := folded.(ProjectNode).Input.(FilterNode)
	terms := flattenAnd(filter.Predicate)
	if len(terms) != 1 {
		t.Fatalf("got %d terms, want 1 duplicate collapsed: %v", len(terms), terms)
	}
}

func TestConstantFoldDetectsContradictoryEquality(t *testing.T) {
	plan := buildFrom(t, `{status=error, status=ok}`)
	folded := ConstantFold(plan)
	filter := folded.(ProjectNode).Input.(FilterNode)
	terms := flattenAnd(filter.Predicate)
	if len(terms) != 1 {
		t.Fatalf("got %d terms, want 1 (constant false)", len(terms))
	}
	lit, ok := terms[0].(Literal)
	if !ok || lit.Value != false {
		t.Errorf("term = %v, want Literal(false)", terms[0])
	}
}

func TestConstantFoldDetectsImpossibleRange(t *testing.T) {
	plan := buildFrom(t, `{} | duration > 1s | duration < 500ms`)
	folded := ConstantFold(plan)
	filter := folded.(ProjectNode).Input.(FilterNode)
	terms := flattenAnd(filter.Predicate)
	if len(terms) != 1 {
		t.Fatalf("got %d terms, want 1 (constant false)", len(terms))
	}
	lit, ok := terms[0].(Literal)
	if !ok || lit.Value != false {
		t.Errorf("term = %v, want Literal(false) for an impossible range", terms[0])
	}
}

func TestConstantFoldLeavesUnrelatedFieldsAlone(t *testing.T) {
	plan := buildFrom(t, `{service="checkout"} | duration > 500ms`)
	folded := ConstantFold(plan)
	filter := folded.(ProjectNode).Input.(FilterNode)
	terms := flattenAnd(filter.Predicate)
	if len(terms) != 2 {
		t.Fatalf("got %d terms, want 2 (unrelated fields untouched): %v", len(terms), terms)
	}
}

// ---- pass 2: predicate pushdown -----------------------------------------

func TestPredicatePushdownMovesFilterIntoScan(t *testing.T) {
	plan := buildFrom(t, `{service="checkout"} | duration > 500ms`)
	plan = ConstantFold(plan)
	pushed := PredicatePushdown(plan)

	filter := pushed.(ProjectNode).Input.(FilterNode)
	if filter.Predicate != nil {
		t.Errorf("Filter.Predicate = %v, want nil after pushdown", filter.Predicate)
	}
	scan, ok := filter.Input.(ScanNode)
	if !ok {
		t.Fatalf("Filter.Input = %T, want ScanNode", filter.Input)
	}
	if scan.Predicate == nil {
		t.Fatal("Scan.Predicate is nil, want the pushed-down predicate")
	}
	if !strings.Contains(scan.Predicate.String(), "checkout") {
		t.Errorf("Scan.Predicate = %s, missing pushed term", scan.Predicate)
	}
}

// A Filter sitting above an Aggregate cannot push below it: the predicate
// might reference the aggregate's OUTPUT, which doesn't exist until
// Aggregate has run. The current grammar can't produce this shape, so it is
// built by hand to test the boundary directly.
func TestPredicatePushdownRefusesToCrossAggregate(t *testing.T) {
	scan := ScanNode{Table: "spans"}
	agg := AggregateNode{Input: scan, Exprs: []AggExpr{{Func: AggCount}}, GroupBy: []string{"span_name"}}
	havingLike := BinaryExpr{Op: ">", Left: ColumnRef{Name: "count"}, Right: Literal{Value: 5.0}}
	plan := FilterNode{Input: agg, Predicate: havingLike}

	pushed := PredicatePushdown(plan)

	f, ok := pushed.(FilterNode)
	if !ok {
		t.Fatalf("got %T, want FilterNode left in place", pushed)
	}
	if f.Predicate == nil {
		t.Fatal("predicate was dropped, not just left unpushed")
	}
	a, ok := f.Input.(AggregateNode)
	if !ok {
		t.Fatalf("Filter.Input = %T, want AggregateNode untouched", f.Input)
	}
	if a.Input.(ScanNode).Predicate != nil {
		t.Error("predicate leaked into Scan across an Aggregate boundary")
	}
}

// ---- pass 3: partition pruning -------------------------------------------

func TestPartitionPruningInjectsHalfOpenTimestampBound(t *testing.T) {
	plan := buildFrom(t, `{service="checkout"}`)
	plan = PredicatePushdown(ConstantFold(plan))
	pruned := PartitionPruning(plan)

	scan := pruned.(ProjectNode).Input.(FilterNode).Input.(ScanNode)
	terms := flattenAnd(scan.Predicate)
	var haveLower, haveUpper bool
	for _, term := range terms {
		b, ok := term.(BinaryExpr)
		if !ok {
			continue
		}
		col, ok := b.Left.(ColumnRef)
		if !ok || col.Name != "timestamp" {
			continue
		}
		switch b.Op {
		case ">=":
			haveLower = true
		case "<":
			haveUpper = true
		default:
			t.Errorf("timestamp bound used op %q, want half-open >= / < (never wrapped in a function)", b.Op)
		}
	}
	if !haveLower || !haveUpper {
		t.Fatalf("missing half-open timestamp bound in %v", terms)
	}
}

func TestPartitionPruningIsIdempotent(t *testing.T) {
	plan := buildFrom(t, `{}`)
	plan = PredicatePushdown(ConstantFold(plan))
	once := PartitionPruning(plan)
	twice := PartitionPruning(once)

	scan1 := once.(ProjectNode).Input.(FilterNode).Input.(ScanNode)
	scan2 := twice.(ProjectNode).Input.(FilterNode).Input.(ScanNode)
	if len(flattenAnd(scan1.Predicate)) != len(flattenAnd(scan2.Predicate)) {
		t.Errorf("running the pass twice changed the term count: %d vs %d",
			len(flattenAnd(scan1.Predicate)), len(flattenAnd(scan2.Predicate)))
	}
}

func TestPartitionPruningDefaultsToLastHourWhenRangeOmitted(t *testing.T) {
	q, err := NewParser(`{}`, fixedNow).Parse()
	if err != nil {
		t.Fatal(err)
	}
	sq := q.(SelectQuery)
	if sq.Range.Explicit {
		t.Fatal("expected an implicit default range")
	}
	if got, want := sq.Range.End.Sub(sq.Range.Start), time.Hour; got != want {
		t.Errorf("default range = %s, want %s", got, want)
	}
}

// ---- pass 4: limit pushdown -----------------------------------------------

func TestLimitPushdownReachesScanThroughFilterAndProject(t *testing.T) {
	plan := buildFrom(t, `{service="checkout"} | limit 20`)
	plan = ProjectionPushdown(PartitionPruning(PredicatePushdown(ConstantFold(plan))))
	pushed := LimitPushdown(plan)

	lim, ok := pushed.(LimitNode)
	if !ok {
		t.Fatalf("root = %T, want LimitNode (the node itself must remain)", pushed)
	}
	scan := findScan(t, lim.Input)
	if scan.Limit != 20 {
		t.Errorf("Scan.Limit = %d, want 20 (pushdown should reach the scan)", scan.Limit)
	}
}

func TestLimitPushdownNeverCrossesAggregate(t *testing.T) {
	plan := buildFrom(t, `{service="checkout"} | count by (operation) | limit 5`)
	plan = PartitionPruning(PredicatePushdown(ConstantFold(plan)))
	pushed := LimitPushdown(plan)

	lim, ok := pushed.(LimitNode)
	if !ok {
		t.Fatalf("root = %T, want LimitNode", pushed)
	}
	if _, ok := lim.Input.(AggregateNode); !ok {
		t.Fatalf("Limit.Input = %T, want AggregateNode directly beneath (not pushed through it)", lim.Input)
	}
	scan := findScan(t, lim.Input)
	if scan.Limit != 0 {
		t.Errorf("Scan.Limit = %d, want 0 -- pushing a raw-row limit below Aggregate would corrupt every group's count", scan.Limit)
	}
}

func findScan(t *testing.T, n PlanNode) ScanNode {
	t.Helper()
	for {
		if s, ok := n.(ScanNode); ok {
			return s
		}
		ins := n.Inputs()
		if len(ins) != 1 {
			t.Fatalf("no Scan node found under %T", n)
		}
		n = ins[0]
	}
}

// ---- pass 5: projection pushdown -----------------------------------------

func TestProjectionPushdownRestrictsColumnsForAggregateQuery(t *testing.T) {
	plan := buildFrom(t, `{service="checkout"} | count by (operation)`)
	plan = PartitionPruning(PredicatePushdown(ConstantFold(plan)))
	pushed := ProjectionPushdown(plan)

	scan := findScan(t, pushed)
	if scan.Columns == nil {
		t.Fatal("Scan.Columns is nil, want a restricted set for an aggregate query")
	}
	want := map[string]bool{"service_name": true, "span_name": true, "timestamp": true, "sampling_weight": true}
	if len(scan.Columns) != len(want) {
		t.Fatalf("Scan.Columns = %v, want exactly %v", scan.Columns, want)
	}
	for _, c := range scan.Columns {
		if !want[c] {
			t.Errorf("unexpected column %q pushed into scan (columnar storage's whole point is NOT reading this)", c)
		}
	}
	for c := range want {
		found := false
		for _, sc := range scan.Columns {
			if sc == c {
				found = true
			}
		}
		if !found {
			t.Errorf("missing expected column %q", c)
		}
	}
}

func TestProjectionPushdownLeavesRawListingUnrestricted(t *testing.T) {
	plan := buildFrom(t, `{service="checkout"}`)
	plan = PartitionPruning(PredicatePushdown(ConstantFold(plan)))
	pushed := ProjectionPushdown(plan)

	scan := findScan(t, pushed)
	if scan.Columns != nil {
		t.Errorf("Scan.Columns = %v, want nil (a raw span listing has no explicit column list to narrow to)", scan.Columns)
	}
}

func TestProjectionPushdownIncludesBothAttributeMapsForAttrRef(t *testing.T) {
	plan := buildFrom(t, `{} | count by (http.route)`)
	plan = PartitionPruning(PredicatePushdown(ConstantFold(plan)))
	pushed := ProjectionPushdown(plan)

	scan := findScan(t, pushed)
	has := map[string]bool{}
	for _, c := range scan.Columns {
		has[c] = true
	}
	if !has["span_attributes"] || !has["resource_attributes"] {
		t.Errorf("Scan.Columns = %v, want both attribute maps for an AttrRef group-by", scan.Columns)
	}
}

// ---- full pipeline sanity -------------------------------------------------

func TestOptimizeRunsAllFivePassesInOrder(t *testing.T) {
	plan := buildFrom(t, `{service="checkout", status=error} | duration > 500ms | count by (operation) | limit 10`)
	optimized := Optimize(plan)

	scan := findScan(t, optimized)
	if scan.Predicate == nil {
		t.Error("predicate pushdown did not run")
	}
	if !strings.Contains(scan.Predicate.String(), "timestamp") {
		t.Error("partition pruning did not run")
	}
	if scan.Columns == nil {
		t.Error("projection pushdown did not run")
	}
	// count by (operation) means Limit sits above Aggregate -- must NOT push.
	if scan.Limit != 0 {
		t.Error("limit pushdown incorrectly crossed an Aggregate boundary")
	}
}
