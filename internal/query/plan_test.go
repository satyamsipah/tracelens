package query

import (
	"strings"
	"testing"
)

func buildFrom(t *testing.T, src string) PlanNode {
	t.Helper()
	q, err := NewParser(src, fixedNow).Parse()
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	sq, ok := q.(SelectQuery)
	if !ok {
		t.Fatalf("got %T, want SelectQuery", q)
	}
	return Build(sq)
}

// Build must always produce the fixed Scan -> Filter -> Project ->
// [Aggregate] -> [Sort] -> [Limit] shape, regardless of which stages the
// query text actually used.
func TestBuildProducesFixedNodeOrder(t *testing.T) {
	plan := buildFrom(t, `{service="checkout"} | duration > 500ms | count by (operation) | sort by (count desc) | limit 10`)

	limit, ok := plan.(LimitNode)
	if !ok {
		t.Fatalf("root: got %T, want LimitNode", plan)
	}
	sortN, ok := limit.Input.(SortNode)
	if !ok {
		t.Fatalf("under Limit: got %T, want SortNode", limit.Input)
	}
	aggN, ok := sortN.Input.(AggregateNode)
	if !ok {
		t.Fatalf("under Sort: got %T, want AggregateNode", sortN.Input)
	}
	projN, ok := aggN.Input.(ProjectNode)
	if !ok {
		t.Fatalf("under Aggregate: got %T, want ProjectNode", aggN.Input)
	}
	filterN, ok := projN.Input.(FilterNode)
	if !ok {
		t.Fatalf("under Project: got %T, want FilterNode", projN.Input)
	}
	if _, ok := filterN.Input.(ScanNode); !ok {
		t.Fatalf("under Filter: got %T, want ScanNode", filterN.Input)
	}
}

func TestBuildOmitsOptionalStagesWhenAbsent(t *testing.T) {
	plan := buildFrom(t, `{service="checkout"}`)
	proj, ok := plan.(ProjectNode)
	if !ok {
		t.Fatalf("root: got %T, want ProjectNode (no aggregate/sort/limit present)", plan)
	}
	if _, ok := proj.Input.(FilterNode); !ok {
		t.Fatalf("under Project: got %T, want FilterNode", proj.Input)
	}
}

func TestBuildResolvesKnownFieldsAndAttributes(t *testing.T) {
	plan := buildFrom(t, `{service="checkout", http.route="/pay"}`)
	filter := plan.(ProjectNode).Input.(FilterNode)
	and := filter.Predicate.(BinaryExpr)
	if and.Op != "AND" {
		t.Fatalf("predicate = %s, want an AND chain", filter.Predicate)
	}
	left := and.Left.(BinaryExpr)
	if _, ok := left.Left.(ColumnRef); !ok {
		t.Errorf("service should resolve to a ColumnRef, got %T", left.Left)
	}
	right := and.Right.(BinaryExpr)
	if _, ok := right.Left.(AttrRef); !ok {
		t.Errorf("http.route should resolve to an AttrRef, got %T", right.Left)
	}
}

func TestBuildConvertsDurationUnitsToNanoseconds(t *testing.T) {
	plan := buildFrom(t, `{} | duration > 500ms`)
	filter := plan.(ProjectNode).Input.(FilterNode)
	cmp := filter.Predicate.(BinaryExpr)
	lit := cmp.Right.(Literal)
	if lit.Value.(float64) != 500*1e6 {
		t.Errorf("duration literal = %v, want 500ms in ns", lit.Value)
	}
}

func TestPlanStringIsReadable(t *testing.T) {
	plan := buildFrom(t, `{service="checkout"} | count by (operation)`)
	s := PlanString(plan)
	for _, want := range []string{"Aggregate", "Project", "Filter", "Scan"} {
		if !strings.Contains(s, want) {
			t.Errorf("PlanString missing %q:\n%s", want, s)
		}
	}
}
