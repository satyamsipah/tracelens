package query

import (
	"strings"
	"testing"
)

// These tests exist because the physical compiler is compositional
// (bottom-up, wrapping a node in a subquery only when it carries a genuinely
// unpushed transformation): a pass that runs must produce DIFFERENT SQL
// than the identical plan with that pass skipped, or the pass has no real
// effect regardless of what the logical plan looks like. This is exactly
// the property docs/BENCHMARKS.md's on/off numbers depend on being real.

func compileWithout(t *testing.T, src string, skip string) Physical {
	t.Helper()
	plan := buildFrom(t, src)
	optimized := OptimizeExcept(plan, skip)
	phys, err := Compile(optimized)
	if err != nil {
		t.Fatalf("compile without %s: %v", skip, err)
	}
	return phys
}

func TestAblationPredicatePushdownChangesGeneratedSQL(t *testing.T) {
	src := `{service="checkout"} | duration > 500ms`
	on := compileFrom(t, src)
	off := compileWithout(t, src, "predicate_pushdown")

	if on.SQL == off.SQL {
		t.Fatal("predicate pushdown on vs off produced identical SQL -- the pass has no measurable effect")
	}
	// Off: the predicate is evaluated in a WRAPPING subquery over an
	// otherwise-unfiltered scan, not inside the scan's own WHERE.
	if !strings.Contains(off.SQL, "FROM (SELECT * FROM tracelens.spans") {
		t.Errorf("without pushdown, expected an unfiltered inner scan wrapped in a filter: %s", off.SQL)
	}
	// On: the scan's own FROM clause carries the predicate directly.
	if strings.Contains(on.SQL, "FROM (SELECT * FROM tracelens.spans)") {
		t.Errorf("with pushdown, the scan should not be a bare unfiltered subquery: %s", on.SQL)
	}
}

func TestAblationProjectionPushdownChangesGeneratedSQL(t *testing.T) {
	src := `{service="checkout"} | count by (operation)`
	on := compileFrom(t, src)
	off := compileWithout(t, src, "projection_pushdown")

	if on.SQL == off.SQL {
		t.Fatal("projection pushdown on vs off produced identical SQL")
	}
	if !strings.Contains(off.SQL, "SELECT * FROM tracelens.spans") {
		t.Errorf("without projection pushdown, expected the scan to read every column via SELECT *: %s", off.SQL)
	}
	if strings.Contains(on.SQL, "SELECT * FROM tracelens.spans") {
		t.Errorf("with projection pushdown, the scan should read a restricted column list, not *: %s", on.SQL)
	}
}

func TestAblationLimitPushdownChangesGeneratedSQL(t *testing.T) {
	src := `{service="checkout"} | limit 20`
	on := compileFrom(t, src)
	off := compileWithout(t, src, "limit_pushdown")

	if on.SQL == off.SQL {
		t.Fatal("limit pushdown on vs off produced identical SQL")
	}
	// On: the scan's own inner SQL carries a LIMIT, letting ClickHouse stop
	// reading early. Off: LIMIT appears only in the outermost wrapper, over
	// a scan that must be fully read (and filtered) before it applies.
	if strings.Count(on.SQL, "LIMIT") < 2 {
		t.Errorf("with pushdown, expected LIMIT at both the scan and the outer query: %s", on.SQL)
	}
	if strings.Count(off.SQL, "LIMIT") != 1 {
		t.Errorf("without pushdown, expected exactly one LIMIT (the outer wrapper only): %s", off.SQL)
	}
}

func TestAblationPartitionPruningChangesGeneratedSQL(t *testing.T) {
	src := `{service="checkout"}`
	on := compileFrom(t, src)
	off := compileWithout(t, src, "partition_pruning")

	if strings.Contains(off.SQL, "timestamp") {
		t.Errorf("without partition pruning, expected no timestamp bound at all: %s", off.SQL)
	}
	if !strings.Contains(on.SQL, "timestamp >=") || !strings.Contains(on.SQL, "timestamp <") {
		t.Errorf("with partition pruning, expected the half-open timestamp bound: %s", on.SQL)
	}
}

func TestAblationConstantFoldChangesGeneratedSQL(t *testing.T) {
	src := `{} | duration > 500ms | duration > 300ms`
	on := compileFrom(t, src)
	off := compileWithout(t, src, "constant_fold")

	onCount := strings.Count(on.SQL, "duration_ns >")
	offCount := strings.Count(off.SQL, "duration_ns >")
	if onCount != 1 {
		t.Errorf("with constant folding, expected exactly one tightened duration comparison, got %d: %s", onCount, on.SQL)
	}
	if offCount != 2 {
		t.Errorf("without constant folding, expected both redundant duration comparisons to survive, got %d: %s", offCount, off.SQL)
	}
}
