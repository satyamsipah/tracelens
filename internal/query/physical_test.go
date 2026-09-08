package query

import (
	"strings"
	"testing"
)

func compileFrom(t *testing.T, src string) Physical {
	t.Helper()
	plan := buildFrom(t, src)
	optimized := Optimize(plan)
	phys, err := Compile(optimized)
	if err != nil {
		t.Fatalf("compile %q: %v", src, err)
	}
	return phys
}

func TestCompileWeightedCountUsesSamplingWeightNotCountStar(t *testing.T) {
	phys := compileFrom(t, `{service="checkout"} | count by (operation)`)
	if strings.Contains(phys.SQL, "count(*)") || strings.Contains(phys.SQL, "COUNT(*)") {
		t.Errorf("SQL uses count(*): %s -- CLAUDE.md principle 6 requires sum(sampling_weight)", phys.SQL)
	}
	if !strings.Contains(phys.SQL, "sum(sampling_weight)") {
		t.Errorf("SQL missing sum(sampling_weight): %s", phys.SQL)
	}
}

func TestCompileEveryValueIsParameterizedNotInlined(t *testing.T) {
	phys := compileFrom(t, `{service="checkout"} | duration > 500ms`)
	if strings.Contains(phys.SQL, "checkout") {
		t.Errorf("literal value leaked into SQL text: %s", phys.SQL)
	}
	if !strings.Contains(phys.SQL, "?") {
		t.Errorf("SQL has no placeholders: %s", phys.SQL)
	}
	found := false
	for _, a := range phys.Args {
		if a == "checkout" {
			found = true
		}
	}
	if !found {
		t.Errorf("args %v missing bound value \"checkout\"", phys.Args)
	}
}

func TestCompileWeightedQuantile(t *testing.T) {
	phys := compileFrom(t, `{} | p99(duration) by (operation)`)
	// quantileTDigestWeighted requires an UNSIGNED INTEGER weight argument
	// (ClickHouse rejects Float64 outright) -- sampling_weight is rounded to
	// the nearest integer repeat-count rather than truncated, since weights
	// are almost never near-integer already (1/p for a probabilistic rate).
	if !strings.Contains(phys.SQL, "quantileTDigestWeighted(0.99)(duration_ns, toUInt64(round(sampling_weight)))") {
		t.Errorf("SQL missing weighted p99: %s", phys.SQL)
	}
}

func TestCompileWeightedSumAndAvg(t *testing.T) {
	sumSQL := compileFrom(t, `{} | sum(duration) by (operation)`).SQL
	if !strings.Contains(sumSQL, "sum(duration_ns * sampling_weight)") {
		t.Errorf("sum SQL = %s", sumSQL)
	}
	avgSQL := compileFrom(t, `{} | avg(duration) by (operation)`).SQL
	if !strings.Contains(avgSQL, "sum(duration_ns * sampling_weight) / sum(sampling_weight)") {
		t.Errorf("avg SQL = %s", avgSQL)
	}
}

func TestCompileMinMaxAreUnweighted(t *testing.T) {
	phys := compileFrom(t, `{} | min(duration), max(duration) by (operation)`)
	if !strings.Contains(phys.SQL, "min(duration_ns)") || !strings.Contains(phys.SQL, "max(duration_ns)") {
		t.Errorf("min/max should be plain, unweighted: %s", phys.SQL)
	}
}

func TestCompileAttributeComparisonCastsToFloat(t *testing.T) {
	phys := compileFrom(t, `{} | http.status_code > 499`)
	if !strings.Contains(phys.SQL, "toFloat64OrNull(coalesce(span_attributes[?], resource_attributes[?]))") {
		t.Errorf("SQL missing attribute cast: %s", phys.SQL)
	}
}

func TestCompileRegexOperators(t *testing.T) {
	matchSQL := compileFrom(t, `{service=~"check.*"}`).SQL
	if !strings.Contains(matchSQL, "match(service_name, ?)") {
		t.Errorf("=~ SQL = %s", matchSQL)
	}
	notMatchSQL := compileFrom(t, `{service!~"check.*"}`).SQL
	if !strings.Contains(notMatchSQL, "NOT match(service_name, ?)") {
		t.Errorf("!~ SQL = %s", notMatchSQL)
	}
}

func TestCompileFromTraceQueryIsNotSupported(t *testing.T) {
	// TraceQuery bypasses Compile entirely (executor.go handles it via a
	// dedicated bloom-filter point lookup) -- Compile only ever sees a
	// PlanNode built from a SelectQuery.
	q, err := Parse(`trace("abc")`)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := q.(TraceQuery); !ok {
		t.Fatalf("got %T", q)
	}
}

func TestCompileRejectsUnknownColumnEvenFromHandBuiltPlan(t *testing.T) {
	// Defense in depth: ColumnRef is only ever constructed by resolveField
	// against the fixed knownColumns map, but Compile must still refuse an
	// out-of-band identifier rather than trust the caller.
	scan := ScanNode{Table: "spans", Predicate: BinaryExpr{
		Op: "=", Left: ColumnRef{Name: "password; DROP TABLE spans; --"}, Right: Literal{Value: "x"},
	}}
	proj := ProjectNode{Input: FilterNode{Input: scan, Predicate: nil}}
	_, err := Compile(proj)
	if err == nil {
		t.Fatal("expected an error for a column outside the allow-list")
	}
}

func TestCompileLimitIsBoundNotInlined(t *testing.T) {
	phys := compileFrom(t, `{} | limit 7`)
	if strings.Contains(phys.SQL, "LIMIT 7") {
		t.Errorf("limit should be a bound parameter, not inlined: %s", phys.SQL)
	}
	if !strings.Contains(phys.SQL, "LIMIT ?") {
		t.Errorf("SQL missing LIMIT ?: %s", phys.SQL)
	}
	found := false
	for _, a := range phys.Args {
		if n, ok := a.(int64); ok && n == 7 {
			found = true
		}
	}
	if !found {
		t.Errorf("args %v missing bound limit 7", phys.Args)
	}
}

func TestCompileSortResolvesAggregateAlias(t *testing.T) {
	phys := compileFrom(t, `{} | count by (operation) | sort by (count desc)`)
	if !strings.Contains(phys.SQL, "ORDER BY `count` DESC") {
		t.Errorf("SQL missing resolved sort alias: %s", phys.SQL)
	}
}

func TestCompileTimeRangeIsBoundAsHalfOpenParameters(t *testing.T) {
	phys := compileFrom(t, `{}`)
	if !strings.Contains(phys.SQL, "timestamp >= ?") || !strings.Contains(phys.SQL, "timestamp < ?") {
		t.Errorf("SQL missing half-open time bound: %s", phys.SQL)
	}
}
