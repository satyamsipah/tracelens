package query

import (
	"strings"
	"testing"
	"time"
)

func fixedNow() time.Time {
	return time.Date(2026, 9, 8, 12, 0, 0, 0, time.UTC)
}

func mustParse(t *testing.T, src string) Query {
	t.Helper()
	q, err := NewParser(src, fixedNow).Parse()
	if err != nil {
		t.Fatalf("parse %q: %v", src, err)
	}
	return q
}

func TestParserShouldParseExampleQueryIntoExpectedShape(t *testing.T) {
	q := mustParse(t, `{service="checkout", status=error} | duration > 500ms | count by (operation)`)
	sq, ok := q.(SelectQuery)
	if !ok {
		t.Fatalf("got %T, want SelectQuery", q)
	}
	if len(sq.Selector) != 2 {
		t.Fatalf("selector: got %d matchers, want 2", len(sq.Selector))
	}
	if sq.Selector[0] != (LabelMatcher{Name: "service", Op: OpEq, Value: "checkout"}) {
		t.Errorf("selector[0] = %+v", sq.Selector[0])
	}
	if sq.Selector[1] != (LabelMatcher{Name: "status", Op: OpEq, Value: "error"}) {
		t.Errorf("selector[1] = %+v", sq.Selector[1])
	}
	if sq.Range.Explicit {
		t.Errorf("range should default to implicit (1h), got explicit")
	}
	if got, want := sq.Range.End.Sub(sq.Range.Start), time.Hour; got != want {
		t.Errorf("default range width = %s, want %s", got, want)
	}
	if len(sq.Stages) != 2 {
		t.Fatalf("stages: got %d, want 2", len(sq.Stages))
	}
	nf, ok := sq.Stages[0].(NumericFilter)
	if !ok {
		t.Fatalf("stage 0: got %T, want NumericFilter", sq.Stages[0])
	}
	if nf.Field != "duration" || nf.Op != CmpGT || nf.Value != 500 || nf.Unit != "ms" {
		t.Errorf("stage 0 = %+v", nf)
	}
	agg, ok := sq.Stages[1].(Aggregation)
	if !ok {
		t.Fatalf("stage 1: got %T, want Aggregation", sq.Stages[1])
	}
	if len(agg.Exprs) != 1 || agg.Exprs[0].Func != AggCount || agg.Exprs[0].Field != "" {
		t.Errorf("agg exprs = %+v", agg.Exprs)
	}
	if len(agg.GroupBy) != 1 || agg.GroupBy[0] != "operation" {
		t.Errorf("agg group by = %+v", agg.GroupBy)
	}
}

func TestParserLabelOperators(t *testing.T) {
	tests := []struct {
		src  string
		want LabelOp
	}{
		{`{service="a"}`, OpEq},
		{`{service!="a"}`, OpNeq},
		{`{service=~"a.*"}`, OpRegexMatch},
		{`{service!~"a.*"}`, OpRegexNotMatch},
	}
	for _, tt := range tests {
		sq := mustParse(t, tt.src).(SelectQuery)
		if sq.Selector[0].Op != tt.want {
			t.Errorf("%s: op = %v, want %v", tt.src, sq.Selector[0].Op, tt.want)
		}
	}
}

func TestParserEmptySelectorMatchesEverything(t *testing.T) {
	sq := mustParse(t, `{}`).(SelectQuery)
	if len(sq.Selector) != 0 {
		t.Errorf("selector = %+v, want empty", sq.Selector)
	}
}

func TestParserTraceQuery(t *testing.T) {
	q := mustParse(t, `trace("abcd1234")`)
	tq, ok := q.(TraceQuery)
	if !ok {
		t.Fatalf("got %T, want TraceQuery", q)
	}
	if tq.TraceID != "abcd1234" {
		t.Errorf("trace id = %q", tq.TraceID)
	}
}

func TestParserShouldRejectEmptyTraceID(t *testing.T) {
	if _, err := Parse(`trace("")`); err == nil {
		t.Fatal("expected error for empty trace id")
	}
}

func TestParserSinceClause(t *testing.T) {
	sq := mustParse(t, `{} since 30m`).(SelectQuery)
	if !sq.Range.Explicit {
		t.Error("range should be explicit")
	}
	if got, want := sq.Range.End.Sub(sq.Range.Start), 30*time.Minute; got != want {
		t.Errorf("range width = %s, want %s", got, want)
	}
	if !sq.Range.End.Equal(fixedNow()) {
		t.Errorf("range end = %s, want now", sq.Range.End)
	}
}

func TestParserCompoundDuration(t *testing.T) {
	sq := mustParse(t, `{} since 1h30m`).(SelectQuery)
	if got, want := sq.Range.End.Sub(sq.Range.Start), 90*time.Minute; got != want {
		t.Errorf("range width = %s, want %s", got, want)
	}
}

func TestParserRangeClauseAbsoluteAndRelative(t *testing.T) {
	sq := mustParse(t, `{} range("2026-01-01T00:00:00Z", now)`).(SelectQuery)
	wantStart := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	if !sq.Range.Start.Equal(wantStart) {
		t.Errorf("range start = %s, want %s", sq.Range.Start, wantStart)
	}
	if !sq.Range.End.Equal(fixedNow()) {
		t.Errorf("range end = %s, want now", sq.Range.End)
	}

	sq2 := mustParse(t, `{} range(now-2h, now-1h)`).(SelectQuery)
	if got, want := sq2.Range.End.Sub(sq2.Range.Start), time.Hour; got != want {
		t.Errorf("range width = %s, want %s", got, want)
	}
}

func TestParserShouldRejectInvertedTimeRange(t *testing.T) {
	if _, err := Parse(`{} range(now, now-1h)`); err == nil {
		t.Fatal("expected error for start after end")
	}
}

func TestParserMultipleAggregationsAndSortAndLimit(t *testing.T) {
	sq := mustParse(t, `{service="checkout"} | p50(duration), p95(duration), p99(duration) by (operation) | sort by (p99_duration desc) | limit 20`).(SelectQuery)
	agg := sq.Stages[0].(Aggregation)
	if len(agg.Exprs) != 3 {
		t.Fatalf("got %d agg exprs, want 3", len(agg.Exprs))
	}
	for i, want := range []AggFunc{AggP50, AggP95, AggP99} {
		if agg.Exprs[i].Func != want || agg.Exprs[i].Field != "duration" {
			t.Errorf("expr[%d] = %+v", i, agg.Exprs[i])
		}
	}
	sort := sq.Stages[1].(SortStage)
	if len(sort.Fields) != 1 || sort.Fields[0].Field != "p99_duration" || !sort.Fields[0].Desc {
		t.Errorf("sort = %+v", sort)
	}
	lim := sq.Stages[2].(LimitStage)
	if lim.N != 20 {
		t.Errorf("limit = %d, want 20", lim.N)
	}
}

func TestParserNumericComparisonOperators(t *testing.T) {
	tests := []struct {
		src  string
		want CmpOp
	}{
		{`{} | duration > 1s`, CmpGT},
		{`{} | duration >= 1s`, CmpGTE},
		{`{} | duration < 1s`, CmpLT},
		{`{} | duration <= 1s`, CmpLTE},
		{`{} | duration == 1s`, CmpEQ},
		{`{} | duration != 1s`, CmpNEQ},
	}
	for _, tt := range tests {
		sq := mustParse(t, tt.src).(SelectQuery)
		nf := sq.Stages[0].(NumericFilter)
		if nf.Op != tt.want {
			t.Errorf("%s: op = %v, want %v", tt.src, nf.Op, tt.want)
		}
	}
}

// Errors must point at the offending column, not just report "parse error".
func TestParserErrorMessagesPointAtColumn(t *testing.T) {
	tests := []struct {
		name    string
		src     string
		wantCol int
		wantMsg string
	}{
		{
			name:    "should point at closing brace when a label operator is missing",
			src:     `{service}`,
			wantCol: 9,
			wantMsg: "label operator",
		},
		{
			name:    "should point at bad comparison token",
			src:     `{} | duration ~ 500ms`,
			wantCol: 15,
			wantMsg: "comparison operator",
		},
		{
			name:    "should point at unterminated string",
			src:     `{service="checkout}`,
			wantCol: 10,
			wantMsg: "unterminated string",
		},
		{
			name:    "should point at trailing garbage after a complete query",
			src:     `{} extra`,
			wantCol: 4,
			wantMsg: "trailing input",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Parse(tt.src)
			if err == nil {
				t.Fatal("expected an error")
			}
			pe, ok := err.(*ParseError)
			if !ok {
				t.Fatalf("got %T, want *ParseError: %v", err, err)
			}
			if pe.Col != tt.wantCol {
				t.Errorf("col = %d, want %d (msg: %s)", pe.Col, tt.wantCol, pe.Msg)
			}
			if !strings.Contains(pe.Msg, tt.wantMsg) {
				t.Errorf("msg = %q, want substring %q", pe.Msg, tt.wantMsg)
			}
		})
	}
}

func TestParserDurationMissingUnitIsAnError(t *testing.T) {
	_, err := Parse(`{} since 5`)
	if err == nil {
		t.Fatal("expected error for a duration with no unit")
	}
	if !strings.Contains(err.Error(), "unit") {
		t.Errorf("error = %v, want mention of missing unit", err)
	}
}
