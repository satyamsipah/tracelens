package query

import "testing"

// Every DSL example printed in docs/QUERY-LANGUAGE.md, parsed. A reference
// whose examples do not parse is worse than no reference.
func TestDocExamplesParse(t *testing.T) {
	valid := []string{
		`{service="checkout", status=error} | duration > 500ms | p95(duration) by (operation)`,
		`{}`,
		`{service="checkout"}`,
		`{service=~"checkout|payments", status=error}`,
		`{service!="frontend", http.route="/cart"}`,
		`{service=~"^checkout$"}`,
		`{service="api"} since 15m`,
		`{service="api"} since 1h30m`,
		`{service="api"} range("2026-09-01T00:00:00Z", "2026-09-01T06:00:00Z")`,
		`{service="api"} range(now-6h, now-1h)`,
		`{service="checkout"} | duration > 500ms`,
		`{service="checkout"} | duration >= 1s | duration < 5s`,
		`{} | http.status_code >= 500`,
		`{service="checkout"} | count`,
		`{service="checkout"} | count by (operation)`,
		`{} | count, p95(duration) by (service, operation)`,
		`{} | avg(duration), max(duration) by (service)`,
		`{} | p95(duration) by (operation) | sort by (p95_duration desc) | limit 20`,
		`{service="api"} | duration > 1s | sort by (duration desc) | limit 100`,
		`trace("4bf92f3577b34da6a3ce929d0e0e4736")`,
	}
	for _, src := range valid {
		if _, err := Parse(src); err != nil {
			t.Errorf("documented example should parse\n  query: %s\n  error: %v", src, err)
		}
	}

	// A bare number in a FILTER stage is legal and means the column's native
	// unit -- nanoseconds for duration. Documented as a sharp edge rather
	// than silently "fixed" here, since it is what makes
	// `http.status_code >= 500` work.
	if _, err := Parse(`{service="checkout"} | duration > 500`); err != nil {
		t.Errorf("a unitless filter bound should parse: %v", err)
	}

	// The rejections the reference promises.
	invalid := []string{
		`{service="api"} since 30`,     // no duration unit
		`{service="api"} since 1h 30m`, // whitespace splits a compound duration
	}
	for _, src := range invalid {
		if _, err := Parse(src); err == nil {
			t.Errorf("documented rejection should fail to parse: %s", src)
		}
	}
}
