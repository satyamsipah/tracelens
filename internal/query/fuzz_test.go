package query

import "testing"

// FuzzParser's job is crash-freedom, not semantic correctness -- that is
// what the table-driven parser tests are for. Any input either parses or
// returns a *ParseError; a panic, an infinite loop, or an unbounded
// allocation from malformed input is the only thing this target exists to
// catch.
func FuzzParser(f *testing.F) {
	seeds := []string{
		``,
		`{}`,
		`{service="checkout", status=error} | duration > 500ms | count by (operation)`,
		`trace("abcd1234")`,
		`trace("")`,
		`{} since 1h30m`,
		`{} range("2026-01-01T00:00:00Z", now)`,
		`{} range(now-2h, now-1h)`,
		`{a=~"b.*"} | p99(duration) by (operation) | sort by (p99_duration desc) | limit 10`,
		`{`,
		`}`,
		`{{{{`,
		`{service="a"`,
		`{service=}`,
		`"unterminated`,
		`{} | limit -1`,
		`{} | limit 99999999999999999999999999`,
		`{} | duration > `,
		`trace(`,
		string([]byte{0x00, 0x01, 0x02}),
		`{"": ""}`,
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, src string) {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("parser panicked on input %q: %v", src, r)
			}
		}()
		_, _ = NewParser(src, fixedNow).Parse()
	})
}
