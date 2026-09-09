# The TraceLens query language

A small, hand-written DSL for querying distributed traces. No parser
generator, no embedded SQL, no off-the-shelf query library — the lexer,
recursive-descent parser, planner and optimiser in
[`internal/query`](../internal/query) are the substance of the project.

```
{service="checkout", status=error} | duration > 500ms | p95(duration) by (operation)
```

Read that left to right: pick a set of spans, narrow it, aggregate it.

---

## Grammar

```ebnf
query        = trace_query | select_query ;

trace_query  = "trace" "(" string ")" ;

select_query = selector [ time_range ] { "|" stage } ;

selector     = "{" [ matcher { "," matcher } ] "}" ;
matcher      = field ( "=" | "!=" | "=~" | "!~" ) value ;

time_range   = "since" duration
             | "range" "(" time_literal "," time_literal ")" ;
time_literal = rfc3339_string | "now" [ "-" duration ] ;

stage        = numeric_filter | aggregation | sort | limit ;

numeric_filter = field ( ">" | ">=" | "<" | "<=" | "==" | "!=" ) number ;

aggregation  = agg_expr { "," agg_expr } [ "by" "(" field { "," field } ")" ] ;
agg_expr     = "count"
             | ( "count" | "sum" | "avg" | "min" | "max"
               | "p50" | "p95" | "p99" ) "(" field ")" ;

sort         = "sort" "by" "(" sort_field { "," sort_field } ")" ;
sort_field   = field [ "asc" | "desc" ] ;

limit        = "limit" integer ;

value        = string | identifier ;
duration     = number unit { number unit } ;   (* no whitespace between parts *)
unit         = "ns" | "us" | "ms" | "s" | "m" | "h" ;
```

Keywords are case-insensitive and are **not** reserved: a span attribute
called `limit` or `count` still works as a selector field, because the parser
only treats those words as keywords in the positions where a stage can begin.

---

## Fields

Nine field names map to real columns. Anything else is looked up in the
span's attribute map, so `{http.status_code="500"}` needs no schema change.

| DSL field | Column | Type |
|---|---|---|
| `service` | `service_name` | string |
| `operation` | `span_name` | string |
| `status` | `status_code` | `unset` \| `ok` \| `error` |
| `kind` | `span_kind` | `internal` \| `server` \| `client` \| `producer` \| `consumer` |
| `duration` | `duration_ns` | duration |
| `trace_id` | `trace_id` | hex string |
| `span_id` | `span_id` | hex string |
| `parent_span_id` | `parent_span_id` | hex string |
| `sampling_weight` | `sampling_weight` | float |

Anything else → `span_attributes['<name>']`. A numeric comparison against an
attribute is cast at query time (`toFloat64OrNull`), since attribute values
are stored as strings.

---

## Selectors

The `{...}` block is the only mandatory part of a non-trace query. An empty
selector `{}` is legal and means "all spans in the time range".

| Operator | Meaning |
|---|---|
| `=` | equals |
| `!=` | not equals |
| `=~` | matches RE2 regex (**unanchored** — `=~"pay"` matches `payments`) |
| `!~` | does not match RE2 regex |

Values may be quoted or bare: `status=error` and `status="error"` are the
same query. Bare values are convenient for enums; anything else — spaces,
punctuation, or a value that starts with a digit — must be quoted, since only
strings and identifiers are accepted here.

These compile to ClickHouse `match()`, which is unanchored. Anchor it
yourself when you mean the whole value: `{service=~"^checkout$"}`.

```
{}                                            all spans, last hour
{service="checkout"}
{service=~"checkout|payments", status=error}
{service!="frontend", http.route="/cart"}
```

---

## Time range

Omit it and you get **the last hour**. That default is deliberate: an
unbounded scan over a spans table is the single easiest way to take the
cluster down, and a query language whose default is "everything, forever"
teaches people to write exactly that.

```
{service="api"} since 15m
{service="api"} since 1h30m
{service="api"} range("2026-09-01T00:00:00Z", "2026-09-01T06:00:00Z")
{service="api"} range(now-6h, now-1h)
```

Compound durations must not contain whitespace: `1h30m` is one token,
`1h 30m` is two and is a parse error. Durations always require a unit — a
bare `since 30` is rejected rather than guessed at.

---

## Filter stages

Numeric comparisons live after a pipe, not inside the selector, because they
are ordering comparisons rather than set membership.

```
{service="checkout"} | duration > 500ms
{service="checkout"} | duration >= 1s | duration < 5s
{} | http.status_code >= 500
```

A bare number is legal here and is taken in the **column's own native unit**,
which is what makes `http.status_code >= 500` work. For `duration` that
native unit is nanoseconds, so `duration > 500` is a valid query meaning
"longer than 500ns" — which matches essentially every span. Always write the
unit on a duration; the parser cannot tell a deliberate nanosecond bound from
a forgotten `ms`.

(The `since` / `range` clause is stricter and *does* reject a unitless
number, because there is no native unit for it to fall back on.)

---

## Aggregations

```
{service="checkout"} | count
{service="checkout"} | count by (operation)
{} | count, p95(duration) by (service, operation)
{} | avg(duration), max(duration) by (service)
```

`count` is the one function that may be written bare. Everything else takes a
field.

### Sampling changes what these mean

Every aggregate is weighted by `sampling_weight`. This is not a detail —
it is the difference between a correct answer and a plausible-looking wrong
one, and it is CLAUDE.md principle 6.

| DSL | Compiles to | Why |
|---|---|---|
| `count` | `sum(sampling_weight)` | A 1%-sampled trace stands for ~100 real ones. `count(*)` would report the sample size, not the population. |
| `sum(x)`, `avg(x)` | weighted sum / weighted mean | Same reasoning. |
| `p50`/`p95`/`p99` | `quantileTDigestWeighted(...)(duration_ns, toUInt64(round(sampling_weight)))` | A genuinely weighted quantile, not a quantile of the sample. |
| `min`, `max` | **unweighted** `min()` / `max()` | Deliberate. Sampling makes the true extreme less likely to have been *observed at all*; no weighting formula recovers a value you never saw. Treat these as "the most extreme sampled value", not "the most extreme value". |

The quantile weight must be an unsigned integer, so `sampling_weight` is
rounded. At the sampling rates this system is designed for the error is far
below the t-digest's own approximation error, but it is an approximation and
it is written down here rather than hidden.

---

## Sort and limit

```
{} | p95(duration) by (operation) | sort by (p95_duration desc) | limit 20
{service="api"} | duration > 1s | sort by (duration desc) | limit 100
```

Sort defaults to ascending. An aggregate's output column is named
`<func>_<field>` (`p95_duration`, `avg_duration`), or just `count` for a bare
count.

---

## Trace lookup

```
trace("4bf92f3577b34da6a3ce929d0e0e4736")
```

A different query shape entirely: it skips the planner and optimiser and goes
straight through the `trace_id` bloom-filter skip index, returning every span
of that trace with parent/child structure intact. This is what the UI's
waterfall view calls.

---

## EXPLAIN

Any query can be planned without running it:

```bash
curl -s localhost:8080/api/explain \
  -H 'content-type: application/json' \
  -d '{"query":"{service=\"checkout\"} since 1h | p95(duration) by (operation)"}'
```

The response carries five things:

1. **Logical plan** — `Scan → Filter → Project → Aggregate → Sort → Limit`
2. **Optimised plan** — after all five passes
3. **Compiled SQL** — parameterised, with `?` placeholders
4. **Bound arguments** — every literal, separately
5. **ClickHouse's own `EXPLAIN`** and `EXPLAIN ESTIMATE` row count, when a
   live connection is available

The UI's query explorer renders these side by side. Watching partition
pruning turn a full-table scan into three partitions is the fastest way to
see that the optimiser is real.

### The five passes

In fixed order, each independently tested (including the cases where it must
*refuse* to fire):

1. **Constant folding** — intersects redundant range predicates on one field,
   dedupes exact duplicates, folds a contradiction straight to "no rows".
2. **Predicate pushdown** — moves filters into the scan's `WHERE`. Never
   across an `Aggregate`, where the predicate may reference output that does
   not exist until the aggregate has run.
3. **Partition pruning** — rewrites the time bound into the half-open form
   (`timestamp >= ? AND timestamp < ?`) the partition key and sort key can
   actually use, never wrapped in a function that would defeat both.
4. **Limit pushdown** — pushes `LIMIT` into the scan as an early-exit hint,
   but never across `Sort` or `Aggregate`, where it would silently return
   wrong rows.
5. **Projection pushdown** — restricts the scan to the columns the query
   actually references, so attribute maps and event arrays are not
   decompressed for a query that never reads them. Runs last, because it
   needs the settled plan.

Measured effect of each pass is in [BENCHMARKS.md](BENCHMARKS.md).

---

## Safety

Two guards run on every query, both of them because an observability system
is one careless query away from being its own outage:

- **Preflight `EXPLAIN ESTIMATE`.** Before execution, ClickHouse is asked how
  many rows the query would read. Over `TRACELENS_QUERY_MAX_ROWS_SCANNED`
  (default 50M) it is rejected with the estimate, not run.
- **Hard wall-clock timeout**, `TRACELENS_QUERY_TIMEOUT` (default 30s).

And on the compilation path:

- **Identifiers come from a fixed allow-list.** A column name that is not in
  `allowedColumns` fails compilation rather than reaching SQL text.
- **Every value is a bound `?` parameter** — including attribute map keys.
  Nothing user-supplied is ever formatted into a SQL string. A test
  hand-builds a `ColumnRef` named `password; DROP TABLE spans; --` and
  asserts compilation refuses it.

---

## Errors

Parse errors carry line and column:

```
query:1:29: unexpected "error", expected a quoted string
query:1:12: "30" is missing a duration unit (ns, us, ms, s, m, h)
query:1:1: unexpected "|", expected a selector starting with "{"
```

`FuzzParser` has run several million executions against the lexer and parser
without a crash. Its job is crash-freedom only — that malformed input
produces an error rather than a panic. Semantic correctness is the
table-driven parser tests' job, because a fuzzer cannot tell a wrong parse
from a right one.
