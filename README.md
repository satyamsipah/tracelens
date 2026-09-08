# TraceLens

An OpenTelemetry-compatible observability platform: telemetry ingestion,
columnar storage, tail-based sampling, and a query engine for distributed
traces, logs and metrics.

**Current phase: ingestion, storage, tail-based sampling, the log pipeline,
a hand-written query engine, the service dependency graph, and anomaly
detection/alerting.** Only the UI is not implemented yet — see
[Roadmap](#roadmap).

---

## Architecture

```
Instrumented apps ──OTLP gRPC:4317 / HTTP:4318──▶ collector
                                                     │  bounded queue per signal
                                                     │  reject on saturation
                                                     ▼
                                         Redpanda (partitioned by trace_id)
                                          spans │ logs │ metrics
                                                     │
                                                     ▼
                                                 assembler
                             spans:  buffer by trace_id ▶ tail-sample ▶ insert
                             logs:   Drain templating ▶ cardinality guard ▶ insert
                                                     │
                                                     ▼
                                        ClickHouse (explicit codecs, TTL tiering)
                                          spans │ logs │ metrics │ metrics_rollup_1m
```

Everything exports Prometheus metrics on an admin port, scraped by Prometheus
and visualised in Grafana.

### The two decisions that shape everything else

**Spans are partitioned by `trace_id`, never round-robin.** The trace assembler
is stateful: it buffers in-flight spans keyed by trace, and consumer-group
members each own a subset of partitions. Split one trace across partitions and
each instance sees only a fragment — so the phase-2 tail sampler would drop a
failing trace because the instance holding it never saw the `ERROR` span, and
each fragment would carry its own sampling weight, double-counting every
aggregate. The producer treats a keyless span record as a hard error rather
than falling back to round-robin, because that failure is otherwise invisible.

**Saturation rejects rather than drops.** The bounded queue answers
`RESOURCE_EXHAUSTED` (gRPC) / `429 + Retry-After` (HTTP). Both are retryable
per the OTLP spec, so a conforming exporter backs off and resends: under
transient pressure nothing is lost, only deferred. Rejection is whole-request —
admitting a prefix would hand the assembler a truncated trace while the client
believes it delivered a complete one.

---

## Quickstart

Requires Docker and Go 1.25+.

```bash
make up
```

That builds every binary and image (including the Next.js UI), starts
ClickHouse, Redpanda, the collector, the assembler, the query API, the web
UI, the four demo services, Prometheus and Grafana, and waits for all health
checks. The demo gateway self-drives at 5 rps, so traces start flowing
immediately. Open `localhost:3001` — the query explorer's EXPLAIN toggle and
the service map are the fastest way to see what this project actually does.

Confirm telemetry is landing end to end:

```bash
make spans
```

Generate synthetic load at a chosen rate:

```bash
make loadgen RATE=5000 DURATION=30s
```

Measure what compression the codecs actually achieved:

```bash
make compression
```

Run every Part B benchmark (query optimiser at scale, storage/codec/batch-size,
sampler, Drain, correctness-under-load) and leave raw output under `bench/out/`:

```bash
make bench-all
```

Tear everything down:

```bash
make down
```

| Service | Address |
|---|---|
| Web UI | `localhost:3001` |
| Query API | `localhost:8080` (`/api/query`, `/api/explain`, `/api/trace/{id}`, `/api/services`, `/api/services/graph`, `/api/logs/templates`, `/api/health`) |
| OTLP gRPC | `localhost:4317` |
| OTLP HTTP | `localhost:4318` |
| Collector metrics | `localhost:9464/metrics` |
| Assembler metrics | `localhost:9465/metrics` |
| Query metrics | `localhost:9466/metrics` |
| ClickHouse | `localhost:9000` (native), `localhost:8123` (HTTP) |
| Prometheus | `localhost:9090` |
| Grafana | `localhost:3000` (anonymous admin) |

---

## Layout

| Path | What lives there |
|---|---|
| `cmd/collector` | OTLP receiver → bounded queue → Redpanda |
| `cmd/assembler` | Redpanda → decode → ClickHouse (tail sampling, service-graph edges land here) |
| `cmd/query` | Query engine HTTP API: execute, explain, trace-by-id, services, service graph; alert evaluator |
| `cmd/querybench` | Optimiser on/off benchmark harness (bytes read, latency) at real scale |
| `cmd/loadgen` | Synthetic span generator |
| `cmd/migrate` | Standalone schema migration runner |
| `internal/ingest` | OTLP transports, splitting, bounded queue, batching |
| `internal/pipeline` | Kafka producer/consumer, commit-gated offset tracking |
| `internal/sampling` | Trace buffer, policy chain, router, cardinality guard, service-edge extraction |
| `internal/logs` | Drain template extraction, template dictionary |
| `internal/query` | DSL lexer/parser/AST, logical plan, 5 optimiser passes, physical SQL compiler, executor, service graph, RED reader |
| `internal/anomaly` | Rolling seasonal z-score detector with hysteresis |
| `internal/alerting` | YAML rules, scheduled evaluation, webhook/Slack sinks, dedup, cooldown |
| `internal/storage` | ClickHouse schema, migrations, decoding, async writer |
| `internal/observability` | Prometheus registry, admin server, logging |
| `demo/` | Four instrumented services in one binary |
| `deploy/` | Compose stack, Dockerfile, ClickHouse, Prometheus and Grafana config |

---

## Schema

Three tables plus a rollup, every column with an explicit codec. Full DDL with
per-column reasoning is in [`internal/storage/migrations/`](internal/storage/migrations/).

The codec choices are not decoration — each is tied to the data's shape:

| Column kind | Codec | Why |
|---|---|---|
| `timestamp` | `DoubleDelta, ZSTD(1)` | Sorted within a granule, so deltas are tiny |
| `trace_id` | `ZSTD(1)` | CSPRNG output, but repeats across one trace's spans within a batch — measured 1.23× |
| `span_id` | `NONE` | CSPRNG output with **no** cross-row repetition — ZSTD measured *inflating* it (0.9995×), so compression is off entirely |
| `duration_ns` | `T64, ZSTD(1)` | Not monotonic, so Delta is noise; T64 crops provably-unused high bits |
| `service_name`, `span_name` | `LowCardinality + ZSTD(1)` | Dictionary-encoded, leading sort key |
| `span_kind`, `status_code` | `Enum8, ZSTD(1)` | Closed sets; 1 byte and garbage is rejected at insert |
| metric `value` | `Gorilla, ZSTD(1)` | XOR against predecessor — only works because `labels_hash` precedes `timestamp` in the sort key |

Insert-path compression is deliberately `ZSTD(1)` because insert CPU *is*
ingestion throughput; a `TTL … RECOMPRESS CODEC(ZSTD(6))` lifts cold parts
later, so both cheap inserts and dense cold storage are possible.

A test asserts that **no column lacks a codec** — a column added later without
one fails CI rather than review.

**Measured:** 905,166 spans from live demo + loadgen traffic compress
143.72 MiB → 35.97 MiB, a **4.0× ratio** (`make compression`). An
independent schema review against this same live data found and fixed two
issues: an index that pruned zero granules, and a codec that was quietly
*inflating* its column — see [docs/DECISIONS.md §3a](docs/DECISIONS.md).

---

## Tail sampling

Spans arrive out of order and never all at once, so the assembler buffers
in-flight spans per `trace_id` and decides, later, what to keep.

**Completion heuristic: fixed wait + root-closure early exit.** Every trace
gets `TRACELENS_DECISION_WAIT` (default 5s) from first-seen before a periodic
sweep (`RunSweep`) forces a decision — but if the root span has already
arrived and every buffered span's interval falls inside
`[root.start, root.end]`, the trace decides immediately rather than waiting
out the timer. A quiet-period heuristic (reset the timer on every new span)
and a pure root-seen+grace heuristic were considered and rejected — see
[docs/DECISIONS.md](docs/DECISIONS.md) for why a fixed ceiling with an early
exit was chosen over both.

**Buffer eviction: forced early decision, not discard.** The in-flight
buffer is bounded by both trace count (`TRACELENS_BUFFER_MAX_TRACES`, default
50,000) and bytes (`TRACELENS_BUFFER_MAX_BYTES`, default 256MiB). At
capacity, the oldest trace is forced through the policy chain immediately —
on whatever spans it has so far — rather than silently discarded, because a
discarded trace under load is indistinguishable from a trace that was never
sampled, while a forced decision is at least visible as
`tracelens_forced_decisions_total`.

**Policy chain — composable, ordered, hot-reloadable.** Policies evaluate in
order; the first that doesn't abstain wins (`internal/sampling/policy.go`).
The deployed chain ([`deploy/tracelens/policies.yaml`](deploy/tracelens/policies.yaml)):

1. `always_sample_errors` — any error span, kept with certainty
2. `always_sample_slow` — latency over a threshold (or above a rolling p99),
   kept with certainty
3. `attribute_match` — an explicit debug flag, kept with certainty,
   **before** the rate cap so it truly bypasses it
4. `rate_limiting` — a per-service token-bucket ceiling; **abstains** while
   capacity remains (deferring to probabilistic below) and only vetoes once
   exhausted — this composability fix mattered: an earlier version always
   resolved with a smoothed weight, which silently made `probabilistic` and
   `attribute_match` dead code (see DECISIONS.md)
5. `probabilistic` — the baseline, a fixed rate sampled deterministically by
   `trace_id`; must be last, since an empty/no-match chain drops rather than
   silently keeping everything

The assembler polls the policy file's mtime every
`TRACELENS_RELOAD_INTERVAL` (default 5s) and swaps the live chain on a valid
change; a malformed edit is logged and the previous chain stays active.

**Sampling weight.** Every kept trace carries `Decision.Weight() =
1/Probability`, so a downstream aggregate can recover the true population
rate rather than just the sampled count — verified by
`TestUpweightingRecoversTruePopulation`, which generates a known population,
samples it, and checks the weighted aggregate against ground truth (within
~5% at a 1% sampling rate over 100k traces).

**Routing.** Spans are partitioned by `trace_id`, and `internal/sampling/router.go`
implements genuine consistent hashing (rendezvous / HRW, not `hash%N`) for a
future multi-assembler topology — resize adds/removes only ~20% of key
assignments versus 75%+ for naive modulo sharding. It is not yet wired into
the live consume path; partitioning today comes from the Kafka consumer
group itself. `TestConsumerNoTraceSplitAcrossThreeInstances` runs three real
consumer instances against a live Redpanda topic and asserts no trace_id is
ever observed by more than one instance.

**Late spans.** A span for an already-decided trace is attached to storage
if that trace was sampled (carrying the original decision's weight), and
dropped with `tracelens_late_spans_total{outcome="dropped"}` otherwise. A
small decided-trace cache (bounded, LRU-evicted) makes this possible without
re-buffering.

**Commit safety.** `internal/sampling/watermark.go` tracks, per partition,
the earliest offset of any trace not yet durably decided, and the consumer
(`RunWithCommitGate`) never commits past it — so a crash mid-decision
replays exactly the undecided traces, and nothing already flushed is
re-processed.

---

## Log pipeline

**Template extraction: Drain, implemented from scratch** (`internal/logs/drain.go`,
no external library). A fixed-depth tree groups log lines first by token
count, then by leading tokens; numeric-looking tokens are wildcarded eagerly
during descent (`user_id=482913` and `user_id=17` reach the same leaf); the
leaf does a position-wise similarity match against existing clusters and
only ever widens a template, never narrows it. `Config{Depth,
SimilarityThreshold, MaxChildren, MaxTemplates, MaxClustersPerLeaf}` are all
tunable (`TRACELENS_DRAIN_*`).

Only `template_id` + extracted `params` are stored per log line; the
rendered text lives once in a `log_templates` dictionary table, upserted
only when a template is newly created or widened. Measured on a 10,000-line
corpus: raw bodies 783,425 bytes vs. templated 283,408 bytes — a **2.76×**
storage reduction (`TestTemplateStoreMeasuresStorageSaving`). The number of
distinct templates is bounded both tree-wide and per-leaf
(`MaxClustersPerLeaf`, default 200 in production), LRU-evicting the
coldest template rather than growing unbounded — a load test found a 138×
slowdown with no per-leaf cap, resolved to 6.5× after (see DECISIONS.md).

**Cardinality control** uses a HyperLogLog per attribute key
(`internal/sampling/hyperloglog.go`) — an approximate sketch, deliberately
not an exact set, so tracking cardinality never itself becomes an unbounded
memory cost. Each key has a configurable budget
([`deploy/tracelens/cardinality.yaml`](deploy/tracelens/cardinality.yaml))
and a breach action:

- **`drop`** — strip the attribute entirely once its key exceeds budget
- **`bucket`** *(default)* — hash the value into a fixed number of buckets,
  keeping the attribute queryable at reduced granularity instead of losing
  it
- **`keep_and_alert`** — keep the value as-is but count the breach, for keys
  where cardinality is a symptom to investigate rather than a cost to cap

Every breach increments `tracelens_cardinality_breaches_total{key}`, and the
live estimate is exported as `tracelens_cardinality_estimate{key}`, together
forming the "top offenders" panel in Grafana.

**Correlation and retention.** Logs carry `trace_id`/`span_id` when present,
joinable against `spans` in the query engine. Retention is TTL'd by
severity, not a single flat window: TRACE/DEBUG for 1 day, INFO/WARN for 14
days, ERROR/FATAL for 90 days (`internal/storage/migrations/0006_log_templates.up.sql`).

---

## Query engine

A hand-written DSL, lexer, parser, logical planner, five optimiser passes,
and a physical compiler to parameterised ClickHouse SQL — no off-the-shelf
query language library anywhere in `internal/query`.

```
{service="checkout", status=error} | duration > 500ms | count by (operation)
```

Label selectors (`=`, `!=`, `=~`, `!~`), numeric filters with duration units
(`500ms`, `1h30m`), a time range (`since 1h`, `range(...)`, defaulting to the
last hour when omitted), aggregations (`count`, `sum`, `avg`, `min`, `max`,
`p50`/`p95`/`p99`, several in one query), `sort by`, `limit`, and a dedicated
`trace("<id>")` point-lookup form that bypasses the planner entirely and
goes straight through the `trace_id` bloom index. Every parse error points
at the exact line and column (`query:1:29: unexpected "error", expected...`),
and `FuzzParser` has run millions of executions against the lexer/parser
with zero crashes — its job is crash-freedom, not semantic correctness,
which the table-driven parser tests cover instead.

**Sampling-aware aggregation, concretely (CLAUDE.md principle 6):**
`count` compiles to `sum(sampling_weight)`, never `count(*)`; `sum`/`avg`
weight every value the same way; `p50`/`p95`/`p99` use ClickHouse's
`quantileTDigestWeighted`, which is a genuinely weighted quantile, not an
approximation of one (its weight argument must be an unsigned integer, so
`sampling_weight` is rounded — `toUInt64(round(sampling_weight))` — a small,
documented, accepted approximation). `min`/`max` are deliberately **not**
reweighted: sampling makes the true population extreme less likely to have
been observed at all, and no weighting formula fixes that.

**Logical plan:** `Scan -> Filter -> Project -> [Aggregate] -> [Sort] ->
[Limit]`, always in that fixed shape. **Five optimiser passes**, in this
order, each independently testable and each with a dedicated ablation test
proving it changes the generated SQL (`internal/query/ablation_test.go`):

1. **Constant folding + predicate simplification** — tightens redundant
   range comparisons on the same field to their intersection, dedupes exact
   duplicates, and folds a contradictory or impossible range straight to
   "no rows" rather than scanning for an outcome that's already known.
2. **Predicate pushdown into the scan** — moves a filter into the scan's own
   `WHERE`. Cannot push a predicate that sits above an `Aggregate` (it may
   reference the aggregate's *output*, which doesn't exist until the
   aggregate runs) — that boundary is enforced structurally, not just by
   convention, and is unit-tested by hand-building exactly that plan shape.
3. **Partition pruning from the time range** — rewrites the bound into the
   exact half-open form (`timestamp >= start AND timestamp < end`) the
   partition index and sort key can use, never wrapped in a function that
   would defeat both.
4. **Limit pushdown, never across an aggregate** — lets the scan itself
   carry `LIMIT N` (an early-exit hint) when nothing between `Limit` and
   `Scan` changes which rows count toward "first N"; refuses to cross
   `Sort` or `Aggregate`, where it would silently corrupt results.
5. **Projection pushdown** — restricts the scan to exactly the columns an
   aggregate query's filter, group-by and aggregation expressions actually
   reference, so `span_attributes`, event arrays and link arrays are never
   even decompressed for a query that never looks at them. Runs last,
   since it needs the fully settled plan to compute that closure. A raw
   (non-aggregate) span listing is left unrestricted — there's no explicit
   column list in the DSL to narrow to.

The physical compiler is bottom-up and compositional: a node wraps its
input in a subquery only when it carries a genuinely unpushed
transformation, so disabling one pass produces textually different SQL
(a wrapping `WHERE`, a bare `SELECT *`, a missing inner `LIMIT`) rather than
the compiler quietly absorbing the difference regardless — verified
directly by a dedicated ablation test per pass before any benchmark ever
ran on the result.

**Measured on 10.3M real rows** (`make querybench`): partition pruning is
the standout — **13.0× fewer bytes read, a 5.2× latency improvement**
(1.965s → 377ms) for a query scoped to the last hour of a week-old table.
Limit pushdown cuts latency 2.1× via early scan termination even where
bytes read barely move. Not every pass shows a large win, and the full
table in [docs/BENCHMARKS.md](docs/BENCHMARKS.md) says exactly why for each
one — including a case where predicate pushdown's real, structural SQL
effect didn't translate to a latency win at this specific scale, reported
as measured rather than rounded into a cleaner story.

**`EXPLAIN`** returns the logical plan, the optimised plan, the compiled SQL
and bound arguments, and — against a live connection — ClickHouse's own
`EXPLAIN` output and `EXPLAIN ESTIMATE` row count. **Every query** goes
through a preflight `EXPLAIN ESTIMATE` checked against a configurable
max-rows-scanned guard *before* it runs, plus a hard wall-clock timeout —
an unbounded query against a multi-billion-row table is a self-inflicted
denial of service, not just a slow request.

**Every identifier is validated, every value is bound.** Column names come
only from a fixed allow-list; attribute keys and every literal travel as
`?` parameters, never string-formatted into SQL — checked directly by a
test that hand-builds a `ColumnRef` containing `"password; DROP TABLE
spans; --"` and asserts compilation refuses it.

**API** (`cmd/query`, port `8080`): `POST /api/query`, `POST /api/explain`,
`GET /api/trace/{id}`, `GET /api/services`, `GET /api/services/graph?window=1h`.

---

## Service graph, anomaly detection and alerting

**The service dependency graph is computed incrementally, not with a
per-request scan or a ClickHouse-side join.** The join a graph needs — a
child span to its *parent* span, to know who called whom — can't be a
ClickHouse materialized view: an MV fires per insert block, and a trace's
parent and child spans aren't guaranteed to land in the same one. The
assembler already builds the full parent-child tree in memory to make the
tail-sampling decision, so `internal/sampling.ExtractServiceEdges` does the
join right there, once per decided trace, emitting one pre-joined
caller→callee row per cross-service call (same-service parent-child calls
are internal, not graph edges). ClickHouse's job is then only the
genuinely incremental part: an `AggregatingMergeTree` rolls those rows up
by minute (`tracelens.service_edges`), and the graph endpoint reads that —
never raw spans.

Cycle detection (a real call graph should be a DAG; a cycle is either a
genuine circular dependency or a tracing bug) and a per-service
criticality score (the fraction of total call volume flowing *into* that
service — "how much of everything depends on this being up") are computed
in Go over the small aggregated edge set, alongside articulation-point
detection (services whose removal would disconnect the graph — a
structural single-point-of-failure signal, independent of volume).

**RED metrics are pre-aggregated at 1m/5m/1h**, each level a rollup of the
level below (`spans -> red_rollup_1m -> red_rollup_5m -> red_rollup_1h`, via
`quantilesTDigestWeightedMergeState` re-aggregating an already-aggregated
state rather than re-scanning raw spans three times). The storage-vs-query
trade-off: finer rollups are precise but expensive to keep long (1m: 7-day
TTL); coarser rollups are cheap to keep for a year (1h: 400-day TTL) at the
cost of minute-level resolution — see docs/DECISIONS.md for the full
reasoning.

**Anomaly detection** (`internal/anomaly`) is a rolling seasonal z-score:
a bounded sliding-window median/MAD baseline per (service, operation,
hour-of-day, day-of-week) bucket — genuine order statistics from a fixed
32-sample ring buffer, not an EWMA approximation of them — with hysteresis
(trigger at 3σ, clear at 1.5σ, two consecutive ticks either direction, so
one noisy point can't flap an alert). Evaluated against injected synthetic
anomalies on a known seasonal population: **precision 0.806, recall
0.833** (`internal/anomaly/detector_test.go`). An STL-decomposition
alternative was considered and rejected for now — see DECISIONS.md — since
it's a batch/windowed fit that doesn't update incrementally the way every
other piece of state in this codebase does.

**Alerting** (`internal/alerting`) evaluates YAML rules
([`deploy/tracelens/alerts.yaml`](deploy/tracelens/alerts.yaml)) on a
schedule, notifying a webhook or Slack incoming-webhook only on a firing
*state transition* (dedup — a rule that stays firing for an hour notifies
once, not every tick) and never more often than its configured cooldown,
independent of how fast the underlying condition flaps. A rule is either
`anomaly`-typed (rides the detector above) or `threshold`-typed (a fixed
SLO number, for when "acceptable" is a contract, not a baseline).

**Self-monitoring:** TraceLens ingests its own telemetry. The Grafana
dashboard at [`deploy/grafana/dashboards/self-monitoring.json`](deploy/grafana/dashboards/self-monitoring.json)
covers ingestion rate, queue depths, drops, sampler memory, consumer lag,
storage size and compression ratio — the last two read live from
`system.parts` via a second (ClickHouse) Grafana datasource, since neither
has a Prometheus metric behind it. Consumer lag is a wall-clock proxy
(`time.Since(record.Timestamp)` at consume time), not an offset-based one —
a documented, accepted simplification, not a hidden gap.

---

## UI

Next.js 14 (App Router) + TypeScript + Tailwind, in [`web/`](web/) — see
[`web/README.md`](web/README.md) for the full view-by-view breakdown. It
talks only to `cmd/query`'s HTTP API, never ClickHouse directly.

- **Trace waterfall** (`/traces/[id]`) — nested bars on a shared time axis
  (d3-scale), critical path highlighted, virtualised so a 500+ span trace
  only ever mounts the rows scrolled into view, with a live FPS counter
  while scrolling rather than an assumption that virtualising was enough.
- **Flamegraph** (`/flamegraph`) — merges the call trees of up to 20 recent
  traces containing a chosen operation into one aggregated icicle chart,
  click-to-zoom with breadcrumb navigation.
- **Service map** (`/services`) — d3-force directed graph over the same
  `service_edges` rollup the API's graph endpoint reads: node size =
  traffic, edge colour = error rate, click an edge for its latency
  distribution.
- **Query explorer** (`/query`) — the DSL with syntax highlighting
  (a hand-rolled CodeMirror `StreamLanguage`, matching `internal/query`'s
  own lexer token classes) and autocomplete on known services/operations,
  results as a table or bar chart, and an **EXPLAIN toggle** rendering the
  logical plan, optimised plan, compiled SQL, and ClickHouse's own EXPLAIN
  side by side.
- **Log explorer** (`/logs`) — grouped by Drain template, expandable to
  instances, jump-to-trace link when a log carries a trace_id.
- **System health** (`/health`) — the same ingestion/queue/sampler/storage
  numbers as the self-monitoring Grafana dashboard, read from the same
  Prometheus/ClickHouse sources via `cmd/query`'s `/api/health` (which
  proxies a fixed set of Prometheus instant queries server-side, since
  Prometheus sets no CORS headers for a browser to call it directly).

**A real bug found and fixed while wiring this up, worth recording because
it is a general trap, not a one-off:** `internal/query`'s generic row
scanner returned Go's zero-value nil slice for an empty result set, and
`encoding/json` marshals `nil` to `null`, not `[]` — a client iterating
`result.Rows` on any zero-row query (a completely ordinary case, e.g. an
aggregation over a narrow time window with no matching data) crashed
outright. Fixed at the source (`internal/query/executor.go`,
`servicegraph.go`, `logs.go`): every slice-typed API field is now
initialized non-nil, so "no results" is `[]`, not `null`, everywhere.

---

## Testing

```bash
make test        # unit tests only, no containers
make test-race   # full suite under -race, with real ClickHouse and Redpanda
make bench       # hot-path benchmarks with allocs/op
```

Storage and broker behaviour is tested against real containers, never mocks —
including that a replayed Kafka batch is a no-op, that duplicates collapse
under `FINAL`, and that out-of-order timestamps lose nothing. The
Testcontainers ClickHouse mounts the *production* `storage.xml`, so the schema
under test is byte-identical to the deployed one. `internal/query` and
`cmd/query` follow the identical pattern — every DSL query, EXPLAIN, trace
lookup, and service-graph test runs against a real, migrated ClickHouse,
never a stand-in.

```bash
make querybench  # optimiser on/off benchmark at real scale (needs `make up` first)
```

### Hot path

Ingest allocations are constant per request regardless of how many traces a
batch carries:

| Benchmark | ns/op | B/op | allocs/op |
|---|---|---|---|
| `SplitTraces` 1 trace × 10 spans | 2,129 | 24 | 1 |
| `SplitTraces` 10 traces × 10 spans | 21,150 | 24 | 1 |
| `SplitTraces` 50 traces × 4 spans | 53,573 | 25 | 1 |
| `EnqueueBatch` | 219 | 0 | 0 |
| `SplitAndEnqueue` (full receiver path) | 23,715 | 28 | 1 |

*(Apple M1. See [docs/DECISIONS.md](docs/DECISIONS.md) for the before/after
that produced these.)*

### Headline benchmark numbers (Part B)

Full methodology, every table, and the honest caveats (what ran under
contention, what didn't reach its target scale, and why) are in
[docs/BENCHMARKS.md](docs/BENCHMARKS.md). One command reproduces all of it
against a running stack (`make up` first):

```bash
make bench-all   # ROWS=100000000 ITERS=5 to push further than the default
```

| Result | Number | Source |
|---|---|---|
| Partition pruning (1h of 7d), bytes read | **19.3× fewer** | `make querybench`, ~25M rows |
| Partition pruning (1h of 7d), latency | **5.1× faster** | `make querybench`, ~25M rows |
| ClickHouse insert throughput (200k row batches) | **1.78M rows/sec** | `make storagebench` |
| Timestamp codec (DoubleDelta vs none) | **~230× smaller** | `make storagebench` |
| Log template throughput (steady state) | **~897,000 lines/sec** | `go test ./internal/logs/... -bench .` |
| Log storage saving (template_id+params vs raw body) | **2.76×** | `internal/logs/store_test.go` |
| Anomaly detector precision / recall | **0.806 / 0.833** | `internal/anomaly/detector_test.go` |
| Error retention under sampling | **100%** (200/200), non-errors at 5.0% vs 5% target | `internal/sampling` Phase 2 tests |
| Buffer memory per in-flight trace (accounted, floor) | **98 bytes** | `internal/sampling/loadbench_test.go` |

Two things this table deliberately does **not** claim, because they were
measured and turned out not to hold up: predicate pushdown and limit
pushdown's specific numbers at the ~25M-row scale (both ran concurrently
with a background reseed and showed the pass making queries *slower* —
structurally impossible, flagged as contention noise, not reported as a
result); and a full 100M-row measurement (seeding reached ~25-30M inside
this session's time budget before this table was written — the seed
process for `-rows 100000000` sustains roughly 12-17k rows/sec, making
100M a multi-hour operation. Re-run `make querybench ROWS=100000000` on
an idle stack to complete it).

---

## Configuration

Everything is environment-driven; defaults make `docker compose up` work with
nothing set. The knobs that matter:

| Variable | Default | Meaning |
|---|---|---|
| `TRACELENS_QUEUE_CAPACITY` | `8192` | Hard cap of the bounded queue, per signal |
| `TRACELENS_QUEUE_HIGH_WATER_RATIO` | `0.8` | Fraction of capacity at which rejection begins |
| `TRACELENS_BACKPRESSURE_{TRACES,LOGS,METRICS}` | `reject` | `reject` or `drop_oldest` |
| `TRACELENS_BATCH_MAX_RECORDS` | `512` | Flush trigger — records |
| `TRACELENS_BATCH_MAX_BYTES` | `4MiB` | Flush trigger — bytes |
| `TRACELENS_BATCH_FLUSH_INTERVAL` | `200ms` | Flush trigger — time |
| `TRACELENS_KAFKA_PARTITIONS` | `12` | **Fixed by design** — changing it rehashes every key |
| `TRACELENS_CLICKHOUSE_BATCH_SIZE` | `10000` | Rows per INSERT |
| `TRACELENS_POLICY_FILE` | `/etc/tracelens/policies.yaml` | Tail-sampling policy chain, hot-reloaded |
| `TRACELENS_CARDINALITY_FILE` | `/etc/tracelens/cardinality.yaml` | Per-key cardinality budgets and actions |
| `TRACELENS_RELOAD_INTERVAL` | `5s` | Policy-file mtime poll interval |
| `TRACELENS_DECISION_WAIT` | `5s` | Fixed-wait ceiling before a trace is force-decided |
| `TRACELENS_BUFFER_MAX_TRACES` | `50000` | In-flight buffer cap, trace count |
| `TRACELENS_BUFFER_MAX_BYTES` | `256MiB` | In-flight buffer cap, bytes |
| `TRACELENS_BUFFER_EVICTION` | `forced_decision` | `forced_decision` or `discard` at capacity |
| `TRACELENS_DRAIN_DEPTH` / `_SIMILARITY` / `_MAX_CHILDREN` / `_MAX_TEMPLATES` | see `internal/config` | Drain tree shape and template caps |
| `TRACELENS_QUERY_HTTP_ADDR` | `:8080` | Query API listen address |
| `TRACELENS_QUERY_TIMEOUT` | `30s` | Per-query hard wall-clock bound |
| `TRACELENS_QUERY_MAX_ROWS_SCANNED` | `50000000` | Preflight `EXPLAIN ESTIMATE` guard |
| `TRACELENS_ALERT_RULES_FILE` | `/etc/tracelens/alerts.yaml` | Alerting rules; missing/invalid disables alerting, not startup |
| `TRACELENS_ALERT_EVAL_INTERVAL` | `60s` | How often every alert rule is evaluated |

### Metrics worth watching

- `otlp_spans_dropped_total{reason}` — `rejected`, `queue_full`, `decode_error`, `cardinality_budget`, `produce_failed`
- `ingest_queue_depth{signal}` vs `ingest_queue_capacity{signal}`
- `clickhouse_rows_inserted_total{table}`, `clickhouse_insert_retries_total{table}`
- `tracelens_inflight_traces`, `tracelens_inflight_bytes`, `tracelens_forced_decisions_total`, `tracelens_evicted_traces_total`
- `tracelens_decisions_total{outcome,policy}`, `tracelens_late_spans_total{outcome}`
- `tracelens_cardinality_breaches_total{key}`, `tracelens_cardinality_estimate{key}`
- `tracelens_templates_created_total`, `tracelens_templates_evicted_total`, `tracelens_templates_total`
- `tracelens_consumer_lag_seconds{topic}` — a wall-clock proxy, not offset-based

---

## Known gaps

These are deliberate and recorded in [docs/DECISIONS.md](docs/DECISIONS.md):

- **Histogram buckets are not stored.** The schema's single `Float64 value`
  cannot hold bucket bounds; histograms decompose into `_count` and `_sum`
  series, so quantile queries over histograms are not answerable yet.
- **Attribute values are flattened to `String`.** Resolved for querying:
  the query engine casts a numeric predicate over an attribute
  (`toFloat64OrNull(...)`) automatically; the underlying storage
  representation is unchanged.
- **OTLP/JSON is not implemented** (protobuf only — JSON is optional in the
  spec). A JSON request gets `415` rather than a silent misparse.
- **The `spans` ORDER BY costs ~4.2× read amplification on a service+time
  query with no `span_name` filter** (measured via `EXPLAIN ESTIMATE`).
  Swapping `span_name` and `timestamp` in the key would move the identical
  cost onto service-map queries instead — left as-is pending a real query-mix
  measurement to decide which pattern is higher QPS.
- **Consistent-hash routing is implemented but not wired in** — `internal/sampling/router.go`
  is ready for a multi-assembler topology, but today the Kafka consumer
  group is the only thing partitioning trace ownership across instances.
- **`template_id` allocation is process-scoped**, not shared across
  assembler instances or restarts — a restart or a second instance can
  assign different IDs to the same template shape. Acceptable for now since
  `log_templates` is a `ReplacingMergeTree` keyed by `template_id` within
  one process's lifetime; a shared allocator is future work if templating
  needs to survive restarts with stable IDs.
- **Critical-path computation is a simplification** (follows the child with
  the latest end-time at each level), not full gap-accounting critical-path
  analysis.
- **A commit-floor watermark can get stuck indefinitely if the trace
  holding it never resolves**, and once stuck long enough for broker
  retention to age out that offset, every consumer restart hits a
  permanent "offset no longer exists" loop — found live while running the
  Part B correctness-under-load proof, not by inspection. This is the
  single most important open item in the project right now; see
  [docs/DECISIONS.md](docs/DECISIONS.md)'s Phase 4 §8 for the full
  evidence and the reasoning for leaving it open rather than patching a
  principle-1-critical mechanism under time pressure.

## Roadmap

1. ~~OTLP ingestion, ClickHouse schema, demo workload~~ — done
2. ~~Trace assembly + tail sampling (bounded buffer, eviction policy, sampling weights)~~ — done
3. ~~Log pipeline (Drain templating, cardinality control, per-severity retention)~~ — done
4. ~~Query engine: DSL → AST → logical plan → physical plan~~ — done
5. ~~Service graph, RED rollups, anomaly detection, alerting~~ — done
6. ~~UI: trace waterfall, flamegraph, service map, log explorer, query explorer, system health~~ — done
7. ~~Load testing and benchmarks (Part B)~~ — done, with two open items: the
   100M-row query-engine scale target (reached ~25-30M in-session; see
   [docs/BENCHMARKS.md](docs/BENCHMARKS.md)) and the stuck-commit-floor bug
   below
