# TraceLens

An OpenTelemetry-compatible observability platform: telemetry ingestion,
columnar storage, tail-based sampling, and a query engine for distributed
traces, logs and metrics.

**Current phase: ingestion, storage, tail-based sampling, and the log
pipeline.** The query engine and UI are not implemented yet — see
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

That builds every binary, starts ClickHouse, Redpanda, the collector, the
assembler, the four demo services, Prometheus and Grafana, and waits for all
health checks. The demo gateway self-drives at 5 rps, so traces start flowing
immediately.

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

Tear everything down:

```bash
make down
```

| Service | Address |
|---|---|
| OTLP gRPC | `localhost:4317` |
| OTLP HTTP | `localhost:4318` |
| Collector metrics | `localhost:9464/metrics` |
| Assembler metrics | `localhost:9465/metrics` |
| ClickHouse | `localhost:9000` (native), `localhost:8123` (HTTP) |
| Prometheus | `localhost:9090` |
| Grafana | `localhost:3000` (anonymous admin) |

---

## Layout

| Path | What lives there |
|---|---|
| `cmd/collector` | OTLP receiver → bounded queue → Redpanda |
| `cmd/assembler` | Redpanda → decode → ClickHouse (tail sampling lands here) |
| `cmd/query` | Query engine (stub — phase 3) |
| `cmd/loadgen` | Synthetic span generator |
| `cmd/migrate` | Standalone schema migration runner |
| `internal/ingest` | OTLP transports, splitting, bounded queue, batching |
| `internal/pipeline` | Kafka producer/consumer, commit-gated offset tracking |
| `internal/sampling` | Trace buffer, policy chain, router, cardinality guard |
| `internal/logs` | Drain template extraction, template dictionary |
| `internal/storage` | ClickHouse schema, migrations, decoding, async writer |
| `internal/observability` | Prometheus registry, admin server, logging |
| `demo/` | Four instrumented services in one binary |
| `deploy/` | Compose stack, Dockerfile, ClickHouse and Prometheus config |

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
under test is byte-identical to the deployed one.

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

### Metrics worth watching

- `otlp_spans_dropped_total{reason}` — `rejected`, `queue_full`, `decode_error`, `cardinality_budget`, `produce_failed`
- `ingest_queue_depth{signal}` vs `ingest_queue_capacity{signal}`
- `clickhouse_rows_inserted_total{table}`, `clickhouse_insert_retries_total{table}`
- `tracelens_inflight_traces`, `tracelens_inflight_bytes`, `tracelens_forced_decisions_total`, `tracelens_evicted_traces_total`
- `tracelens_decisions_total{outcome,policy}`, `tracelens_late_spans_total{outcome}`
- `tracelens_cardinality_breaches_total{key}`, `tracelens_cardinality_estimate{key}`
- `tracelens_templates_created_total`, `tracelens_templates_evicted_total`, `tracelens_templates_total`

---

## Known gaps

These are deliberate and recorded in [docs/DECISIONS.md](docs/DECISIONS.md):

- **Histogram buckets are not stored.** The schema's single `Float64 value`
  cannot hold bucket bounds; histograms decompose into `_count` and `_sum`
  series, so quantile queries over histograms are not answerable yet.
- **Attribute values are flattened to `String`.** Numeric predicates in the
  query engine will need a cast.
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

## Roadmap

1. ~~OTLP ingestion, ClickHouse schema, demo workload~~ — done
2. ~~Trace assembly + tail sampling (bounded buffer, eviction policy, sampling weights)~~ — done
3. ~~Log pipeline (Drain templating, cardinality control, per-severity retention)~~ — done
4. Query engine: DSL → AST → logical plan → physical plan
5. UI: trace waterfall, flamegraph, service map, log explorer
