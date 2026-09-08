# TraceLens

An OpenTelemetry-compatible observability platform: telemetry ingestion,
columnar storage, tail-based sampling, and a query engine for distributed
traces, logs and metrics.

**Current phase: ingestion, storage, and a demo workload.** Tail sampling and
the query engine are not implemented yet — see [Roadmap](#roadmap).

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
                                          decode ▶ batch ▶ retry ▶ insert
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
| `internal/pipeline` | Kafka producer/consumer, topic management |
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

### Metrics worth watching

- `otlp_spans_dropped_total{reason}` — `rejected`, `queue_full`, `decode_error`, `cardinality_budget`, `produce_failed`
- `ingest_queue_depth{signal}` vs `ingest_queue_capacity{signal}`
- `clickhouse_rows_inserted_total{table}`, `clickhouse_insert_retries_total{table}`

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
- **`sampling_weight` is always 1**, since nothing samples yet. The column
  exists now so no aggregate needs rewriting when it stops being 1.
- **The `spans` ORDER BY costs ~4.2× read amplification on a service+time
  query with no `span_name` filter** (measured via `EXPLAIN ESTIMATE`).
  Swapping `span_name` and `timestamp` in the key would move the identical
  cost onto service-map queries instead — left as-is pending a real query-mix
  measurement to decide which pattern is higher QPS.

## Roadmap

1. ~~OTLP ingestion, ClickHouse schema, demo workload~~ — done
2. Trace assembly + tail sampling (bounded buffer, eviction policy, sampling weights)
3. Query engine: DSL → AST → logical plan → physical plan
4. UI: trace waterfall, flamegraph, service map, log explorer
