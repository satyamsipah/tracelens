# Runbook

Operating TraceLens: what to look at, what the common failures look like, and
what to do about them.

- [First five minutes](#first-five-minutes)
- [Alert playbooks](#alert-playbooks)
- [Scaling](#scaling)
- [Cold storage tiering](#cold-storage-tiering)
- [Schema changes](#schema-changes)
- [Disaster recovery](#disaster-recovery)
- [Configuration reference](#configuration-reference)
- [Metrics reference](#metrics-reference)

---

## First five minutes

Three places to look, in order:

1. **`/health` in the UI** — ingestion rate, queue depth, sampler memory,
   consumer lag, storage size, compression ratio.
2. **The self-monitoring Grafana dashboard**
   ([`deploy/grafana/dashboards/self-monitoring.json`](../deploy/grafana/dashboards/self-monitoring.json))
   — same numbers with history, which is usually the question.
3. **`/readyz` on each component** — tells you *which* dependency is down
   rather than that something is.

```bash
curl -s localhost:9464/readyz   # collector  → Kafka
curl -s localhost:9465/readyz   # assembler  → ClickHouse AND Kafka
curl -s localhost:9466/readyz   # query      → ClickHouse
```

A 503 body names the failing dependency. `/healthz` deliberately never
consults a downstream, so a 200 there with a 503 on `/readyz` means "the
process is fine, something it needs is not".

### The four metrics that explain most incidents

```promql
rate(otlp_spans_dropped_total[5m])          # by reason — see the table below
ingest_queue_depth / ingest_queue_capacity  # collector backpressure
tracelens_consumer_lag_seconds              # assembler falling behind
tracelens_inflight_bytes                    # sampler memory against its cap
```

---

## Alert playbooks

### `otlp_spans_dropped_total` is rising

Check the `reason` label first — the four causes are unrelated problems.

| `reason` | Means | Do |
|---|---|---|
| `rejected` | Queue over its high-water mark; clients were told to retry | Not data loss yet. Scale collectors, or check why the assembler is not draining Kafka. |
| `queue_full` | Hard cap hit | Same causes, worse. Raise `TRACELENS_QUEUE_CAPACITY` only if memory allows — this is a symptom of a slow consumer, not a small queue. |
| `decode_error` | Malformed OTLP | Almost always one bad client. Correlate by peer address; the payload is not the platform's problem to fix. |
| `cardinality_budget` | An attribute key blew its budget | See below. |
| `produce_failed` | Kafka rejected the write | Broker health. Check `/readyz` on the collector. |

### `tracelens_consumer_lag_seconds` climbing

The assembler is behind. In order of likelihood:

1. **ClickHouse is slow or down.** Check `clickhouse_insert_retries_total` and
   the assembler's `/readyz`. Inserts are the assembler's only slow path.
2. **Too few assembler replicas.** Scale up — but **never past
   `TRACELENS_KAFKA_PARTITIONS`** (default 12). Kafka gives each partition to
   exactly one group member; extra replicas idle while holding memory.
3. **A genuine traffic increase.** Confirm against ingestion rate.

Note the metric is a wall-clock proxy (`time.Since(record.Timestamp)` at
consume time), not offset-based. It answers "how stale is what I am
processing", which is the operator question, but it will read high after a
backlog even once throughput has recovered.

### `tracelens_forced_decisions_total` rising

The in-flight buffer is at capacity and traces are being decided on partial
data. Sampling still works, but decisions are being made without having seen
the whole trace — an error span arriving after the forced decision is a late
span, and may be dropped.

- Raise `TRACELENS_BUFFER_MAX_BYTES` / `_MAX_TRACES` if the machine has
  headroom (the memory limit must stay above the byte cap).
- Or lower `TRACELENS_DECISION_WAIT` so traces leave the buffer faster,
  accepting more incomplete traces in exchange for fewer forced ones.
- Or scale out, which divides the trace population across more buffers.

This metric existing at all is deliberate: the alternative design silently
discarded traces, which is indistinguishable from normal sampling.

### `tracelens_cardinality_breaches_total{key}` firing

An attribute key exceeded its budget. Look at
`tracelens_cardinality_estimate{key}` for how far.

Usually a span or attribute carrying an unbounded value — a URL with an ID
in the path, a raw user agent, a UUID. Fix it at the source if you can. If
you cannot, set that key's action in
[`deploy/tracelens/cardinality.yaml`](../deploy/tracelens/cardinality.yaml):

- `bucket` (default) — hash into fixed buckets; stays queryable at reduced
  granularity
- `drop` — strip the attribute entirely past budget
- `keep_and_alert` — keep it, just count the breach, for keys where high
  cardinality is a symptom worth investigating rather than a cost to cap

### `tracelens_offset_watermark_data_loss_total` incremented

The broker told the consumer that offsets it was tracking no longer exist —
retention expiry or a KIP-320 leader-epoch truncation. The watermark entries
below the reset point were released explicitly, which is the only way to
unstick a commit floor that would otherwise never advance.

**Spans in that offset range are gone.** That is broker data loss, not a
TraceLens bug, but it should not pass silently: check broker retention
against how long the assembler was down, since a consumer offline longer than
retention is the usual cause.

### Query API returning 400 "would scan N rows"

Working as intended: the preflight `EXPLAIN ESTIMATE` guard refused a query
before running it. Narrow the time range or add a `service` selector. Raise
`TRACELENS_QUERY_MAX_ROWS_SCANNED` only deliberately — the guard is what
stops one query from becoming an outage.

---

## Scaling

| Component | Scale on | Ceiling |
|---|---|---|
| collector | CPU | none — stateless |
| assembler | **consumer lag**, not CPU | `TRACELENS_KAFKA_PARTITIONS` |
| query | CPU | none — stateless |
| web | CPU | none — stateless |

The assembler is the one that needs care. Its cost is dominated by *waiting*
— on the decision window, on ClickHouse — so CPU stays flat while the backlog
grows, and a CPU-based HPA would never fire. Kubernetes configuration for
lag-based autoscaling (prometheus-adapter or KEDA) is in
[`deploy/k8s/README.md`](../deploy/k8s/README.md).

**Scale the assembler down slowly.** Every scale event triggers a
consumer-group rebalance that stops consumption on the partitions that move.
Shedding replicas eagerly after a burst costs throughput exactly while the
backlog is still draining. The shipped config uses a 600s scale-down
stabilization window and one pod at a time.

**Changing the partition count is a migration, not a knob.** Kafka's
partitioner is `hash(key) % N`, so changing N rehashes every trace_id and
splits any trace in flight across the change — the same failure as
round-robin routing, once, at cutover. Drain the topic first.

---

## Cold storage tiering

ClickHouse's own TTL moves parts to a cold volume at 7 days and deletes at 30.
For anything longer, `cmd/coldexport` writes whole partitions to S3 as
Parquet.

Parquet rather than a native ClickHouse backup on purpose: cold data is
exactly the data you want readable by something other than this cluster in
three years. DuckDB, Spark, Athena and pandas all read it; a native backup is
readable only by ClickHouse.

### Export

```bash
export TRACELENS_S3_BUCKET=tracelens-cold
export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=...

# 1. Dry run — always. Lists candidate partitions with row and byte counts.
go run ./cmd/coldexport -table spans -older-than 720h

# 2. Export, keeping the local copy.
go run ./cmd/coldexport -table spans -older-than 720h -export

# 3. Export and reclaim the space.
go run ./cmd/coldexport -table spans -older-than 720h -export -drop
```

Objects land Hive-style so the bucket is directly queryable:

```
s3://tracelens-cold/tracelens/spans/dt=2026-08-01/data.parquet
s3://tracelens-cold/tracelens/spans/dt=2026-08-01/_manifest.json
```

**Nothing is dropped unverified.** `-drop` re-reads the exported object,
counts its rows, and compares against both the manifest and the *current*
source count. Two distinct guards:

- The object read-back catches an export deleted or corrupted by a lifecycle
  rule since it was written.
- The source re-count catches rows that arrived between export and drop.
  Without it, a partition that gained late spans would lose exactly those
  rows, silently.

If rows arrived in between, the drop is refused with `re-export first`. Do
exactly that — re-export overwrites, so it is safe to repeat.

### Restore

```bash
go run ./cmd/coldexport -table spans -restore 2026-08-01
```

The manifest names the columns, so a table whose schema has moved on since
the export still restores: new columns take their `DEFAULT`, and columns the
table no longer has are skipped with a warning rather than failing the whole
restore.

The round trip is covered by `TestColdTierRoundTrip`, which runs a real
MinIO and asserts not just row counts but that `Enum8`, `Map`, `Array` and
`sampling_weight` survive Parquet intact — a count-only check would pass on a
restore that silently emptied every map.

### Reading cold data without ClickHouse

```sql
-- DuckDB
SELECT service_name, count(*)
FROM read_parquet('s3://tracelens-cold/tracelens/spans/dt=2026-08-01/*.parquet')
GROUP BY 1;
```

---

## Schema changes

Migrations live in
[`internal/storage/migrations/`](../internal/storage/migrations/) and are
applied by the assembler on start, or standalone with `make migrate`.

Two rules:

1. **Applied history is never rewritten.** Migration 0002 still contains a
   codec choice that 0005 corrects, with a comment saying so. Editing 0002
   would mean a fresh database and an existing one disagree about what
   "version 2" means.
2. **Every column declares a codec, and every codec carries a comment**
   naming the query it serves and what it measured. A test fails CI if a
   column has no codec.

Deploy order matters: the assembler owns migrations, so it must roll before
the query API reads against the new schema. The GitHub Actions workflow
sequences it that way.

---

## Disaster recovery

**ClickHouse lost, Kafka intact.** Re-run migrations, then reset the
assembler's consumer group to the earliest available offset. Everything still
inside broker retention replays. Traces older than retention are gone unless
they were cold-exported.

**Kafka lost, ClickHouse intact.** Stored spans are unaffected. In-flight
traces (up to `TRACELENS_DECISION_WAIT` plus whatever was unconsumed) are
lost. Recreate the topics with the **same partition count** — a different
count rehashes every trace_id.

**Assembler restarted.** Normal operation. Undecided traces replay from the
commit floor. The offset watermark is in-memory and rebuilds from the
committed offset, which is by definition at or below the old floor, so
nothing is skipped. Expect a lag spike and a burst of duplicate-suppressed
inserts (`ReplacingMergeTree` plus an insert deduplication token make a
replayed batch a no-op — tested directly).

**Both lost.** Restore from cold storage, per partition, oldest first. The
service graph and RED rollups do **not** restore with the raw spans: they are
derived at ingest time, not computed from stored rows. Their own retention
(up to 400 days for the 1h rollup) usually outlives the raw data anyway,
which is the point of keeping them.

---

## Configuration reference

Everything is environment-driven, and the defaults make `docker compose up`
work with nothing set. The knobs that matter:

### Ingestion

| Variable | Default | Meaning |
|---|---|---|
| `TRACELENS_QUEUE_CAPACITY` | `8192` | Hard cap of the bounded queue, per signal |
| `TRACELENS_QUEUE_HIGH_WATER_RATIO` | `0.8` | Fraction of capacity at which rejection begins |
| `TRACELENS_BACKPRESSURE_{TRACES,LOGS,METRICS}` | `reject` | `reject` or `drop_oldest` |
| `TRACELENS_BATCH_MAX_RECORDS` | `512` | Flush trigger — records |
| `TRACELENS_BATCH_MAX_BYTES` | `4MiB` | Flush trigger — bytes |
| `TRACELENS_BATCH_FLUSH_INTERVAL` | `200ms` | Flush trigger — time |
| `TRACELENS_KAFKA_PARTITIONS` | `12` | **Fixed by design** — changing it rehashes every key |

### Tail sampling

| Variable | Default | Meaning |
|---|---|---|
| `TRACELENS_DECISION_WAIT` | `5s` | Fixed-wait ceiling before a trace is force-decided |
| `TRACELENS_BUFFER_MAX_TRACES` | `50000` | In-flight buffer cap, trace count |
| `TRACELENS_BUFFER_MAX_BYTES` | `256MiB` | In-flight buffer cap, bytes |
| `TRACELENS_BUFFER_EVICTION` | `forced_decision` | `forced_decision` or `discard` at capacity |
| `TRACELENS_POLICY_FILE` | `/etc/tracelens/policies.yaml` | Policy chain, hot-reloaded on mtime change |
| `TRACELENS_CARDINALITY_FILE` | `/etc/tracelens/cardinality.yaml` | Per-key budgets and breach actions |
| `TRACELENS_RELOAD_INTERVAL` | `5s` | Policy-file mtime poll interval |
| `TRACELENS_WATERMARK_MAX_AGE` | `10m` | Age past which the watchdog releases an offset the buffer no longer tracks |

### Storage

| Variable | Default | Meaning |
|---|---|---|
| `TRACELENS_CLICKHOUSE_ADDR` | `localhost:9000` | Native protocol address |
| `TRACELENS_CLICKHOUSE_TLS` | `false` | **Required by every managed ClickHouse** (Cloud listens on 9440 with TLS) |
| `TRACELENS_CLICKHOUSE_BATCH_SIZE` | `10000` | Rows per INSERT |
| `TRACELENS_DRAIN_DEPTH` / `_SIMILARITY` / `_MAX_CHILDREN` / `_MAX_TEMPLATES` | `4` / `0.6` / `100` / `10000` | Drain tree shape and template caps |

### Query and alerting

| Variable | Default | Meaning |
|---|---|---|
| `TRACELENS_QUERY_HTTP_ADDR` | `:8080` | Query API listen address |
| `TRACELENS_QUERY_TIMEOUT` | `30s` | Per-query hard wall-clock bound |
| `TRACELENS_QUERY_MAX_ROWS_SCANNED` | `50000000` | Preflight `EXPLAIN ESTIMATE` guard |
| `TRACELENS_ALERT_RULES_FILE` | `/etc/tracelens/alerts.yaml` | Missing or invalid disables alerting, not startup |
| `TRACELENS_ALERT_EVAL_INTERVAL` | `60s` | How often every rule is evaluated |

### Cold tiering (`cmd/coldexport`)

| Variable | Meaning |
|---|---|
| `TRACELENS_S3_BUCKET` | Destination bucket |
| `TRACELENS_S3_PREFIX` | Key prefix, default `tracelens` |
| `TRACELENS_S3_ENDPOINT` | S3-compatible endpoint; empty means AWS |
| `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` | Credentials. Never passed as flags, so they stay out of shell history and `ps` |

---

## Metrics reference

Every pipeline stage exports received / dropped / errored / queue depth /
processing latency. The ones worth alerting on:

```
otlp_spans_dropped_total{reason}          rejected | queue_full | decode_error
                                          | cardinality_budget | produce_failed
ingest_queue_depth{signal}                against ingest_queue_capacity{signal}
clickhouse_rows_inserted_total{table}
clickhouse_insert_retries_total{table}

tracelens_inflight_traces                 sampler buffer, count
tracelens_inflight_bytes                  sampler buffer, bytes
tracelens_forced_decisions_total          decided early under memory pressure
tracelens_evicted_traces_total
tracelens_decisions_total{outcome,policy} which policy decided, and how
tracelens_late_spans_total{outcome}       arrived after the decision

tracelens_cardinality_breaches_total{key}
tracelens_cardinality_estimate{key}
tracelens_templates_created_total
tracelens_templates_evicted_total
tracelens_templates_total

tracelens_consumer_lag_seconds{topic}     wall-clock proxy, not offset-based
tracelens_offset_watermark_data_loss_total
tracelens_offset_watermark_expired_total
```
