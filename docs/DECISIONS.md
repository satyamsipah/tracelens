# Architecture Decision Log

Every significant decision, the options rejected, and why.

---

## 2026-09-08 — Phase 1: OTLP ingestion, ClickHouse schema, demo workload

### 1. Backpressure policy: reject, not drop

**Decided.** When a signal's bounded queue crosses its high-water mark, the
receiver answers gRPC `RESOURCE_EXHAUSTED` (with a `RetryInfo` detail) or HTTP
`429` (with `Retry-After` and a `google.rpc.Status` body). Rejection is
whole-request. The high-water mark is 80% of capacity, not 100%.

**Rejected: drop-oldest with a counter.** It keeps producer latency flat and
avoids retry storms, and it remains available per-signal via
`TRACELENS_BACKPRESSURE_*` because it is genuinely defensible for metrics,
where the next scrape supersedes what was lost.

**Why reject won:**

- Both OTLP status codes are in the exporter's *retryable* set, so the SDK
  backs off and resends. Under transient pressure nothing is lost — it is
  deferred. Drop-oldest, by contrast, returns `OK` for data it discarded, so
  the client can never resend it and the loss is permanent.
- Drop-oldest evicts the *oldest* entry, which preferentially discards the
  earliest spans of in-flight traces. The damage therefore lands as **truncated
  traces**, which are far harder to detect downstream than absent ones, and
  which will make the phase-2 tail sampler decide on fragments.
- Buffering moves to the client's `BatchSpanProcessor`, which is itself bounded
  and exports its own drop counter. If data must die, it dies where its owner
  can see it.

**Why the 80% high-water mark:** refusing before the queue is physically full
leaves headroom for batches already in flight, so the queue degrades smoothly
instead of wedging at exactly full. That slack also absorbs races between
concurrent requests, which is what lets `EnqueueBatch` use non-blocking sends
and still be atomic in practice.

**Why whole-request atomicity:** admitting a prefix of a request would hand the
assembler a trace missing spans the client believes it delivered. Enforced by
`Queue.EnqueueBatch` and tested in `TestQueueEnqueueBatchIsAtomic`.

`otlp_spans_dropped_total{reason}` is exported under both policies, with
`reason` distinguishing `rejected` from `queue_full`.

---

### 2. Kafka partitioning: `trace_id`, never round-robin

**Decided.** Span records are keyed with the raw 16-byte `trace_id`;
franz-go's `StickyKeyPartitioner` hashes the key, so identical trace ids always
resolve to identical partitions. A span record with no key is a **hard error**
(`ErrMissingPartitionKey`), not a fallback.

**What breaks without it — specifically, in phase 2:**

The trace assembler is stateful, buffering in-flight spans keyed by trace, and
consumer-group members each own a subset of partitions. If one trace's spans
land on different partitions:

- **Each instance sees only a fragment.** Each independently judges its
  fragment complete enough and emits, so one trace becomes N partial traces.
- **Sampling decides on incomplete data.** The instance that never received the
  `ERROR` span drops a failing trace — a false negative that is invisible,
  because nothing records that the trace ever existed.
- **The bounded in-flight buffer fills with fragments that never complete**,
  forcing eviction of good traces. Principle 2's hard cap becomes a liability
  rather than a safeguard.
- **Aggregates double-count.** Each fragment carries its own `sampling_weight`,
  so any count over sampled data is wrong — violating principle 6 at the
  storage layer, silently.

The insidious part is that with round-robin *the pipeline keeps working*.
Ingestion succeeds, rows land, dashboards populate. Nothing looks wrong until
sampling starts making decisions, which is why this is a produce-time error and
not a warning.

**Partition count is fixed configuration.** Kafka's partitioner is
`hash(key) % N`, so changing N rehashes every key and splits any trace in
flight across the change — reintroducing the exact failure. Growing partition
count is a planned migration, not a knob.

**Other signals:** logs are keyed by `trace_id` when present (co-locating them
with their spans for correlation) and by service name otherwise. Metrics are
keyed by a hash of service + metric name, which is coarser than series
granularity but a strict superset of it — enough to guarantee no series is
split, without paying to group by label set on the hot path.

Verified against a real Redpanda in `TestProducerRoutesTraceToSinglePartition`.

---

### 3. ClickHouse codecs

**Decided.** Every column declares an explicit codec, matched to its data
shape. This is enforced by a test
(`TestSchemaDeclaresCodecOnEveryColumn`) that queries `system.columns` for an
empty `compression_codec`, so a column added later without one fails CI rather
than depending on review vigilance.

| Column | Codec | Reasoning |
|---|---|---|
| `timestamp` | `DoubleDelta, ZSTD(1)` | Rows in a granule are sorted by (service, name, time), so the stream is monotonic and near-regular — ~1–2 bytes/row against 8 raw |
| `trace_id`, `span_id`, `parent_span_id` | `ZSTD(1)` | CSPRNG output. **Delta, T64 and Gorilla would all inflate it** by adding framing to incompressible bytes. ZSTD only, and only because spans of one trace repeat within a granule |
| `service_name`, `span_name` | `LowCardinality(String), ZSTD(1)` | Under the ~10k threshold, and leading the sort key so runs are long |
| `span_kind`, `status_code` | `Enum8, ZSTD(1)` | Closed sets (6 and 3 values). One byte, and ClickHouse rejects garbage at insert — strictly better than `LowCardinality(String)` here |
| `duration_ns` | `T64, ZSTD(1)` | **Not** Delta: durations are non-monotonic, so Delta encodes noise. T64 transposes the 64-bit block and crops provably-unused high bits — nearly every duration fits in <2³² |
| `severity_number` | `T64, ZSTD(1)` | OTLP range is 1–24, so 56 bits per value are provably zero |
| `template_id` | `T64, ZSTD(1)` | **Constrains phase 2:** must be a *dense dictionary id*, not a hash. A hash has random high bits and T64 would do nothing |
| metric `value` | `Gorilla, ZSTD(1)` | XOR against predecessor. **Only works if consecutive rows are the same series**, which is why `labels_hash` precedes `timestamp` in the sort key — the codec choice and the sort key are one decision |
| `AggregateFunction` states | `ZSTD(1)` | Opaque serialised t-digest; no structural codec can read into it. Stated explicitly rather than defaulted |

**ZSTD(1) on the insert path, ZSTD(6) on cold parts.** Insert CPU *is*
ingestion throughput, so paying for level 6 on every write would trade the
metric that matters for one that does not. `TTL … RECOMPRESS CODEC(ZSTD(6))`
at 2–3 days resolves the tension instead of picking a side.

**Measured compression — two numbers, and the difference between them matters:**

| Dataset | Rows | Raw | Compressed | Ratio | Bytes/span |
|---|---|---|---|---|---|
| Test fixture (`TestCompressionRatioIsMeasured`) | 20,000 | 2.65 MiB | 74 KiB | **36.67×** | 3.8 |
| Live `loadgen` + demo traffic (`make compression`) | 50,963 | 9.14 MiB | 1.84 MiB | **4.97×** | 37.8 |

The test fixture draws service and operation names from a set of four and
reuses the same attribute map on every row, so 36× is an artefact of the
fixture, not a property of the schema. **4.97× at ~38 bytes/span is the honest
figure** — and even that is optimistic, since loadgen's attribute values are
far less varied than production. The test therefore *records* the ratio rather
than asserting a threshold; a threshold on synthetic input would be a fake
assertion that passes forever regardless of codec changes.

**Measurement is table-level, not per-column.** The ClickHouse 24.8 alpine
build reports `column_data_compressed_bytes` as 0 in both
`system.parts_columns` and `system.columns`, while `system.parts` is
populated. The first version of this test summed the per-column figures,
which meant it was asserting over an all-zero result set. Corrected to
`system.parts`; `make compression` still prints the per-column breakdown for
humans, where zeros are visibly zeros rather than silently aggregated away.

---

### 4. Sort keys

| Table | `ORDER BY` | `PRIMARY KEY` |
|---|---|---|
| `spans` | `(service_name, span_name, timestamp, trace_id, span_id)` | `(service_name, span_name, timestamp)` |
| `logs` | `(service_name, timestamp, trace_id, span_id)` | `(service_name, timestamp)` |
| `metrics` | `(service_name, metric_name, labels_hash, timestamp)` | (full key) |

**`spans` leads with `service_name`** because the analytical queries — RED
metrics, service map, slow operations — all filter it first, *and* because
clustering like values is what makes every downstream column's codec work.
`trace_id`/`span_id` are in the tail only to give `ReplacingMergeTree` a dedup
identity; `PRIMARY KEY` is deliberately the shorter prefix so the sparse index
stays small and cache-resident.

**Rejected: leading with `trace_id`.** It would make trace retrieval a granule
seek, but destroys locality for every analytical query and wrecks compression
on `service_name`. Trace lookup is served by a bloom filter instead.

**`logs` deliberately excludes `severity_number` from the key.** Putting it
second would prune "show me errors" nicely but scatter the far more common
all-severity tail across the whole day. A `set(24)` skip index buys the error
pruning without that cost — chosen over `minmax` because severity is
categorical, not ordinal-ranged.

---

### 5. Skip indexes

- **`bloom_filter(0.01)` on `trace_id`, GRANULARITY 1** — serves "open trace X".
  Since `trace_id` is not a sort-key prefix, without this a point lookup
  full-scans the partition. It is the one index here worth real bytes
  (~10KB/granule).
- **`tokenbf_v1` on log `body`** — serves free-text search. Without it every
  substring query decompresses the whole body column.
- **`set(24)` on `severity_number`** — see above.
- **`bloom_filter` on `mapKeys(span_attributes)`** — serves "spans that *have*
  attribute k" without decompressing the map value stream.
- **`minmax` on `duration_ns` — flagged as probably not earning its keep.**
  `ORDER BY` does not correlate with duration, so every granule likely holds a
  near-full duration range and this prunes almost nothing. It is kept only to
  be measured with `EXPLAIN indexes=1`; the honest expectation is that it gets
  dropped. The real pruning for "slow spans" comes from the service+time
  prefix. Recorded here so the decision to remove it is already justified.

---

### 6. Duplicate handling: `ReplacingMergeTree` + insert dedup token

**Decided.** Two independent defences, because they cover different failures:

1. **`insert_deduplication_token`**, derived deterministically from the Kafka
   topic, partition and offset set of the batch. A redelivery after a crash
   between the insert and the offset commit replays the same offsets, produces
   the same token, and ClickHouse discards it server-side. This requires
   `non_replicated_deduplication_window` on every table — **without that
   setting the token is silently ignored**, which is exactly the kind of
   quiet no-op worth writing down.
2. **`ReplacingMergeTree`** with `trace_id`/`span_id` in the sort-key tail,
   covering duplicates that arrive via *different* Kafka batches (an OTLP
   client resending a batch it already delivered), where the tokens differ.

**Rejected: plain `MergeTree` with query-time `GROUP BY`.** Shortest sort key
and fastest inserts, but every query must remember to deduplicate, and any
query that forgets silently over-counts.

**Caveat, accepted:** `ReplacingMergeTree` collapses only on merge and only
within a partition. Exact counts need `FINAL` until merges catch up. Since
`PARTITION BY toDate(timestamp)` and duplicates arrive close in time, they land
in the same partition. Both behaviours are pinned by
`TestWriterDeduplicatesDuplicateInput`.

---

### 7. Consumer offsets: at-least-once, commit after write

Auto-commit is disabled. Offsets advance only after the writer reports the
batch durably stored. Committing on poll would make the pipeline at-most-once
and lose a batch on any writer crash — silently. At-least-once plus the two
dedup mechanisms above is the correct trade.

The writer uses **exactly one worker goroutine**. Not an oversight: inserts
must land in submission order for offset commits to stay monotonic, and
ClickHouse wants few large inserts rather than many concurrent small ones,
since every INSERT becomes a part and part count drives the merge backlog.
Pipelining comes from the consumer decoding batch N+1 while the worker writes
batch N. The writer's queue is deliberately shallow (4) so that a slow writer
becomes visible as **consumer lag** rather than as unbounded memory growth.

---

### 8. Retry with full jitter

Retries use exponential backoff with full jitter (`[d/2, d)`), and a set of
permanent ClickHouse error codes (`TYPE_MISMATCH`, `UNKNOWN_TABLE`,
`SYNTAX_ERROR`, …) that skip the retry loop entirely. Jitter decorrelates
replicas: several assemblers hit "too many parts" simultaneously, and
un-jittered backoff would have them all retry in lockstep and re-trigger it.
Retrying a schema bug five times just delays the error the operator needs.

---

### 9. Hot-path allocation: arena grouping (before/after)

The splitter originally grouped spans as `map[traceID][]*Span`. Because each
request `clear()`s the map, every reset discarded the slices' backing arrays,
so each trace paid the full 1→2→4→8 append growth again.

Replaced with an index into a **reused arena** (`map[traceID]int` +
`[][]*Span`), so backing arrays survive across requests.

| Benchmark | Before | After |
|---|---|---|
| `SplitTraces` 1 trace × 10 spans | 2,558 ns · 272 B · **6 allocs** | 2,129 ns · 24 B · **1 alloc** |
| `SplitTraces` 10 traces × 10 spans | 24,335 ns · 2,508 B · **51 allocs** | 21,150 ns · 24 B · **1 alloc** |
| `SplitTraces` 50 traces × 4 spans | 63,974 ns · 2,831 B · **151 allocs** | 53,573 ns · 25 B · **1 alloc** |
| `SplitAndEnqueue` (full path) | 26,444 ns · 2,518 B · **51 allocs** | 23,715 ns · 28 B · **1 alloc** |

*(Apple M1, `go test -bench . -benchmem`.)* Allocations are now **constant per
request** rather than linear in trace count — which is the property that
matters, since batch size grows with load. `EnqueueBatch` is 0 allocs/op.

The arena is capped at 256 slices so one pathological batch cannot pin
unbounded memory in the pool.

---

### 10. Accepted gaps and their costs

- **Histogram buckets are not stored.** The requested schema's single
  `Float64 value` cannot represent bucket bounds and counts. Histograms and
  summaries decompose into `<name>_count` and `<name>_sum` (the Prometheus
  convention) and **buckets are dropped**. Quantile queries over histograms are
  therefore unanswerable until a dedicated `metrics_histogram_buckets` table
  exists. This is a limitation of the phase-1 schema, not an oversight.
- **Attribute values flatten to `String`.** A `Map(String, String)` is one
  column pair with one codec; preserving OTLP's `AnyValue` types would need
  several typed maps or a variant encoding. The cost is that numeric predicates
  in the query engine must cast.
- **`resource_attributes` are stored per span**, though they are byte-identical
  for every span from one process. Sorted by `service_name` they compress well,
  but the honest alternative is a `resource_fingerprint` + dimension table.
  Deferred until there is a measured ratio to argue from.
- **OTLP/JSON is not implemented.** Protobuf is required by the spec, JSON is
  optional. A JSON request gets `415` rather than a silent misparse.
- **`sampling_weight` is always 1.** The column exists now, before anything
  samples, so no aggregate written against this table needs rewriting later and
  there is no window where queries silently undercount.

---

### 11. Two bugs worth recording, because both failed *silently*

**A `--` inside an XML comment made `storage.xml` invalid.** XML forbids a
double hyphen in a comment, and ClickHouse refuses to start on a malformed
config rather than ignoring it. The stack failed to come up.

**And the tests did not catch it — they reported green.** The container-backed
tests treated *any* `testcontainers.Run` error as "Docker is probably not
available" and called `t.Skip`. Because `go test` prints `ok` for a package
whose tests all skip, the suite reported success while executing none of the
storage assertions. Two defects compounding: a broken config, and a test
harness that made the breakage look like a pass.

**Fix:** the harness now distinguishes *Docker absent* (a legitimate skip) from
*Docker present but the container failed* (a failure), via
`provider.Health()`. A container that will not start on a machine that has
Docker is a real defect — a bad config, a broken image tag, a schema the
server rejects — and must fail loudly.

The general lesson, recorded because it will recur: **a conditional skip is a
silent test-coverage hole.** Any skip condition broad enough to catch real
failures will eventually catch one.

**A third, related one: the ClickHouse healthcheck probed `localhost`.**
ClickHouse binds `0.0.0.0` (IPv4 only), while `localhost` resolves to `::1`
first, so `wget http://localhost:8123/ping` got connection refused against a
server that was working perfectly — the native protocol on 9000 was serving
queries the whole time. The container sat `unhealthy`, and because the
assembler and demo services gate on `condition: service_healthy`, *none of
them started*. Fixed by probing `127.0.0.1`. Worth remembering that a
healthcheck can fail for reasons entirely unrelated to health.

---

### 12. Smaller decisions

- **Schema columns declined:** `trace_state`, `scope_name`/`scope_version`, and
  the OTLP `dropped_*_count` fields were proposed and cut to keep the span row
  to the specified shape. Consequence: the receiver cannot round-trip
  instrumentation-scope metadata, and SDK-side attribute clamping is not
  visible in storage.
- **Go floor is 1.25**, inherited from `golang.org/x/sync` and
  `genproto/googleapis/rpc`. Discovered when the 1.24 build image failed at
  `go mod download` with `GOTOOLCHAIN=local`.
- **Four demo services, one binary.** The call graph is the interesting part of
  a demo workload; keeping it in one table (`demo/topology.go`) makes it
  readable and stops four near-identical copies drifting. Compose still runs
  four containers, so traces are genuinely cross-process.
- **Head sampling is off in the demo** (`AlwaysSample`). Tail sampling can only
  decide on traces it actually receives.
- **Testcontainers mounts the production `storage.xml`**, so the schema under
  test is byte-identical to the deployed one — including
  `storage_policy='tiered'`. Testing a different schema than you run is how
  storage bugs escape.
- **Decompression is double-capped** — `MaxBytesReader` on the compressed
  stream and `io.LimitReader` on the decompressed one. Only the second stops a
  decompression bomb.
