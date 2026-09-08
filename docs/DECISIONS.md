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

### 3a. `@storage-reviewer` findings against ~900K live spans, and what was fixed

A general-purpose agent ran the storage-reviewer checklist against the
running stack after it had processed real demo traffic plus loadgen runs
(887,710 → 905,166 spans across two passes). Findings, ranked, with
independent verification before any fix was applied (see below for why that
mattered):

| # | Finding | Measured | Action |
|---|---|---|---|
| 1 | `idx_duration` minmax prunes **zero** granules | `EXPLAIN indexes=1` on `service_name='gateway' AND duration_ns>200000000`: `Skip idx_duration Granules: 38/38` — every granule surviving the primary-key filter also survives the minmax check | **Fixed.** Dropped in migration 0005. Confirms exactly what was flagged as suspect when the index was added (§5 above) |
| 2 | `span_id CODEC(ZSTD(1))` **inflates** the column | `system.columns`: raw 7,111,792 B → compressed 7,115,186 B (ratio 0.9995, i.e. *larger* after compression) | **Fixed.** Changed to `CODEC(NONE)` in migration 0005, along with `links.span_id` (identical shape, fixed ahead of having its own data to measure, on structural grounds) |
| 3 | ORDER BY costs ~4.2× read amplification for the RED-metrics query shape (service+time, no span_name filter) | `EXPLAIN ESTIMATE`: ground truth 6,265 matching rows, 26,236 rows read | **Not acted on.** A real trade-off, not a bug — swapping `span_name` and `timestamp` in the key would move the identical cost onto the service-map query shape instead. Needs a real QPS split between the two patterns to decide, which phase 1 does not have |
| 4–5 | `idx_trace_id` and `idx_attr_keys` bloom filters both prune real work (89.7% and 49% of granules respectively, confirmed via `SET use_skip_indexes=0/1`) | — | No change — validated as earning their keep |
| 6 | 6–7 active parts for <900K rows in one partition | `EXPLAIN` shows `Parts: 6/6` | Not a DDL issue — background merges lagging insert rate under sustained loadgen pressure. Self-resolves; not acted on |
| 7 | `logs`/`metrics` had 0 rows at review time | — | Indexes on those tables are validated only statically; re-review once log/metric ingestion carries real traffic |

**Why finding #2 was verified independently before acting on it, and it was
the right call:** the agent's report cited non-zero `system.columns` byte
values, but an earlier compression test in this same session had found those
columns reporting **zero** on a single-row sample. Re-querying directly
confirmed the columns populate correctly once the table holds real volume —
the zero was a small-sample artifact, not a permanent build limitation, and
the finding itself held up exactly as reported. The general policy stands
regardless of outcome: verify an agent's cited numbers against the live system
before changing schema on their word, especially when a prior measurement in
the same session appears to contradict them.

**Why this is migration 0005, not an edit to 0002:** 0002 was already applied
against the running instance, and its ledger entry exists. Editing that file's
`CREATE TABLE` in place would silently diverge a fresh install (which reads
the edited file) from the already-migrated instance (whose `IF NOT EXISTS`
skips re-running it) — the exact kind of silent divergence this log exists to
prevent. `0002`'s inline comments were updated to point at 0005 for a future
reader, since comment text carries no schema semantics and isn't re-executed.

Post-fix compression: 905,166 spans, 143.72 MiB → 35.97 MiB, **4.0× overall**.

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
- **`minmax` on `duration_ns` — flagged as probably not earning its keep, then
  measured and removed.** `ORDER BY` does not correlate with duration, so
  every granule holds a near-full duration range. `EXPLAIN indexes=1` on a
  live "slow spans for service X" query confirmed 0 of 38 granules pruned.
  Dropped in migration 0005 (§3a). The real pruning for that query comes from
  the service+time prefix, as originally predicted.

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

---

## 2026-09-08 — Phase 2: trace assembly, tail sampling, cardinality control, Drain

Four decisions were gated behind explicit approval before any code was written.

### 1. Completion heuristic: fixed wait + root-closure early exit

**Decided.** A trace decides at `T+DecisionWait` (default 5s) from its first
span, unless the root span (parent_span_id all-zero) is present and every
buffered span's interval already fits inside `[root.start, root.end]`, in
which case it decides immediately.

**Rejected: plain fixed wait.** Simpler (one timestamp per trace), but holds
every trace — including one that finished in 50ms — for the full window.
**Rejected: quiet period after last span.** Adapts to real trace shape, but
has no upper bound: a heartbeat-emitting or retrying service can keep a
trace "alive" indefinitely, fighting the hard memory cap directly.
**Rejected as primary: root-seen + grace window.** The root is not
guaranteed to ever arrive (dropped in transit, or a genuinely async
fire-and-forget child that outlives it), so it needs the identical fallback
timer as the other options for that case — it only ever *shortens* the
common path, never replaces the backstop.

The early-exit condition costs nothing extra to check (it runs once per
span, on the already-buffered spans) and captures most of quiet-period's
benefit — most real traces are trees fully contained in their root's own
span — without quiet-period's unbounded-liveness risk.

### 2. Buffer eviction: forced early decision, not discard

**Decided.** When the in-flight buffer's hard cap (trace count or bytes) is
hit, the oldest trace is decided *right now* using whatever spans arrived so
far, through the identical decide-and-emit path a natural completion uses.
Nothing that entered the buffer is ever discarded outright.

**Rejected: oldest-first discard.** Simpler, but a genuine, permanent loss of
data that already arrived — the trace-buffer equivalent of phase 1's
rejected drop-oldest queue policy, for the same reason.

Both policies are implemented and selectable
(`TRACELENS_BUFFER_EVICTION`); both metrics
(`tracelens_forced_decisions_total`, `tracelens_evicted_traces_total`) exist
under either configuration, only one increments, matching phase 1's
"export every reason, touch every series at zero" pattern.

### 3. Policy engine: ordered chain, first-non-abstain-wins

**Decided.** `always_sample_errors`, `always_sample_slow` (threshold or
rolling p99), `rate_limiting`, `attribute_match`, `probabilistic`, composed
into `PolicyChain.Decide`: walk the configured list, the first policy that
returns a real verdict (not abstain) wins. An entirely-abstaining chain is
treated as `Drop`, not a silent 100% keep — a real deployment must end its
list with an explicit catch-all.

**Weight is 1/p, computed once per decision, never ad hoc.** `p=1` for every
deterministic keep (errors, slow-threshold, a matching attribute) — a trace
you always keep isn't a sample of a larger population, it *is* the
population, and upweighting it would overstate reality.  `probabilistic`
reports its configured rate. `rate_limiting` no longer reports a weight at
all — see decision 3a below for why that changed mid-implementation.

**Verified statistically** (`TestUpweightingRecoversTruePopulation`): at a
1% probabilistic rate over 100,000 synthetic traces, the raw sampled count
was 99.0% off the true population; the same traces upweighted by 1/p were
4.7% off. A second test mixing a deterministic population (errors, p=1) with
a 5%-probabilistic one recovered the true mixed total within 0.6%.
**Verified for the explicit "1% errors, retained at 100%" requirement**
(`TestErrorsAlwaysRetainedAtOverallLowRate`): 200/200 synthetic error traces
retained, non-error traces sampled at 5.0% against a configured 5% target.

### 3a. A real semantic bug found via the perf audit's benchmark: `rate_limiting` and `probabilistic` were mutually exclusive

**The bug.** `rate_limiting`'s original design *always* resolved (Sample or
Drop, reporting an empirical admit-ratio as its weight either way) — the
same "always resolves" property `probabilistic` has. In `PolicyChain.Decide`
(first-non-abstain-wins), two policies that both always resolve are mutually
exclusive by construction: whichever comes first in the configured list
makes the *other* completely unreachable, for **any** ordering. This was not
a benchmark artifact — it affected the real deployed `policies.yaml`, where
`attribute_match` and `probabilistic` sat after `rate_limiting` and were
therefore silently dead code the entire time this phase's chain was live.
It surfaced when a perf-audit subagent flagged that
`BenchmarkPolicyChainDecide` never touched `probabilistic` at all, and
tracing *why* led straight to the design flaw, not just the benchmark.

**The fix.** `rate_limiting` now **abstains** while token capacity is
available (consuming a token but deferring the actual keep/drop decision —
and its weight — to whichever policy runs next), and only actively
**vetoes** (Drop, certainty, `Probability=1`) once genuinely exhausted. This
makes it compose correctly as a capacity guard ahead of a baseline policy,
which is the role a rate limiter is actually meant to play, rather than a
second, competing decision-maker. Verified directly
(`TestRateLimiterComposesWithProbabilistic`): with capacity available,
`probabilistic` now decides and is named as the resolving policy; once
exhausted, `rate_limiting` vetoes before `probabilistic` ever runs.

**Accepted imprecision:** a trace that passes through while capacity is
available is weighted purely by whatever policy resolves it downstream, not
adjusted for the (normally small, under healthy load) probability that
capacity could have been exhausted. Computing that joint probability
correctly across an arbitrary chain is a harder problem this does not
attempt to solve, and is out of scope here.

**`deploy/tracelens/policies.yaml` was also reordered.** `attribute_match`
now runs **before** `rate_limiting`, not after: the comment above it
promises "always keep traces explicitly flagged for debugging, regardless of
the baseline rate" — with the old order, an exhausted rate-limiter bucket
could veto a debug-flagged trace before `attribute_match` ever got a chance,
breaking that promise outright. `TestDeployedConfigFilesAreValid` guards
this file's parseability; it does not (and cannot, without executing the
whole chain semantically) guard against a reordering bug like this one — a
lesson worth remembering.

**Verified live**, post-fix: `tracelens_decisions_total` on the running
demo stack showed `probabilistic` deciding both `sample` and `drop`
outcomes for the first time — before the fix, only `always_sample_errors`
and `rate_limiting` ever appeared, exactly as the mutual-exclusion bug
predicts.

### 4. Routing: `Router` (rendezvous hashing) as a tested primitive, Kafka partitioning stays the live mechanism

**Decided.** `internal/sampling.Router` implements genuine consistent hashing
(highest random weight / rendezvous), distinct from what Kafka partitioning
actually is: `hash(key) % N` sharding, which phase 1 already documented as
resize-*unstable* (changing partition count remaps nearly every key). Router
gives the formal guarantee sharding does not: adding one replica to a set of
N remaps only ~1/(N+1) of keys.

**Measured, not asserted:** `TestRouterResizeStability` grows a 4-replica set
to 5 and finds ~20.0% of keys remapped (theoretical 1/5 = 20%) against
naive `hash(key)%N` remapping ~75%+ on the *identical* resize, in the same
test, for a direct, honest comparison rather than two separate claims.

**A genuine bug was caught building Router itself:** the first hash
combination (`fnv1a(key || separator || replica)`, one FNV-1a stream) showed
one replica in a 4-way set winning ~2x its fair share in
`TestRouterSpread`. FNV-1a has weak avalanche on short, near-identical tails
— exactly what single-character replica names ("a","b","c","d") are, since
they differ by one bit in their last byte. Fixed by hashing key and replica
**independently** and combining through a SplitMix64 finalizer, which fully
avalanches the combination regardless of how correlated the inputs are;
re-ran spread and resize-stability tests clean afterward.

**Not wired into the live partition-assignment path.** Building a custom
static/deterministic partition assignor to replace Kafka's own consumer-group
rebalancing was considered and rejected: it would trade away Kafka's
automatic failure-driven rebalancing (a crashed replica's partitions
reassign automatically today) for a fragile, self-built alternative, for a
benefit — decoupling replica count from partition count — this phase does
not need. Router ships as a correct, tested, reusable primitive for the
scenario where that decoupling *is* needed later.

**What breaks with round-robin, specifically at the assembler layer**
(the requirement's explicit ask, since phase 1 only documented the ingest
side): each instance sees only a fragment of a trace and independently
judges it complete, so one trace becomes N partial trace records; the
instance that never received the `ERROR` span produces a false-negative on
`always_sample_errors`, invisibly, since nothing records that the full
trace ever existed; the in-flight buffer fills with fragments that never
complete, starving the forced-decision eviction path with garbage instead
of genuine backpressure signal; and every fragment computes its own
`sampling_weight` against the wrong denominator, so any aggregate over
sampled data silently double-counts.

**Verified against a real broker with 3 concurrent consumer-group members**
(`TestConsumerNoTraceSplitAcrossThreeInstances`, new this phase — phase 1's
own test only proved the *producer* side routes correctly; this proves the
*consumer* side holds once the partition set is actually spread across
multiple live processes, which is the layer that matters since the
in-flight buffer lives in one process's memory): 300 traces produced across
12 partitions, 3 independent `Consumer` instances in one group, zero traces
observed by more than one instance.

### 5. Late spans: bounded decided-cache, attach-if-sampled / drop-if-not

**Decided.** A separate, bounded cache (`Buffer.decided`, capped by count and
TTL, same eviction discipline as the in-flight buffer) remembers every
trace's outcome after it decides. A span arriving for a trace not in the
in-flight buffer is checked against this cache: present and sampled →
attach directly to storage carrying the trace's *original* weight (siblings
must share one weight or per-trace aggregates break); present and dropped →
drop, counted; absent from *both* structures (cache itself aged out, or
genuinely new) → falls through to fresh assembly. This last case is an
honest, bounded-memory trade-off: a span late enough to outlive the decided
cache's retention window starts a brand-new, single-span "trace" with its
own fresh decision, rather than growing the cache without bound to catch an
arbitrarily late straggler.

`tracelens_late_spans_total{outcome}` distinguishes `attached` from
`dropped`. Exercised directly by `TestAssemblerLateSpanWeight` and
`TestBufferLateSpans`; the late-span RATE under sustained load is what
`TestErrorsAlwaysRetainedAtOverallLowRate`'s 20,000-trace run implicitly
measures as zero (buffer capacity was never pressured in that test), and
what the live demo stack's `tracelens_late_spans_total{outcome="attached"}`
counter tracks in production — it moved (1987 observed during one verification
run), consistent with real out-of-order network delivery rather than a
synthetic worst case.

### 6. Offset-commit safety under buffered, delayed decisions

**The problem, precisely.** Phase 1 could commit a Kafka offset the instant
its batch was durably written, because decode → write was immediate. Now a
span can sit in the trace buffer for the full completion window before its
trace resolves. Committing an offset as soon as a span is merely *buffered*
— not once its trace *decides and is durably written* — would mean a crash
mid-window loses everything still in memory, with no redelivery, since the
offset already advanced. Exactly the silent loss principle 1 forbids.

**Decided: `OffsetWatermark`**, one per-partition min-heap of (first-seen
offset, trace_id) for currently-undecided traces. `pipeline.Consumer` gained
an additive `RunWithCommitGate` (existing `Run`/`Handler` untouched, zero
regression risk to phase 1's tests) that commits, per partition, up to
`min(highest offset this cycle, the watermark's floor if lower)` — so a
partition with an undecided trace holds its commit at exactly that trace's
first offset, while every other partition proceeds normally. **`finishTrace`
only resolves the watermark once the actual write is confirmed durable**,
not once `emit`/`lateAttach` is merely called — `EmitFunc` and
`LateAttachFunc` both return an error for exactly this reason. On a write
failure, the decision is deliberately left **unrecorded** too (not just the
watermark unresolved): since the trace has already left the buffer by then,
a Kafka redelivery of the same spans finds nothing in-flight and nothing
decided, and restarts fresh assembly from scratch — safe specifically
because probabilistic sampling is deterministic by trace_id, so a
redelivered trace gets the identical verdict, not a second, inconsistent
one. Locked down directly by
`TestAssemblerWithholdsWatermarkOnWriteFailure`.

**Two real bugs found building this, both empirically, via a broker-backed
test that intentionally used a live Redpanda rather than a mock:**

- **A Go map zero-value sentinel bug.** The first `pending[tp]` tracking
  used `if r.Offset > pending[tp]` to decide whether to record a new
  high-water offset — but a *fresh* partition's legitimate first offset is
  `0`, identical to the map's zero-value default for "not present". The
  condition `0 > 0` is false, so the very first offset on any partition was
  silently never recorded. Fixed by checking presence explicitly
  (`if cur, ok := pending[tp]; !ok || r.Offset > cur`).
- **`PollFetches` does not wake up periodically on its own.** The retry
  design assumed an idle poll would return empty every so often, giving the
  ceiling a chance to re-check a previously-withheld partition. In fact
  `PollFetches` blocks until either new data arrives on *any* subscribed
  partition or the caller's context is done — `kgo.FetchMaxWait` only
  bounds one broker-side fetch request/response cycle, not the client's own
  idle-wait behavior. Fixed by wrapping each poll in a
  `context.WithTimeout(ctx, commitRetryInterval)` and treating that
  timeout's `context.DeadlineExceeded` as "nothing new, but still recheck
  the ceiling" rather than a fetch error. Both bugs were caught by
  `TestConsumerCommitGateWithholdsAndReleases` genuinely hanging/failing
  against a real broker, not by code review — exactly the kind of subtle,
  timing-dependent bug a mock would not have exposed.

### 7. Drain: fixed-depth tree, eager numeric wildcarding, LRU template cap

**Decided**, implemented from scratch, no library. Tree: root → level 1 keyed
by token count → levels 2..depth-1 keyed by leading token (a numeric-looking
token routed to a shared wildcard child immediately, per the approved
choice, rather than branching the tree on what's almost certainly an id) →
leaf holding a small list of candidate clusters, matched by
position-wise similarity (`wildcard` counts as an automatic match), merged
(any disagreeing position generalizes to `<*>`, so templates only ever
widen, never narrow) or created fresh with the next dense id.

**Verified against a known-shape corpus**
(`TestDrainRecoversKnownTemplateCount`): a corpus built from exactly 5
known shapes, instantiated with varying parameters, recovers to exactly 5
templates regardless of corpus volume (50 vs 5000 instances of one shape:
template count unchanged). **Verified live**, not just synthetically: 12,000
real OTLP log records sent to the running demo stack across 4 services
extracted exactly the 5 template shapes the generator actually used,
correctly generalizing e.g. `"payment declined for account 4919 code 0"`
into `payment declined for account <*> code <*>`.

**`template_id` is dense and process-scoped, matching the T64 codec
constraint phase 1 already recorded**, not globally coordinated across
replicas or restarts — a documented, accepted gap (see §10), not an
oversight, given the actually-deployed topology is one assembler replica.

**A cheap, real correctness gap found and fixed before it shipped:**
persisting a template to the `log_templates` dictionary only on *creation*
(`IsNew`) would leave the dictionary holding stale, more-literal text
forever once a later log widens that template further. Fixed by tracking
whether `merge()` actually changed a template (a `Changed` bool alongside
`IsNew` on `Match`) and upserting on either — `log_templates` is a
`ReplacingMergeTree` keyed on `template_id` specifically so a repeat upsert
with fresher text is the correct, cheap resolution. Caught by a test
(`TestTemplateStoreReportsUpsertOnCreateAndOnWiden`) written against the
*intended* behavior, before any live traffic exposed it.

**Body retention: kept, not dropped, by explicit choice.** `template_id`
and `params` are populated unconditionally on every log record, satisfying
"store template_id+params, not the rendered string" as written — but the
raw `body` column is *also* kept, rather than cleared, because phase 1
already built a `tokenbf_v1` free-text-search index specifically against
it, and dropping body would silently regress the log explorer's search to
satisfy a phrase in this prompt that didn't ask for that trade explicitly.
The storage saving templating *would* achieve is measured and recorded
below regardless, so the decision to actually drop body later can be made
from real numbers.

**Measured storage saving**
(`TestTemplateStoreMeasuresStorageSaving`, 10,000 repetitive synthetic log
lines): raw body bytes would cost 783,425 bytes; `template_id`+`params` for
the identical lines costs 283,408 bytes — a **2.76× saving**, before
ClickHouse's own column compression is even applied on top.

### 7a. Cache-only performance bug found via benchmarking: `HyperLogLog.Estimate()` rescanned all 16,384 registers on every attribute occurrence

**Measured, before any fix:** `BenchmarkCardinalityGuardEnforce` =
16,811 ns/op; `BenchmarkHyperLogLogEstimate` (a bare register scan) =
16,880 ns/op — nearly identical, proving `Enforce`'s cost was **entirely**
the register scan, called on every single attribute key/value pair rather
than only when checking for a breach. `BenchmarkCardinalityGuardApplyToAttributes`
(a realistic 5-attribute span) measured 83,100 ns/op as a direct
consequence.

**Fixed by caching**, not by approximating: `Estimate()` is a pure function
of register content, so if `Add()` reports that an observation did **not**
change any register (already-seen value), the cached estimate from the
last time a register *did* change is bit-for-bit identical to what a fresh
scan would produce — this is exact, not a looser approximation.
`HyperLogLog.Add`/`Merge` now report whether they changed anything;
`CardinalityTracker` only recomputes on a true change.

**Measured after:** `BenchmarkCardinalityGuardEnforce` = 20.32 ns/op (a
**~827× improvement**); `BenchmarkCardinalityGuardApplyToAttributes` =
271.1 ns/op (a **~306× improvement**). `BenchmarkHyperLogLogEstimate`
itself is unchanged (~16µs) — the raw primitive's cost didn't change, the
caller now simply avoids paying it needlessly.

### 8. Cardinality control: HyperLogLog, `bucket` as the default action

**Decided.** One `HyperLogLog` per attribute key (constant ~16KB regardless
of true cardinality — an exact set's memory grows linearly with true
cardinality, exactly what this exists to prevent). On budget breach:
`drop` (remove the key entirely), `bucket` (replace the value with
`hash(value) % N`, retaining a bounded amount of correlation signal — the
same generalize-rather-than-discard idea Drain uses for its own templates),
or `keep_and_alert` (leave the value untouched, only count the breach).
All three ship as configurable per-key overrides regardless of the default;
`bucket` was chosen as the default because CLAUDE.md's principle 4 names
only "dropped or bucketed" as valid enforcement outcomes, reading
`keep_and_alert` as a legitimate opt-in transitional/diagnostic mode rather
than a permanent default, and because it degrades more gracefully than an
outright drop for a key whose value still carries *some* correlation value.

**Verified live**, not just in unit tests: sending 12,000 real log records
with 5,000 distinct synthetic emails against a configured `customer.email`
budget of 1,000 triggered 1,879 recorded breaches
(`tracelens_cardinality_breaches_total{key="customer.email"}`) on the
running demo stack — the mechanism engages under real traffic exactly as
designed, not only against synthetic unit-test input.

### 8a. Two more unbounded-map bugs found via the same perf audit: `rateLimiter.buckets` and `CardinalityTracker.sketches`

Every other stateful map in this codebase (`Buffer.inflight`/`decided`,
Drain's `clusters`) already has an explicit cap, LRU eviction, and a
counter. Two did not:

- **`rateLimiter.buckets`**, keyed by `service_name`, grew once per distinct
  service ever seen, forever, under one non-sharded lock, with zero
  visibility. If `service.name` ever carried a per-tenant/per-pod identifier
  — precisely the anti-pattern `CardinalityGuard` exists elsewhere to
  police — this was an unbounded, silent memory leak.
- **`CardinalityTracker.sketches`**, keyed by attribute *key* (not value —
  values were already correctly bounded via the HLL itself), had the
  identical shape of gap: lower risk in practice, since attribute keys are
  normally a small, schema-like set, but a producer varying key *names*
  dynamically would grow this map by 16KB per new key forever.

**Fixed identically**: both capped (`maxTrackedServices`,
`maxTrackedKeys`, 10,000 each) with LRU eviction (a generation counter
stamped on every touch, same mechanism Drain's own `evictLRU` already
used) and a counter/gauge each
(`tracelens_rate_limiter_services_{tracked,evicted_total}`,
`tracelens_cardinality_keys_evicted_total`).

### 8b. A third perf bug: `Assembler.Ingest` allocated a fresh closure on every span

`a.buffer.Ingest(s, a.forceDecideFn())` called `forceDecideFn()` — a
function returning a new closure — on **every** ingested span, regardless
of whether capacity eviction ever fires. The closure itself is stateless
(closes only over `a`), so one instance serves the assembler's entire
lifetime. Fixed by binding it once in `NewAssembler`
(`a.forceDecide = a.doForceDecide`) and reading the field in `Ingest`
instead. Verified via allocation profiling
(`go tool pprof -alloc_objects`) that `Assembler.Ingest` itself now
allocates essentially nothing (3,738 objects across a 1,000,000-iteration
benchmark run) — the aggregate "3 allocs/op" `go test -benchmem` still
reports for `BenchmarkAssemblerIngest` is unchanged only because it
coincidentally lines up with a *different*, legitimate, unavoidable
allocation (`OffsetWatermark.Track`'s per-new-trace tracking struct); the
headline number staying flat while profiling proved the fix is a case
worth recording, since the benchmark alone would have looked like "no
improvement" despite genuinely removing a wasteful, per-item allocation.

### 8c. A fourth perf bug: Drain had no per-leaf cluster cap

`MaxTemplates` bounds the whole tree, but not clusters *at one leaf* — and
`findOrCreate`'s similarity scan is O(clusters at that leaf). Global LRU
eviction protects a frequently-touched ("hot") leaf at the expense of
others, letting one leaf's list grow toward the entire tree-wide budget.
**Measured**: a leaf holding 2,000 same-shaped-but-distinct clusters cost
**138×** a leaf holding one (57,532 ns/op vs 416.5 ns/op per `Parse` call).
Fixed with a separate `MaxClustersPerLeaf` cap (default 200, LRU-evicted
independently of the tree-wide cap) — **measured after: 9,188 ns/op, a
6.5× improvement** on the identical stress case. Backward compatible by
construction: `MaxClustersPerLeaf: 0` (the zero value, what every existing
test already used) disables the per-leaf cap entirely, verified by
`TestDrainPerLeafClusterCap`'s explicit "disabled" case.

### 9. Log-to-trace correlation and per-severity retention

Correlation (`trace_id`/`span_id` on `LogRow`, the `idx_trace_id` bloom
filter) already existed structurally from phase 1 — Drain only tokenizes
`body`, leaving these untouched. **New this phase**: per-severity TTL on
`tracelens.logs` (migration 0006), layered under the *same* unconditional
recompress/cold-tier clauses from phase 1: TRACE/DEBUG (severity < 9)
delete after 1 day, INFO/WARN (9–16) after 14 days (unchanged from phase
1's flat default), ERROR/FATAL (≥ 17) after 90 days. A low-severity row's
1-day delete fires before it would ever reach the 7-day cold-tier move,
which is fine — there is no reason to tier a row into cold storage moments
before deleting it.

### 10. Accepted gaps, phase 2

- **`template_id` is process-scoped**, not coordinated across assembler
  replicas or process restarts. Documented in `internal/logs/doc.go` and
  above (§7) rather than building a distributed sequence allocator the
  actual deployed topology (one replica) does not need yet.
- **`Router` is not wired into the live partition-assignment path** (§4) —
  a deliberate scope decision, not an oversight, given the risk of trading
  away Kafka's automatic rebalance-on-failure for a self-built static
  assignor this phase does not need.
- **Rate-limiting's weight for admitted-under-capacity traces does not
  account for the (usually small) probability that capacity could have
  been exhausted** (§3a) — a real, accepted statistical imprecision, not a
  bug, distinct from the mutual-exclusion bug that *was* fixed.
- **A verification tool's own bug, recorded because the debugging process
  is worth remembering, not because it is a TraceLens defect:** the
  one-off script used to push manual OTLP log traffic for end-to-end
  verification reused the *same* `[]*logspb.LogRecord` slice across four
  different `ResourceLogs` parents when hand-constructing a protobuf
  message tree. This violates protobuf-go's assumption that a message
  tree is a tree, not a DAG, and produced a wire-level artifact where only
  one resource's data actually marshaled — `SplitLogs`, `EnqueueBatch`,
  and the batcher were all independently re-verified correct in isolation
  (a small reproduction test, then a full receiver-to-batcher pipeline
  test with a mock sink) before concluding the bug was in the throwaway
  script, not the shipped ingest path. The lesson: when hand-building a
  protobuf message tree for a test/verification tool, never let two parent
  messages share the same child message pointer.

Post-phase live verification, all on the running demo stack rebuilt with
every fix above: 1.2M+ spans processed with weighted `sampling_weight`
values observed spanning the full 1.0–3.0+ range (proving the tail
sampler's rate-limiting admit-ratio weighting is live, not just
unit-tested); 12,000 real log records templated into exactly 5 clusters;
1,879 cardinality breaches recorded on a deliberately-overloaded attribute
key; `probabilistic` confirmed reachable and deciding after the mutual-
exclusion fix, where before the fix it never appeared in
`tracelens_decisions_total` at all.
