# Benchmarks

Measured, not assumed — every number below came from an actual run against
a real ClickHouse instance, not a projection from reading the code.

---

## Query optimiser passes, on vs off, at 10M+ spans

**Method** (`cmd/querybench`, `make querybench`): 10,000,000+ synthetic
spans bulk-inserted directly through `storage.Writer` (bypassing OTLP/Kafka
— this benchmarks the query engine, not ingestion), spread across 10
services × 5 operations over a 7-day window, with a 20% synthetic error
rate and a small `span_attributes`/`resource_attributes` payload per row.
For each pass, the identical logical plan is compiled twice —
`query.Optimize` (pass on) vs `query.OptimizeExcept(plan, passName)` (pass
off, every *other* pass still applied — isolating exactly one pass's effect
rather than conflating all five into one "optimised vs not" number). Each
variant runs 5 times against the live cluster; the **median** is reported
(the very first run of any query is slower due to ClickHouse's page cache
being cold, and a mean would be dragged around by that one outlier).
**Bytes read** comes from ClickHouse's own native-protocol progress
callback (`clickhouse.WithProgress`, summed across the query's progress
packets) — an actual execution measurement, not an `EXPLAIN ESTIMATE`
prediction.

Every pass's presence/absence was independently confirmed to produce
textually different generated SQL before this table was ever run
(`internal/query/ablation_test.go`) — a pass whose SQL is identical on vs
off cannot possibly show a real measured difference, so that check ran
first, deliberately, not as an afterthought.

| Pass | Query | Bytes read (off) | Bytes read (on) | Reduction | Latency (off) | Latency (on) | Speedup |
|---|---|---|---|---|---|---|---|
| Constant fold | `{} \| duration > 500ms \| duration > 300ms` | 154.90 MiB | 154.90 MiB | 1.0× | 745 ms | 744 ms | 1.00× |
| Predicate pushdown | `{service="svc-3"} \| duration > 500ms` | 20.00 MiB | 14.77 MiB | 1.4× | 308 ms | 363 ms | 0.85× |
| Partition pruning | `{service="svc-3"} since 1h` | 187.96 MiB | 14.48 MiB | **13.0×** | 1.965 s | 377 ms | **5.22×** |
| Limit pushdown | `{} \| limit 100` | 11.06 MiB | 10.61 MiB | 1.0× | 166 ms | 80 ms | **2.07×** |
| Projection pushdown | `{service="svc-3"} \| count by (operation)` | 2.11 MiB | 2.11 MiB | 1.0× | 33 ms | 30 ms | 1.09× |

*(10,367,910 total rows in `tracelens.spans` at measurement time; Apple
silicon Docker Desktop, single-node ClickHouse 24.8.)*

### Reading these numbers honestly, pass by pass

- **Partition pruning is the clear, dramatic win** — 13× fewer bytes read
  translates almost linearly into a 5.2× latency improvement. This is
  exactly the expected result for a time-partitioned table: without the
  half-open timestamp bound, the query reads all 7 days instead of the
  requested 1 hour.
- **Constant folding shows zero measured bytes/latency effect here**, and
  that is the honest result, not a failed pass. `duration_ns` carries no
  skip index (phase 1 measured and removed one — `EXPLAIN indexes=1` showed
  it pruned zero granules) and is not part of the sort key, so tightening
  `duration>500ms AND duration>300ms` down to one comparison changes how
  many *comparisons* run per row, not how many *rows* are read. Its real
  value is the correctness/plan-simplicity guarantee (a contradictory range
  folds to "no rows" instead of scanning for an impossible answer), which
  this benchmark shape doesn't exercise.
- **Predicate pushdown reduced bytes read (1.4×) but did not show a
  latency win at this scale** — if anything, a slightly slower median (363ms
  vs 308ms, a 0.85× "speedup"). Measured and reported as-is rather than
  rounded into a cleaner story: at ~15-20 MiB of actual scan work, fixed
  per-query overhead (network round trip, query parse/plan) dominates wall
  clock more than a 5 MiB byte-read difference does, so the real, structural
  SQL-level effect (proven by the ablation test) doesn't reliably surface as
  a latency win until the scan itself is the bottleneck — which is exactly
  what partition pruning's row above demonstrates at a larger byte delta.
- **Limit pushdown shows a real 2.07× latency win with almost no bytes-read
  change.** Consistent, not contradictory: ClickHouse's progress reporting
  is per-block, so an early-exit scan can stop issuing further reads (the
  latency win) while still reporting a similar bytes-read figure for the
  blocks it did touch before stopping.
- **Projection pushdown's measured effect (1.0×, ~1.09× latency) is smaller
  than the Schema section's own "columnar storage's whole point" framing
  predicts — and the reason is a real limitation of this benchmark's
  synthetic data, not the pass.** The seed data populates a handful of
  small `span_attributes`/`resource_attributes` string entries per row but
  leaves the event and link arrays empty, so there is little of the wide,
  unused payload the pass is specifically designed to skip. The pass's
  effect is still proven structurally — `TestProjectionPushdownRestrictsColumnsForAggregateQuery`
  and the ablation test both confirm the aggregate query reads exactly
  `{service_name, span_name, timestamp, sampling_weight}` instead of every
  column — but demonstrating a large measured win here would need a richer
  synthetic dataset (populated span events, multiple links) than this run
  used. Recorded as an accepted gap in this benchmark's methodology, not
  papered over.

---

## Anomaly detector: precision/recall on injected anomalies

See `internal/anomaly/detector_test.go`
(`TestDetectorPrecisionRecallOnInjectedAnomalies`). 56 days of synthetic,
seasonally-varying (business-hours-boosted) latency at 5-minute resolution
warm every (hour-of-day, day-of-week) baseline bucket to the configured
minimum sample count; day 57 injects six 6×-normal anomaly windows spread
across different hours, with everything else ordinary.

| Metric | Value |
|---|---|
| Points evaluated (day 57) | 288 |
| Injected-anomalous points | 30 |
| True positives | 25 |
| False positives | 6 |
| False negatives | 5 |
| **Precision** | **0.806** |
| **Recall** | **0.833** |

The gap from 1.0 on both is expected and by design, not noise: hysteresis
requires two consecutive above-threshold ticks before firing and two
consecutive below-threshold ticks before clearing, which by construction
costs the first tick of each anomaly window as a false negative and can
carry firing state one tick past a window's end as a false positive — the
explicit trade this project chose (see docs/DECISIONS.md) to prevent a
noisy single point from flapping an alert.

---

## Hot-path benchmarks (phase 1)

See [README.md's Hot path section](../README.md#hot-path) for the ingest
splitter/enqueue benchmarks — unchanged by this phase, restated here only
for a single point of reference to every recorded number in the project.
