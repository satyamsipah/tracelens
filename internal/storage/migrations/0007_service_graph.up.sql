-- 0007: service dependency graph, computed incrementally.
--
-- The join a service graph needs -- a child span to its PARENT span, to know
-- who called whom -- cannot be done as an ordinary ClickHouse materialized
-- view over tracelens.spans: an MV fires per insert BLOCK, and a trace's
-- parent and child spans are not guaranteed to land in the same block, or
-- even the same INSERT. Re-deriving the join at query time (a self-join
-- over the full spans table) is exactly the "full scan per request" this
-- requirement rules out.
--
-- The assembler already has the answer for free: it builds the full
-- parent-child tree (internal/sampling.AssembledTree) to make the tail-
-- sampling decision, while every span for a trace is still in memory
-- together. internal/sampling.ExtractServiceEdges walks that tree once per
-- decided trace and emits one already-joined (caller_service,
-- callee_service) row per cross-service call. This table exists to receive
-- those pre-joined rows -- ClickHouse's job is then only the cheap,
-- genuinely-incremental part: aggregating them.
CREATE TABLE IF NOT EXISTS tracelens.service_edges_raw
(
    timestamp       DateTime64(9)         CODEC(DoubleDelta, ZSTD(1)),
    caller_service  LowCardinality(String) CODEC(ZSTD(1)),
    callee_service  LowCardinality(String) CODEC(ZSTD(1)),
    -- Not monotonic (same shape as spans.duration_ns) -- T64 crops the
    -- provably-unused high bits, Delta would encode noise.
    duration_ns     UInt64                CODEC(T64, ZSTD(1)),
    is_error        UInt8                 CODEC(T64, ZSTD(1)),
    sampling_weight Float64               CODEC(ZSTD(1))
)
ENGINE = MergeTree
PARTITION BY toDate(timestamp)
ORDER BY (caller_service, callee_service, timestamp)
TTL toDateTime(timestamp) + INTERVAL 3 DAY
-- Same insert_deduplication_token mechanism as spans/logs/metrics
-- (DECISIONS.md Sec 6): a redelivered decided-trace flush after a crash
-- reuses the trace-derived token, and WITHOUT this setting the token is
-- silently ignored -- ClickHouse still discards the duplicate insert as a
-- no-op, since edge rows carry no natural identity column of their own to
-- dedup on later.
SETTINGS index_granularity = 8192,
         ttl_only_drop_parts = 1,
         non_replicated_deduplication_window = 1000;

-- The aggregated graph an EXPLAIN-free query reads. AggregateFunction state
-- columns let the 1-minute buckets this MV writes be re-merged into any
-- coarser window (a day, a week) at query time via the Merge combinator,
-- without re-scanning service_edges_raw.
CREATE TABLE IF NOT EXISTS tracelens.service_edges
(
    bucket             DateTime CODEC(DoubleDelta, ZSTD(1)),
    caller_service     LowCardinality(String) CODEC(ZSTD(1)),
    callee_service     LowCardinality(String) CODEC(ZSTD(1)),
    call_weight        AggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    error_weight       AggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    -- One state serves p50/p95/p99 together (quantilesTDigestWeighted takes
    -- a level LIST and returns an array), rather than three separate
    -- AggregateFunction columns each re-scanning the same rows. The weight
    -- type is UInt64, not Float64: ClickHouse's weighted-quantile family
    -- requires an unsigned-integer weight (a "repeat count"), which the MV
    -- below satisfies by rounding sampling_weight to the nearest integer.
    duration_quantiles AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), UInt64, UInt64) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (caller_service, callee_service, bucket)
TTL bucket + INTERVAL 90 DAY
SETTINGS index_granularity = 8192,
         ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS tracelens.service_edges_mv
TO tracelens.service_edges
AS
SELECT
    toStartOfMinute(timestamp)                                             AS bucket,
    caller_service,
    callee_service,
    sumState(sampling_weight)                                              AS call_weight,
    sumState(sampling_weight * is_error)                                   AS error_weight,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_ns, toUInt64(round(sampling_weight))) AS duration_quantiles
FROM tracelens.service_edges_raw
GROUP BY bucket, caller_service, callee_service;
