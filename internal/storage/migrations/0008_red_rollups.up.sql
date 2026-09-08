-- 0008: RED metrics (rate, errors, duration) pre-aggregated at 1m/5m/1h.
--
-- Storage-vs-query-cost trade-off (see docs/DECISIONS.md for the full
-- writeup): every rollup level is an ADDITIONAL copy of the same
-- information, so this trades storage for query latency and scan cost on
-- the dashboard's hottest query shape (service+operation RED panels, which
-- would otherwise re-scan raw spans on every page load). The three levels
-- exist because one retention/resolution trade-off can't serve both a
-- "last 15 minutes, per-minute resolution" dashboard and a "last 90 days,
-- trend" dashboard: 1m is precise but expensive to keep long, 1h is cheap
-- to keep long but blind to minute-level spikes.
--
-- Each level is a rollup of the level below it (spans -> 1m -> 5m -> 1h),
-- not independently re-aggregated from raw spans -- re-deriving 1h directly
-- from spans would mean scanning the same rows three times on every insert.
-- The *MergeState combinators (sumMergeState, quantilesTDigestWeightedMergeState)
-- exist precisely to re-aggregate an already-aggregated STATE into a coarser
-- one without ever touching the original rows again.

CREATE TABLE IF NOT EXISTS tracelens.red_rollup_1m
(
    bucket             DateTime CODEC(DoubleDelta, ZSTD(1)),
    service_name       LowCardinality(String) CODEC(ZSTD(1)),
    operation          LowCardinality(String) CODEC(ZSTD(1)),
    call_weight        AggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    error_weight       AggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    duration_quantiles AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), UInt64, UInt64) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (service_name, operation, bucket)
-- Finest grain, kept shortest: a per-minute dashboard is only ever asking
-- about "right now", never about last month.
TTL bucket + INTERVAL 7 DAY
SETTINGS index_granularity = 8192,
         ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS tracelens.red_rollup_1m_mv
TO tracelens.red_rollup_1m
AS
SELECT
    toStartOfMinute(timestamp)                                                        AS bucket,
    service_name,
    span_name                                                                         AS operation,
    sumState(sampling_weight)                                                         AS call_weight,
    sumState(sampling_weight * (status_code = 'error'))                               AS error_weight,
    quantilesTDigestWeightedState(0.5, 0.95, 0.99)(duration_ns, toUInt64(round(sampling_weight))) AS duration_quantiles
FROM tracelens.spans
GROUP BY bucket, service_name, operation;

CREATE TABLE IF NOT EXISTS tracelens.red_rollup_5m
(
    bucket             DateTime CODEC(DoubleDelta, ZSTD(1)),
    service_name       LowCardinality(String) CODEC(ZSTD(1)),
    operation          LowCardinality(String) CODEC(ZSTD(1)),
    call_weight        AggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    error_weight       AggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    duration_quantiles AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), UInt64, UInt64) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (service_name, operation, bucket)
TTL bucket + INTERVAL 30 DAY
SETTINGS index_granularity = 8192,
         ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS tracelens.red_rollup_5m_mv
TO tracelens.red_rollup_5m
AS
SELECT
    toStartOfFiveMinute(bucket)                              AS bucket,
    service_name,
    operation,
    sumMergeState(call_weight)                                AS call_weight,
    sumMergeState(error_weight)                               AS error_weight,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(duration_quantiles) AS duration_quantiles
FROM tracelens.red_rollup_1m
GROUP BY bucket, service_name, operation;

CREATE TABLE IF NOT EXISTS tracelens.red_rollup_1h
(
    bucket             DateTime CODEC(DoubleDelta, ZSTD(1)),
    service_name       LowCardinality(String) CODEC(ZSTD(1)),
    operation          LowCardinality(String) CODEC(ZSTD(1)),
    call_weight        AggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    error_weight       AggregateFunction(sum, Float64) CODEC(ZSTD(1)),
    duration_quantiles AggregateFunction(quantilesTDigestWeighted(0.5, 0.95, 0.99), UInt64, UInt64) CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(bucket)
ORDER BY (service_name, operation, bucket)
-- Coarsest grain, kept longest: this is what a "last 90 days" trend panel
-- or the anomaly detector's seasonal baseline reads, and it costs the least
-- per row of wall-clock time covered.
TTL bucket + INTERVAL 400 DAY
SETTINGS index_granularity = 8192,
         ttl_only_drop_parts = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS tracelens.red_rollup_1h_mv
TO tracelens.red_rollup_1h
AS
SELECT
    toStartOfHour(bucket)                                     AS bucket,
    service_name,
    operation,
    sumMergeState(call_weight)                                AS call_weight,
    sumMergeState(error_weight)                               AS error_weight,
    quantilesTDigestWeightedMergeState(0.5, 0.95, 0.99)(duration_quantiles) AS duration_quantiles
FROM tracelens.red_rollup_5m
GROUP BY bucket, service_name, operation;
