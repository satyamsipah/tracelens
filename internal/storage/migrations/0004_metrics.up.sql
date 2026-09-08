-- 0004: metrics and the 1-minute rollup.

CREATE TABLE IF NOT EXISTS tracelens.metrics
(
    timestamp     DateTime64(9)   CODEC(DoubleDelta, ZSTD(1)),
    service_name  LowCardinality(String) CODEC(ZSTD(1)),
    metric_name   LowCardinality(String) CODEC(ZSTD(1)),
    metric_type   Enum8('unspecified'=0,'gauge'=1,'sum'=2,'histogram'=3,
                        'exponential_histogram'=4,'summary'=5) CODEC(ZSTD(1)),

    -- Gorilla XORs each float against its predecessor and emits only the
    -- differing bits -- a few bits per point for a slowly-changing series.
    -- This works ONLY if consecutive rows belong to the SAME series, which is
    -- exactly why labels_hash sits ahead of timestamp in ORDER BY. The codec
    -- choice and the sort key are one decision, not two.
    value         Float64         CODEC(Gorilla, ZSTD(1)),

    labels        Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    -- Computed by the writer rather than MATERIALIZED here, so the identical
    -- hash can key the Kafka partitioner: one definition of "same series"
    -- shared across the broker and the store.
    labels_hash   UInt64          CODEC(ZSTD(1))
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(timestamp)
ORDER BY (service_name, metric_name, labels_hash, timestamp)
TTL toDateTime(timestamp) + INTERVAL 2 DAY RECOMPRESS CODEC(ZSTD(6)),
    toDateTime(timestamp) + INTERVAL 7 DAY DELETE
SETTINGS index_granularity = 8192,
         storage_policy = 'tiered',
         ttl_only_drop_parts = 1,
         non_replicated_deduplication_window = 1000;


CREATE TABLE IF NOT EXISTS tracelens.metrics_rollup_1m
(
    timestamp    DateTime        CODEC(DoubleDelta, ZSTD(1)),
    service_name LowCardinality(String) CODEC(ZSTD(1)),
    metric_name  LowCardinality(String) CODEC(ZSTD(1)),
    labels_hash  UInt64          CODEC(ZSTD(1)),
    labels       Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    -- SimpleAggregateFunction where the merge is associative and stateless:
    -- stored as the plain value, so T64/Gorilla still apply to it.
    count        SimpleAggregateFunction(sum, UInt64)  CODEC(T64, ZSTD(1)),
    sum          SimpleAggregateFunction(sum, Float64) CODEC(Gorilla, ZSTD(1)),
    min          SimpleAggregateFunction(min, Float64) CODEC(Gorilla, ZSTD(1)),
    max          SimpleAggregateFunction(max, Float64) CODEC(Gorilla, ZSTD(1)),
    -- Opaque serialised t-digest. No structural codec can read into it, so
    -- ZSTD alone -- stated explicitly rather than left to the default.
    quantiles    AggregateFunction(quantilesTDigest(0.5, 0.9, 0.99), Float64)
                                 CODEC(ZSTD(1))
)
ENGINE = AggregatingMergeTree
PARTITION BY toDate(timestamp)
ORDER BY (service_name, metric_name, labels_hash, timestamp)
-- Rollups outlive raw points by design: this is the series you keep for
-- capacity trends long after the raw resolution is gone.
TTL timestamp + INTERVAL 90 DAY DELETE
SETTINGS index_granularity = 8192,
         storage_policy = 'tiered',
         ttl_only_drop_parts = 1;


CREATE MATERIALIZED VIEW IF NOT EXISTS tracelens.metrics_rollup_1m_mv
TO tracelens.metrics_rollup_1m
AS
SELECT
    toStartOfMinute(timestamp)                       AS timestamp,
    service_name,
    metric_name,
    labels_hash,
    any(labels)                                      AS labels,
    toUInt64(count())                                AS count,
    sum(value)                                       AS sum,
    min(value)                                       AS min,
    max(value)                                       AS max,
    quantilesTDigestState(0.5, 0.9, 0.99)(value)     AS quantiles
FROM tracelens.metrics
GROUP BY timestamp, service_name, metric_name, labels_hash;
