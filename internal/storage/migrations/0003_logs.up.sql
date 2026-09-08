-- 0003: logs.

CREATE TABLE IF NOT EXISTS tracelens.logs
(
    timestamp        DateTime64(9)   CODEC(DoubleDelta, ZSTD(1)),
    -- Zero-filled when the record has no trace context, which compresses away.
    trace_id         FixedString(16) CODEC(ZSTD(1)),
    span_id          FixedString(8)  CODEC(ZSTD(1)),
    service_name     LowCardinality(String) CODEC(ZSTD(1)),
    -- OTLP severity is 1..24, so T64 crops 56 provably-zero bits per value.
    severity_number  UInt8           CODEC(T64, ZSTD(1)),
    severity_text    LowCardinality(String) CODEC(ZSTD(1)),
    -- The bulk of this table. Log bodies are near-duplicates of one another
    -- and ZSTD eats them. This is the column the RECOMPRESS TTL exists for.
    body             String          CODEC(ZSTD(1)),

    -- Filled by Drain templating in a later phase, 0 meaning untemplated.
    -- MUST be a DENSE dictionary id, not a hash: a hash has random high bits
    -- and T64 would do nothing, whereas a dense id makes T64 near-total. This
    -- constrains the phase-2 Drain implementation and is recorded here so the
    -- constraint is visible at the point it binds.
    template_id      UInt32          CODEC(T64, ZSTD(1)),
    params           Array(String)   CODEC(ZSTD(1)),
    log_attributes   Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -- Serves: trace -> logs correlation on the trace detail page.
    INDEX idx_trace_id trace_id TYPE bloom_filter(0.01) GRANULARITY 1,
    -- Serves: "errors only". A set index stores the distinct severities per
    -- granule block, so granules holding no ERROR are skipped outright.
    -- Chosen over minmax because severity is categorical, not ordinal-ranged.
    INDEX idx_severity severity_number TYPE set(24) GRANULARITY 4,
    -- Serves: free-text search in the log explorer. Without it, every
    -- substring query decompresses the whole body column.
    INDEX idx_body     body TYPE tokenbf_v1(32768, 3, 0) GRANULARITY 4
)
ENGINE = ReplacingMergeTree
PARTITION BY toDate(timestamp)
-- (service_name, timestamp): "tail the logs for service X" is the dominant
-- query and needs time contiguous immediately after service. severity_number
-- is deliberately NOT in the key -- putting it second would prune the error
-- filter nicely but scatter the far more common all-severity tail across the
-- whole day. The set index buys that pruning without paying that cost.
ORDER BY (service_name, timestamp, trace_id, span_id)
PRIMARY KEY (service_name, timestamp)
TTL toDateTime(timestamp) + INTERVAL 2 DAY  RECOMPRESS CODEC(ZSTD(6)),
    toDateTime(timestamp) + INTERVAL 7 DAY  TO VOLUME 'cold',
    toDateTime(timestamp) + INTERVAL 14 DAY DELETE
SETTINGS index_granularity = 8192,
         storage_policy = 'tiered',
         ttl_only_drop_parts = 1,
         non_replicated_deduplication_window = 1000;
