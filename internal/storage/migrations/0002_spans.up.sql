-- 0002: spans.
--
-- Codec policy: ZSTD(1) on the insert path because insert CPU is ingestion
-- throughput. Cold parts are lifted to ZSTD(6) by the RECOMPRESS TTL below,
-- so we get cheap inserts AND dense cold storage instead of choosing one.

CREATE TABLE IF NOT EXISTS tracelens.spans
(
    ------------------------------------------------------------------ time --
    -- DateTime64(9): OTLP carries UnixNano. Truncating to milliseconds would
    -- destroy ordering fidelity for sub-millisecond spans, which is most of
    -- an in-process trace.
    -- DoubleDelta: rows within a granule are sorted by (service, name, time),
    -- so this stream is monotonic and near-regular -- roughly 1-2 bytes/row
    -- against 8 raw.
    timestamp            DateTime64(9)   CODEC(DoubleDelta, ZSTD(1)),

    -------------------------------------------------------------- identity --
    -- 128-bit CSPRNG output. Nothing compresses this. Delta, T64 and Gorilla
    -- would all INFLATE it by adding framing to incompressible bytes. ZSTD(1)
    -- alone, and only because the N spans of one trace arrive in one batch and
    -- therefore repeat within a granule.
    trace_id             FixedString(16) CODEC(ZSTD(1)),
    span_id              FixedString(8)  CODEC(ZSTD(1)),
    -- Root spans carry 8 zero bytes. That is the only structure in this column.
    parent_span_id       FixedString(8)  CODEC(ZSTD(1)),

    ------------------------------------------------------------ dimensions --
    -- Tens of distinct values, and the leading sort key, so the dictionary
    -- index is one repeated entry for a whole granule run.
    service_name         LowCardinality(String) CODEC(ZSTD(1)),
    -- Below the ~10k LowCardinality threshold ONLY while the ingest
    -- cardinality budget holds. Unbounded span names (URLs with ids baked in)
    -- are how this column detonates. The budget is what makes LowCardinality
    -- the correct type here rather than a latent bug.
    span_name            LowCardinality(String) CODEC(ZSTD(1)),
    -- OTLP defines exactly six kinds. Enum8 is one byte and rejects garbage at
    -- insert time -- strictly better than LowCardinality(String) for a closed
    -- set this small.
    span_kind            Enum8('unspecified'=0,'internal'=1,'server'=2,
                               'client'=3,'producer'=4,'consumer'=5)
                                         CODEC(ZSTD(1)),

    -------------------------------------------------------------- measures --
    -- T64, not Delta: durations are NOT monotonic, so Delta would encode
    -- noise. T64 transposes the 64-bit block and crops provably-unused high
    -- bits -- nearly every duration fits in under 2^32, so half the bits are
    -- zero across the whole block.
    duration_ns          UInt64          CODEC(T64, ZSTD(1)),
    status_code          Enum8('unset'=0,'ok'=1,'error'=2) CODEC(ZSTD(1)),
    -- Empty on the happy path, repetitive when set.
    status_message       String          CODEC(ZSTD(1)),

    ------------------------------------------------------------ attributes --
    -- Map is stored as two parallel arrays; the codec covers both streams.
    -- MEASURE: resource_attributes are byte-identical for every span from one
    -- process. Sorted by service_name they compress very well, but the honest
    -- alternative is a resource_fingerprint UInt64 plus a dimension table.
    -- Deferred until there is a measured ratio to argue from.
    resource_attributes  Map(LowCardinality(String), String) CODEC(ZSTD(1)),
    span_attributes      Map(LowCardinality(String), String) CODEC(ZSTD(1)),

    -------------------------------------------------------------- sampling --
    -- Every aggregate over sampled data must carry its weight or it is wrong.
    -- Written as 1 by this phase; the tail sampler sets it in phase 2. Present
    -- NOW so no aggregate written against this table needs rewriting later,
    -- and so there is no window where queries are silently undercounting.
    sampling_weight      Float64 DEFAULT 1 CODEC(Gorilla, ZSTD(1)),

    -------------------------------------------------------- events (flat) --
    -- Declared as explicit dotted Array columns rather than Nested(...) so
    -- that every column can carry its own codec.
    `events.timestamp`   Array(DateTime64(9)) CODEC(DoubleDelta, ZSTD(1)),
    `events.name`        Array(LowCardinality(String)) CODEC(ZSTD(1)),
    `events.attributes`  Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),

    --------------------------------------------------------- links (flat) --
    `links.trace_id`     Array(FixedString(16)) CODEC(ZSTD(1)),
    `links.span_id`      Array(FixedString(8))  CODEC(ZSTD(1)),
    `links.attributes`   Array(Map(LowCardinality(String), String)) CODEC(ZSTD(1)),

    ----------------------------------------------------------- skip indexes --
    -- Serves: "open trace X". trace_id is not a sort-key prefix, so without
    -- this a point lookup full-scans the partition. 0.01 FPR at GRANULARITY 1
    -- costs roughly 10KB per granule -- the one index here worth real bytes.
    INDEX idx_trace_id  trace_id TYPE bloom_filter(0.01) GRANULARITY 1,

    -- Serves: "spans slower than X". CAVEAT, stated up front: ORDER BY does
    -- not correlate with duration, so each granule holds a near-full range of
    -- durations and this may prune almost nothing. Kept only while we measure
    -- with EXPLAIN indexes=1; if it does not earn its keep, drop it rather
    -- than cargo-cult it. Real pruning for that query comes from the
    -- service+time prefix.
    INDEX idx_duration  duration_ns TYPE minmax GRANULARITY 4,

    -- Serves: "spans that HAVE attribute k" (e.g. http.status_code) without
    -- decompressing the map value stream.
    INDEX idx_attr_keys mapKeys(span_attributes) TYPE bloom_filter(0.01) GRANULARITY 4
)
ENGINE = ReplacingMergeTree
-- Day-aligned with the TTL so expiry is a whole-part drop, not a rewrite.
PARTITION BY toDate(timestamp)
-- Leading service_name: the analytical queries (RED metrics, service map,
-- slow operations) all filter it first, AND clustering like values is what
-- makes every downstream column's codec work. trace_id/span_id are in the
-- tail ONLY to give ReplacingMergeTree a dedup identity.
ORDER BY (service_name, span_name, timestamp, trace_id, span_id)
-- Deliberately a PREFIX of ORDER BY: keeps the sparse primary index small and
-- cache-resident while dedup still gets the full-key identity it needs.
PRIMARY KEY (service_name, span_name, timestamp)
TTL toDateTime(timestamp) + INTERVAL 3 DAY  RECOMPRESS CODEC(ZSTD(6)),
    toDateTime(timestamp) + INTERVAL 7 DAY  TO VOLUME 'cold',
    toDateTime(timestamp) + INTERVAL 30 DAY DELETE
SETTINGS index_granularity = 8192,
         storage_policy = 'tiered',
         -- Whole-part drop on expiry instead of a mutation rewrite.
         ttl_only_drop_parts = 1,
         -- Required for insert_deduplication_token to have any effect on a
         -- NON-replicated MergeTree. Without this the writer's idempotency
         -- token is silently ignored and Kafka retries duplicate rows.
         non_replicated_deduplication_window = 1000;
