-- 0006: log template dictionary (backing internal/logs' Drain output) and
-- per-severity retention on tracelens.logs.

-- Dictionary from a dense template_id to its (possibly still-generalizing)
-- text. ReplacingMergeTree keyed on template_id, ordered by updated_at: a
-- template that widens further after creation is re-inserted with fresher
-- text rather than mutated in place, and the ReplacingMergeTree engine
-- collapses to the latest version on merge -- the identical pattern already
-- used for spans/logs/metrics dedup, applied here to "the same template
-- gained more wildcards" instead of "the same span arrived twice".
CREATE TABLE IF NOT EXISTS tracelens.log_templates
(
    template_id   UInt32   CODEC(T64, ZSTD(1)),
    template_text String   CODEC(ZSTD(1)),
    first_seen    DateTime CODEC(DoubleDelta, ZSTD(1)),
    updated_at    DateTime CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY template_id
SETTINGS index_granularity = 8192,
         non_replicated_deduplication_window = 1000;

-- Per-severity retention. OTLP severity bands: TRACE 1-4, DEBUG 5-8,
-- INFO 9-12, WARN 13-16, ERROR 17-20, FATAL 21-24.
--
-- TRACE/DEBUG are high-volume and low long-term value once the incident
-- they were emitted during is over; INFO/WARN keep the original phase-1
-- default; ERROR/FATAL are kept far longer since they are exactly what an
-- incident postmortem needs weeks later. Layered under the SAME
-- unconditional recompress/cold-tier clauses from phase 1 -- every row ages
-- through one hot -> cold transition regardless of severity, only the final
-- deletion timing differs. A TRACE/DEBUG row's 1-day delete fires before it
-- would ever reach the 7-day cold-tier move, which is fine: there is no
-- reason to tier a row into cold storage moments before deleting it.
ALTER TABLE tracelens.logs MODIFY TTL
    toDateTime(timestamp) + INTERVAL 2 DAY  RECOMPRESS CODEC(ZSTD(6)),
    toDateTime(timestamp) + INTERVAL 7 DAY  TO VOLUME 'cold',
    toDateTime(timestamp) + INTERVAL 1 DAY  DELETE WHERE severity_number < 9,
    toDateTime(timestamp) + INTERVAL 14 DAY DELETE WHERE severity_number BETWEEN 9 AND 16,
    toDateTime(timestamp) + INTERVAL 90 DAY DELETE WHERE severity_number >= 17;
