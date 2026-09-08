-- 0001: database and migration bookkeeping.

CREATE DATABASE IF NOT EXISTS tracelens;

-- Applied-migration ledger. ReplacingMergeTree keyed on version so a re-apply
-- is idempotent rather than a duplicate row.
CREATE TABLE IF NOT EXISTS tracelens.schema_migrations
(
    version     UInt32   CODEC(T64, ZSTD(1)),
    name        String   CODEC(ZSTD(1)),
    applied_at  DateTime CODEC(DoubleDelta, ZSTD(1))
)
ENGINE = ReplacingMergeTree(applied_at)
ORDER BY version;
