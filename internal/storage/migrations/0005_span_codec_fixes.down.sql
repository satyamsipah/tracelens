ALTER TABLE tracelens.spans MODIFY COLUMN `links.span_id` Array(FixedString(8)) CODEC(ZSTD(1));
ALTER TABLE tracelens.spans MODIFY COLUMN span_id FixedString(8) CODEC(ZSTD(1));
ALTER TABLE tracelens.spans ADD INDEX IF NOT EXISTS idx_duration duration_ns TYPE minmax GRANULARITY 4;
