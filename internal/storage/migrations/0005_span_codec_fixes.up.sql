-- 0005: two fixes from an empirical schema review against live traffic
-- (~900K spans). See docs/DECISIONS.md for the full measurements. Deliberately
-- a NEW migration rather than an edit to 0002 -- an already-migrated instance
-- has the ledger marking 0002 applied, so editing that file's DDL in place
-- would silently diverge fresh installs from running ones.

-- FIX 1: idx_duration prunes ZERO granules and never will.
--
-- Measured with EXPLAIN indexes=1 on `service_name = 'gateway' AND
-- duration_ns > 200000000`: "Skip idx_duration Granules: 38/38" -- every
-- granule that survives the primary-key filter also survives the minmax
-- check. This confirms the suspicion recorded when the index was added: ORDER
-- BY does not correlate with duration_ns, so a granule's duration range is
-- close to the column's full range regardless of which granule it is. The
-- index still costs write-side CPU and storage on every insert and merge for
-- zero query benefit.
ALTER TABLE tracelens.spans DROP INDEX IF EXISTS idx_duration;

-- FIX 2: span_id's codec was chosen by analogy with trace_id, and the analogy
-- does not hold.
--
-- Measured: data_compressed_bytes (7,115,186) > data_uncompressed_bytes
-- (7,111,792) for span_id -- ZSTD makes it LARGER. The reasoning that
-- justified ZSTD(1) on trace_id was that spans of one trace repeat the same
-- trace_id within a batch, giving ZSTD real matches to exploit (measured
-- ratio 1.23x). span_id has no such repetition: it is unique per row by
-- construction, so ZSTD sees pure high-entropy noise and only adds frame
-- overhead. `links.span_id` is the identical shape (a span identifier with no
-- expected cross-row repetition) and is fixed for the same reason, ahead of
-- having its own data to measure against, since the argument is structural
-- rather than data-dependent.
--
-- parent_span_id and trace_id are UNCHANGED: parent_span_id measures 1.10x
-- (roots are zero-filled, giving ZSTD real repetition) and trace_id measures
-- 1.23x, so ZSTD(1) is earning its keep on both.
ALTER TABLE tracelens.spans MODIFY COLUMN span_id FixedString(8) CODEC(NONE);
ALTER TABLE tracelens.spans MODIFY COLUMN `links.span_id` Array(FixedString(8)) CODEC(NONE);
