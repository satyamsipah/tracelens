ALTER TABLE tracelens.logs MODIFY TTL
    toDateTime(timestamp) + INTERVAL 2 DAY  RECOMPRESS CODEC(ZSTD(6)),
    toDateTime(timestamp) + INTERVAL 7 DAY  TO VOLUME 'cold',
    toDateTime(timestamp) + INTERVAL 14 DAY DELETE;
DROP TABLE IF EXISTS tracelens.log_templates;
