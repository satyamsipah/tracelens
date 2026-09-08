---
paths:
  - "**/*.sql"
  - "internal/storage/**/*.go"
---

# ClickHouse rules

- Every column declares an explicit CODEC.
- LowCardinality(String) for anything with fewer than ~10k distinct values.
- Timestamps use DoubleDelta. Monotonic integers use T64 or Delta.
- PARTITION BY toDate(timestamp). ORDER BY starts with the column most
  queries filter on first.
- Skip indexes deliberately: bloom_filter for high-cardinality equality
  (trace_id), minmax for range predicates (duration_ns).
- Every table has a TTL.
- Every index and codec choice gets a comment: which query it serves, what
  the measured effect was.
