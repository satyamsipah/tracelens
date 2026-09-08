---
paths:
  - "internal/ingest/**/*.go"
  - "internal/pipeline/**/*.go"
---

# Hot path rules

- Reuse buffers with sync.Pool. Do not allocate per span.
- Avoid interface boxing and reflection in per-span code paths.
- No fmt.Sprintf in the hot path — use strconv and byte slices.
- Every change here needs a go test -bench before/after in the commit.
- Bounded queues only, with a documented full-queue policy.
- Export a Prometheus counter for anything dropped.
