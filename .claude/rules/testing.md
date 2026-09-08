---
paths:
  - "**/*_test.go"
---

# Testing rules

- Testcontainers with real ClickHouse and real Redpanda. Never mock storage
  or broker behaviour.
- Every pipeline stage is tested with out-of-order and duplicate input.
- Backpressure tests actually saturate the queue and assert the drop
  counter moved by exactly the expected amount.
- Hot-path code has a go test -bench benchmark committed alongside it.
- Table-driven tests, named "should <expected> when <condition>".
