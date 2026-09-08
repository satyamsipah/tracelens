# web/

Next.js 14 (App Router) + TypeScript + Tailwind + D3 front end.

**Not implemented yet — phase 4.** This directory is a placeholder so the
module layout is settled before the UI lands.

Planned views, and the storage support each already has:

| View | Backed by |
|---|---|
| Trace waterfall | `bloom_filter` skip index on `spans.trace_id` for the point lookup |
| Flamegraph | `parent_span_id` + `duration_ns` on the same trace fetch |
| Service map | `ORDER BY (service_name, span_name, …)` — the prefix these queries filter on |
| Log explorer | `tokenbf_v1` on `logs.body`, `set(24)` on `severity_number` |

It will talk to `cmd/query` (phase 3), never to ClickHouse directly — the
layering rule is that no ClickHouse SQL exists outside `internal/storage`.

One thing the UI must not get wrong: once tail sampling lands, any count it
displays has to be weighted by `sampling_weight`. An unweighted count renders
a plausible number that is simply false.
