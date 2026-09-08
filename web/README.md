# web/

Next.js 14 (App Router) + TypeScript + Tailwind, talking only to `cmd/query`'s
HTTP API -- never to ClickHouse directly, matching the rest of the codebase's
"no SQL outside the query/storage layers" rule.

## Views

| Route | What it shows |
|---|---|
| `/` | Overview: live health tiles + links to every other view |
| `/query` | DSL editor (syntax highlighting + service/operation autocomplete), results as table or bar chart, an **EXPLAIN** toggle showing the logical plan, optimised plan, compiled SQL, and ClickHouse's own EXPLAIN side by side |
| `/traces/[id]` | Trace waterfall: nested bars on a shared time axis, critical path highlighted, virtualised for 500+ span traces, click a span for attributes/events/linked logs |
| `/flamegraph` | Call tree merged across up to 20 recent traces of one operation, click-to-zoom with breadcrumb navigation |
| `/services` | Force-directed service dependency graph (d3-force): node size = traffic, edge colour = error rate, click an edge for its latency distribution; cycles and cut-vertices (single points of failure) called out explicitly |
| `/logs` | Log explorer grouped by Drain template ("this template occurred N times"), expandable to instances, jump-to-trace link when a log carries a trace_id |
| `/health` | Ingestion rate, drop counters, sampler memory, queue depths, storage size and compression ratio -- the same numbers the self-monitoring Grafana dashboard shows, read from the same Prometheus/ClickHouse sources |

## Running it

```bash
cd web
cp env.example .env.local   # points at cmd/query on localhost:8080
npm install
npm run dev
```

Or as part of the whole stack: `make up` builds and runs it via
`deploy/web.Dockerfile` (Next.js standalone output on a plain `node:20-alpine`
image -- `deploy/Dockerfile` is Go-only and has no Node runtime), on
`localhost:3001`.

## Design notes

- **Server components fetch data** (`/`, `/traces/[id]`, `/services`,
  `/health`); interactive views (`/query`, `/flamegraph`, `/logs`) are client
  components that call the API directly, since their data depends on user
  input after the initial load.
- **`sampling_weight` discipline carries into the UI.** Every count/rate
  rendered anywhere comes from `cmd/query`'s API, which already applies
  CLAUDE.md principle 6 server-side (`sum(sampling_weight)`, weighted
  quantiles) -- the UI never re-derives a count from raw rows itself, so
  there is no place for an unweighted number to sneak back in.
- **Trace/span ids are hex strings everywhere**, including over the wire:
  `internal/query.scanRows` hex-encodes `trace_id`/`span_id`/`parent_span_id`
  before they ever reach JSON, because ClickHouse returns them as raw
  `FixedString` bytes, and `encoding/json` silently mangles invalid UTF-8 in
  a Go string -- a real bug caught and fixed while wiring this UI up, not a
  hypothetical one.
- **The flamegraph's "time-series chart" and DSL "chart" toggle are both
  honest about what the DSL actually supports**: there is no explicit
  time-bucketing pipeline stage (see `internal/query/ast.go`), so "chart"
  means a bar chart over whatever a `by (...)` aggregation actually grouped
  by, not a fabricated time axis.
- **No shadcn CLI dependency.** `components/ui/*` are hand-written
  Tailwind primitives in the same spirit (small, owned, copy-pasted rather
  than a runtime package) so the whole UI has zero opaque component-library
  dependency to reason about.
