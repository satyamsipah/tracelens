# CLAUDE.md — Project Context

## What we are building

TraceLens: an OpenTelemetry-compatible observability platform — telemetry
ingestion, columnar storage, tail-based sampling, and a query engine for
distributed traces, logs, and metrics. This is a data-intensive systems
project. Ingestion throughput, storage compression, and query latency are
the metrics that matter, and every one of them must be measured, not assumed.

## Non-negotiable principles

If any change risks violating one, stop and flag it instead of implementing.

1. Never lose a span silently. Under backpressure we either apply a
   documented drop policy with a counter, or reject with an explicit error.
2. Tail sampling memory is BOUNDED. The in-flight trace buffer has a hard
   cap and an eviction policy.
3. All spans of one trace must reach the SAME sampler instance. Routing is
   consistent-hashed on trace_id. Round-robin silently breaks tail sampling.
4. Cardinality is controlled at ingest, not at query time. Every attribute
   key has a cardinality budget; breaches are detected, counted, and either
   dropped or bucketed.
5. Every stored byte is justified. Every column declares an explicit codec,
   and the compression ratio is measured and recorded.
6. Sampling changes the meaning of counts. Any aggregate over sampled data
   carries its sampling weight, or it is wrong.

## Architecture

Instrumented apps --OTLP(gRPC/HTTP)--> Collector (batch, compress)
    -> Kafka (backpressure buffer, partitioned by trace_id)
        -> Trace Assembler + Tail Sampler  -> ClickHouse (spans)
        -> Log Parser (Drain templating)   -> ClickHouse (logs)
        -> Metric Aggregator (rollups)     -> ClickHouse (metrics)
    -> Query Engine (DSL -> AST -> logical plan -> physical plan)
        -> UI: trace waterfall, flamegraph, service map, log explorer

## Tech stack (do not substitute without asking)

- Language: Go 1.22+ (ingestion path — throughput critical)
- Protocol: OpenTelemetry OTLP over gRPC (4317) and HTTP (4318), protobuf
- Storage: ClickHouse 24.x (MergeTree, explicit codecs, TTL tiering)
- Buffer: Redpanda locally (Kafka API compatible, lighter than Kafka)
- Query DSL: hand-written lexer, parser, and planner — no off-the-shelf
  query language library, the planner IS the project
- UI: Next.js 14 (App Router) + TypeScript + Tailwind + D3
- Demo workload: a 4-service microservice app instrumented with the
  OpenTelemetry Go SDK, with injectable latency and errors
- Load generation: a Go span generator plus k6 for the query side
- Local orchestration: Docker Compose
- CI: GitHub Actions

## Code standards

- Layered: transport -> pipeline -> storage. No ClickHouse SQL outside the
  storage package.
- Hot path (ingestion) allocates as little as possible: reuse buffers with
  sync.Pool, avoid interface boxing in per-span code, benchmark before and
  after every change to it.
- Errors wrapped with context: fmt.Errorf("assemble trace %s: %w", id, err)
- context.Context threaded everywhere; every network and DB call has a
  timeout.
- Every pipeline stage exports Prometheus metrics: received, dropped,
  errored, queue depth, processing latency.
- Table-driven tests. Testcontainers with a real ClickHouse and a real
  Redpanda — never mocks for storage or broker behaviour.
- Go benchmarks (go test -bench) for the hot path, committed alongside code.

## How to work with me

- Before writing code for a new subsystem, propose the design in 5-10
  bullet points and WAIT for my approval.
- When there is a meaningful trade-off, present at least two options with
  pros and cons and give your recommendation. Do not silently pick one.
- Write tests and benchmarks in the same commit as the code they cover.
- After each prompt, append to docs/DECISIONS.md: what was decided, what
  was rejected, and why.
- Commit messages: Conventional Commits (feat:, fix:, test:, docs:, perf:,
  refactor:).

## End-of-prompt workflow (ALWAYS do this, unless told otherwise)

At the end of every prompt in this project, without being asked again:
1. Run the full test suite and confirm it passes
2. Update docs/DECISIONS.md with this prompt's decisions
3. Update the relevant README section
4. Stage and commit with a clear Conventional Commits message
   (multiple commits if the changes are logically separate)
5. Push to origin
6. Print a one-paragraph summary of what shipped and what is still open

## Definition of Done for any prompt

- docker compose up brings the whole stack up cleanly from scratch
- All tests pass with -race
- The demo app generates traffic and it is visible end to end
- New behaviour is covered by at least one failure or load test
- Any hot-path change has a before/after benchmark recorded
- docs/DECISIONS.md and README updated
- Changes committed and pushed
