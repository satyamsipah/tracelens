# Contributing

Thanks for looking. This is a personal systems project rather than a
community-run one, so the bar is less "follow the process" and more "the
change has to hold up under the same scrutiny as the rest of the codebase".

## Getting set up

```bash
git clone https://github.com/satyamsipah/tracelens && cd tracelens
make up          # full stack in Docker, traffic flowing
make test        # unit tests, no containers
make test-race   # full suite: real ClickHouse and Redpanda, race detector
make lint        # gofmt, vet, golangci-lint
```

Docker and Go 1.25+ are the only prerequisites. `make test-race` starts real
containers and takes several minutes; that is deliberate.

## The six principles

These are in [CLAUDE.md](CLAUDE.md) and they are not negotiable. A change
that violates one needs to argue the principle is wrong, not that this case
is special.

1. **Never lose a span silently.** Under backpressure, either apply a
   documented drop policy with a counter, or reject with an explicit error.
2. **Tail-sampling memory is bounded.** The in-flight buffer has a hard cap
   and an eviction policy.
3. **All spans of one trace reach the same sampler.** Routing is
   consistent-hashed on `trace_id`. Round-robin silently breaks tail
   sampling.
4. **Cardinality is controlled at ingest**, not at query time.
5. **Every stored byte is justified.** Every column declares an explicit
   codec, and the compression ratio is measured.
6. **Sampling changes the meaning of counts.** Any aggregate over sampled
   data carries its sampling weight, or it is wrong.

## What a good change looks like here

**Tests come in the same commit as the code.** Not the next one.

**No mocks for storage or broker behaviour.** Use Testcontainers with a real
ClickHouse and a real Redpanda. Mocked storage tests pass while the real
thing is broken, which is worse than no test.

**Name tests for behaviour**, table-driven where there is more than one case:

```go
t.Run("should reject the whole request when the queue is saturated", ...)
```

**Benchmark anything on the hot path**, before and after, and put the numbers
in the PR. The ingestion path allocates 1 time per request today; a change
that makes it 4 needs to say why that is worth it.

**Every ClickHouse column declares a codec, and every codec carries a comment**
naming the query it serves and what it measured. A test fails CI if a column
has none. "It seemed reasonable" is not a codec justification — measure it,
and if the measurement is embarrassing, record the embarrassing number.
There is a column in this schema stored with `CODEC(NONE)` because ZSTD was
measured *inflating* it.

**No ClickHouse SQL outside `internal/storage`.** The layering is
transport → pipeline → storage, and it holds.

**Errors carry context**: `fmt.Errorf("assemble trace %s: %w", id, err)`.

**Every network and database call takes a `context.Context` with a timeout.**

## Comments

Comment the *why*, never the *what*. Well-named code already says what it
does. What a reader cannot recover is the constraint you were working
around, the option you rejected, or the bug that made this line necessary.

The comments in this repo that earn their keep look like:

```go
// idx_duration was HERE and is REMOVED by migration 0005: measured with
// EXPLAIN indexes=1 to prune 0 of 38 granules -- ORDER BY does not
// correlate with duration_ns, so it earns nothing and only costs
// write-side CPU.
```

Not `// remove the index`.

## Commits and PRs

[Conventional Commits](https://www.conventionalcommits.org/): `feat:`,
`fix:`, `test:`, `docs:`, `perf:`, `refactor:`. Split logically separate
changes into separate commits.

A PR should say what changed, what you measured, and what you decided *not*
to do. If it changes behaviour anyone would have to reason about later, add
an entry to [docs/DECISIONS.md](docs/DECISIONS.md) — including what you
rejected and why. That file is the most useful thing in the repository and it
stays that way only if it keeps being written.

## CI

Every PR runs: gofmt, `go vet`, golangci-lint, `go test -race` with real
containers, hot-path benchmarks, and a full `docker compose up` smoke test
that asserts spans actually reach ClickHouse end to end. The benchmark job is
not a gate — shared runners are too noisy for a wall-clock threshold — but it
publishes allocs/op into the log so an allocation regression is visible in
review.

## Reporting a bug

Please include what you ran, what you expected, what happened, and the
relevant metric or log line. For anything involving sampling or ingestion,
`otlp_spans_dropped_total{reason}` and `tracelens_consumer_lag_seconds` are
usually the two numbers that identify the problem.
