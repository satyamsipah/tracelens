// Package sampling will hold trace assembly and the tail sampler (phase 2).
//
// It is empty by design in phase 1, but the constraints it must satisfy are
// already fixed by decisions made upstream of it, and are recorded here so
// they are not rediscovered the hard way:
//
//   - Every span of a trace already arrives at ONE instance, because spans are
//     Kafka-partitioned on trace_id (see internal/pipeline). Consuming this
//     package from a differently-partitioned topic silently breaks it.
//
//   - The in-flight trace buffer must have a HARD cap and an explicit
//     eviction policy. Unbounded buffering here is the classic way a tail
//     sampler becomes an outage: trace completeness is not decidable, so
//     "wait a bit longer" has no natural stopping point.
//
//   - Any span it emits must carry a sampling_weight. The column already
//     exists in tracelens.spans and is written as 1 by phase 1, so nothing
//     downstream needs rewriting -- but a sampler that leaves it at 1 makes
//     every aggregate silently wrong rather than obviously broken.
//
//   - Sampling decisions are per TRACE, not per span. A policy that admits
//     some spans of a trace produces exactly the truncated traces the ingest
//     path takes care never to create.
package sampling
