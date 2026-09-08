// k6 load test for the query side of TraceLens (CLAUDE.md: "Load
// generation: a Go span generator plus k6 for the query side" -- ingestion
// load is cmd/loadgen's job, this only ever hits cmd/query's HTTP API).
//
// Exercises the headline query shapes from the benchmark brief:
//   - trace-by-id lookup
//   - service latency percentiles over a rolling window (RED rollup)
//   - top-N slow operations (aggregate + sort + limit)
//   - service graph derivation
//   - log template search
//
// Usage:
//   BASE_URL=http://localhost:8080 k6 run k6/query-load.js
//   BASE_URL=http://localhost:8080 k6 run --vus 20 --duration 30s k6/query-load.js
//
// TRACE_ID can be set to a real id (see `make spans` or the query explorer)
// so the trace-by-id scenario measures a real point lookup instead of a
// guaranteed-empty one.
import http from "k6/http";
import { check } from "k6";
import { Trend } from "k6/metrics";

const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const TRACE_ID = __ENV.TRACE_ID || "00000000000000000000000000000001";

const traceByIdTrend = new Trend("trace_by_id_duration", true);
const serviceLatencyTrend = new Trend("service_latency_duration", true);
const topSlowTrend = new Trend("top_slow_ops_duration", true);
const serviceGraphTrend = new Trend("service_graph_duration", true);
const logSearchTrend = new Trend("log_template_search_duration", true);

export const options = {
  scenarios: {
    trace_by_id: { executor: "constant-vus", vus: 5, duration: "20s", exec: "traceByID" },
    service_latency: { executor: "constant-vus", vus: 5, duration: "20s", exec: "serviceLatency" },
    top_slow_ops: { executor: "constant-vus", vus: 5, duration: "20s", exec: "topSlowOps" },
    service_graph: { executor: "constant-vus", vus: 3, duration: "20s", exec: "serviceGraph" },
    log_search: { executor: "constant-vus", vus: 3, duration: "20s", exec: "logSearch" },
  },
  thresholds: {
    trace_by_id_duration: ["p(95)<200"],
    service_latency_duration: ["p(95)<500"],
    top_slow_ops_duration: ["p(95)<1000"],
    service_graph_duration: ["p(95)<500"],
    log_template_search_duration: ["p(95)<500"],
    http_req_failed: ["rate<0.01"],
  },
};

function postQuery(dsl) {
  return http.post(`${BASE_URL}/api/query`, JSON.stringify({ query: dsl }), {
    headers: { "Content-Type": "application/json" },
  });
}

export function traceByID() {
  const res = http.get(`${BASE_URL}/api/trace/${TRACE_ID}`);
  traceByIdTrend.add(res.timings.duration);
  check(res, { "trace lookup 200": (r) => r.status === 200 });
}

export function serviceLatency() {
  const res = postQuery('{} | p50(duration), p95(duration), p99(duration) by (service)');
  serviceLatencyTrend.add(res.timings.duration);
  check(res, { "service latency 200": (r) => r.status === 200 });
}

export function topSlowOps() {
  const res = postQuery('{} | p99(duration) by (operation) | sort by (p99_duration desc) | limit 10');
  topSlowTrend.add(res.timings.duration);
  check(res, { "top slow ops 200": (r) => r.status === 200 });
}

export function serviceGraph() {
  const res = http.get(`${BASE_URL}/api/services/graph?window=1h`);
  serviceGraphTrend.add(res.timings.duration);
  check(res, { "service graph 200": (r) => r.status === 200 });
}

export function logSearch() {
  const res = http.get(`${BASE_URL}/api/logs/templates?since=1h`);
  logSearchTrend.add(res.timings.duration);
  check(res, { "log template search 200": (r) => r.status === 200 });
}
