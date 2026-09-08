// Mirrors the JSON shapes cmd/query's HTTP API returns. Kept hand-written
// and adjacent to the fetch helpers rather than generated -- the API
// surface is small and stable enough that codegen would be more ceremony
// than the types themselves.

export interface QueryResult {
  Columns: string[];
  Rows: unknown[][];
}

export interface ExplainResult {
  DSL: string;
  LogicalPlan: string;
  OptimizedPlan: string;
  SQL: string;
  Args: unknown[];
  ClickHouseExplain: string[] | null;
  EstimatedRows: number;
}

export interface SpanRow {
  timestamp: string;
  trace_id: string;
  span_id: string;
  parent_span_id: string;
  service_name: string;
  span_name: string;
  span_kind: string;
  duration_ns: number;
  status_code: string;
  status_message: string;
  resource_attributes: Record<string, string>;
  span_attributes: Record<string, string>;
  sampling_weight: number;
  "events.timestamp"?: string[];
  "events.name"?: string[];
  "events.attributes"?: Record<string, string>[];
}

export interface SpanTreeNode {
  span: SpanRow;
  children: SpanTreeNode[];
  depth: number;
}

export interface ServiceEdgeStat {
  Caller: string;
  Callee: string;
  Calls: number;
  ErrorRate: number;
  P50: number; // nanoseconds (Go time.Duration JSON-encodes as int64 ns)
  P95: number;
  P99: number;
}

export interface ServiceGraph {
  Nodes: string[];
  Edges: ServiceEdgeStat[];
  Cycles: string[][] | null;
  Criticality: Record<string, number>;
  CutVertices: Record<string, boolean>;
}

export interface LogTemplateStat {
  TemplateID: number;
  TemplateText: string;
  Count: number;
  FirstSeen: string;
  LastSeen: string;
}

export interface LogInstance {
  Timestamp: string;
  ServiceName: string;
  SeverityText: string;
  Body: string;
  Params: string[] | null;
  TraceID: string;
  SpanID: string;
}

export interface StorageStat {
  Table: string;
  SizeOnDisk: string;
  Rows: number;
}

export interface CompressionStat {
  Table: string;
  RawBytes: number;
  CompressedBytes: number;
  Ratio: number;
}

export interface HealthResponse {
  ingestion_rate_per_sec: Record<string, number>;
  queue_depth: Record<string, number>;
  queue_capacity: Record<string, number>;
  drops_per_sec: Record<string, number>;
  inflight_traces: number;
  inflight_bytes: number;
  forced_decisions_per_sec: number;
  evicted_traces_per_sec: number;
  consumer_lag_seconds: Record<string, number>;
  templates_total: number;
  cardinality_breaches: Record<string, number>;
  storage: StorageStat[] | null;
  compression: CompressionStat[] | null;
}
