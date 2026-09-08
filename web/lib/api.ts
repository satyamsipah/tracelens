import type {
  CompressionStat,
  ExplainResult,
  HealthResponse,
  LogInstance,
  LogTemplateStat,
  QueryResult,
  ServiceGraph,
  SpanRow,
  StorageStat,
} from "./types";

// cmd/query's HTTP API. Server components read this at request time (no
// build-time baking of telemetry data); client components read it for
// interactive re-fetches (EXPLAIN toggle, log instance expansion, etc).
export const API_BASE = process.env.TRACELENS_API_BASE ?? process.env.NEXT_PUBLIC_API_BASE ?? "http://localhost:8080";

async function apiFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`, {
    ...init,
    cache: "no-store",
    headers: { "Content-Type": "application/json", ...init?.headers },
  });
  if (!res.ok) {
    const body = await res.text().catch(() => "");
    throw new Error(`${path}: ${res.status} ${res.statusText} ${body}`);
  }
  return res.json() as Promise<T>;
}

export function runQuery(dsl: string): Promise<QueryResult> {
  return apiFetch<QueryResult>("/api/query", { method: "POST", body: JSON.stringify({ query: dsl }) });
}

export function explainQuery(dsl: string): Promise<ExplainResult> {
  return apiFetch<ExplainResult>("/api/explain", { method: "POST", body: JSON.stringify({ query: dsl }) });
}

export async function fetchTrace(id: string): Promise<SpanRow[]> {
  const result = await apiFetch<QueryResult>(`/api/trace/${encodeURIComponent(id)}`);
  return rowsToObjects<SpanRow>(result);
}

export function fetchTraceLogs(id: string): Promise<LogInstance[]> {
  return apiFetch<LogInstance[]>(`/api/trace/${encodeURIComponent(id)}/logs`);
}

export function fetchServices(): Promise<string[]> {
  return apiFetch<string[]>("/api/services");
}

export function fetchServiceGraph(window?: string): Promise<ServiceGraph> {
  const qs = window ? `?window=${encodeURIComponent(window)}` : "";
  return apiFetch<ServiceGraph>(`/api/services/graph${qs}`);
}

export function fetchLogTemplates(since?: string): Promise<LogTemplateStat[]> {
  const qs = since ? `?since=${encodeURIComponent(since)}` : "";
  return apiFetch<LogTemplateStat[]>(`/api/logs/templates${qs}`);
}

export function fetchLogInstances(templateID: number): Promise<LogInstance[]> {
  return apiFetch<LogInstance[]>(`/api/logs/templates/${templateID}/instances`);
}

export function fetchHealth(): Promise<HealthResponse> {
  return apiFetch<HealthResponse>("/api/health");
}

export type { StorageStat, CompressionStat };

// rowsToObjects turns the generic {Columns, Rows} shape /api/query and
// /api/trace return into an array of plain objects keyed by column name --
// the query engine's column set is only known at query time, so this can't
// be a typed Scan the way the Go side does it.
export function rowsToObjects<T>(result: QueryResult): T[] {
  return result.Rows.map((row) => {
    const obj: Record<string, unknown> = {};
    result.Columns.forEach((col, i) => {
      obj[col] = row[i];
    });
    return obj as T;
  });
}
