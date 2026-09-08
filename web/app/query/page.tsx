"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { ExplainView } from "@/components/explain-view";
import { QueryEditor } from "@/components/query-editor";
import { ResultView } from "@/components/result-view";
import { explainQuery, fetchServices, runQuery } from "@/lib/api";
import type { ExplainResult, QueryResult } from "@/lib/types";

const EXAMPLES = [
  '{service="checkout", status=error} | duration > 500ms | count by (operation)',
  '{} | p50(duration), p95(duration), p99(duration) by (operation) | sort by (p99_duration desc) | limit 10',
  'trace("00000000000000000000000000000001")',
];

export default function QueryExplorerPage() {
  const [dsl, setDsl] = useState(EXAMPLES[0]);
  const [services, setServices] = useState<string[]>([]);
  const [operations, setOperations] = useState<string[]>([]);
  const [mode, setMode] = useState<"result" | "explain">("result");
  const [result, setResult] = useState<QueryResult | null>(null);
  const [explain, setExplain] = useState<ExplainResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    fetchServices()
      .then(setServices)
      .catch(() => setServices([]));
    // Operation names aren't a dedicated endpoint -- pulled from a cheap
    // aggregate query against the RED rollup via the DSL itself, so
    // autocomplete degrades gracefully (empty list) rather than needing new
    // backend surface just for name completion.
    runQuery("{} | count by (operation) | limit 200")
      .then((r) => {
        const idx = r.Columns.indexOf("operation");
        if (idx >= 0) setOperations(Array.from(new Set(r.Rows.map((row) => String(row[idx])))));
      })
      .catch(() => setOperations([]));
  }, []);

  async function handleRun(explainMode: boolean) {
    setLoading(true);
    setError(null);
    try {
      if (explainMode) {
        setMode("explain");
        setExplain(await explainQuery(dsl));
      } else {
        setMode("result");
        setResult(await runQuery(dsl));
      }
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Query explorer</h1>
        <p className="text-sm text-muted-foreground">
          {"{service=\"x\", status=error} | duration > 500ms | count by (operation)"} -- Ctrl/Cmd+Enter to run.
        </p>
      </div>

      <Card>
        <CardContent className="p-0">
          <QueryEditor value={dsl} onChange={setDsl} onSubmit={() => handleRun(false)} services={services} operations={operations} />
        </CardContent>
      </Card>

      <div className="flex flex-wrap items-center gap-2">
        <Button onClick={() => handleRun(false)} disabled={loading}>
          {loading && mode === "result" ? "Running…" : "Run"}
        </Button>
        <Button variant="outline" onClick={() => handleRun(true)} disabled={loading}>
          {loading && mode === "explain" ? "Explaining…" : "EXPLAIN"}
        </Button>
        <span className="mx-2 h-5 w-px bg-border" />
        {EXAMPLES.map((ex) => (
          <button
            key={ex}
            onClick={() => setDsl(ex)}
            className="rounded-full border px-3 py-1 text-xs text-muted-foreground hover:bg-muted"
          >
            {ex.length > 40 ? ex.slice(0, 40) + "…" : ex}
          </button>
        ))}
      </div>

      {error && (
        <Card className="border-destructive">
          <CardContent className="p-4 font-mono text-sm text-destructive">{error}</CardContent>
        </Card>
      )}

      {!error && mode === "result" && result && (
        <Card>
          <ResultView result={result} />
        </Card>
      )}

      {!error && mode === "explain" && explain && (
        <Card>
          <CardContent className="p-4">
            <ExplainView result={explain} />
          </CardContent>
        </Card>
      )}
    </div>
  );
}
