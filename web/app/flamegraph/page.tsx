"use client";

import { useEffect, useState } from "react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { Flamegraph } from "@/components/flamegraph";
import { Input } from "@/components/ui/input";
import { fetchTrace, rowsToObjects, runQuery } from "@/lib/api";
import { mergeFlamegraphs, type FlameNode } from "@/lib/flamegraph";
import type { SpanRow } from "@/lib/types";

const SAMPLE_TRACES = 20;

export default function FlamegraphPage() {
  const [operations, setOperations] = useState<string[]>([]);
  const [operation, setOperation] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [root, setRoot] = useState<FlameNode | null>(null);
  const [sampleSize, setSampleSize] = useState(0);

  useEffect(() => {
    runQuery("{} | count by (operation) | sort by (count desc) | limit 100")
      .then((r) => {
        const idx = r.Columns.indexOf("operation");
        if (idx >= 0) setOperations(r.Rows.map((row) => String(row[idx])));
      })
      .catch(() => setOperations([]));
  }, []);

  async function loadFlamegraph(op: string) {
    if (!op) return;
    setLoading(true);
    setError(null);
    setRoot(null);
    try {
      // trace_id resolves to a real column (internal/query/plan.go's
      // knownColumns), so grouping by it is a legitimate DSL query -- this
      // finds a sample of traces that contain the chosen operation anywhere
      // in their tree, not just as the root.
      const traceIdsResult = await runQuery(`{operation="${op.replace(/"/g, '\\"')}"} | count by (trace_id) | limit ${SAMPLE_TRACES}`);
      const ids = rowsToObjects<{ trace_id: string }>(traceIdsResult).map((r) => r.trace_id);
      if (ids.length === 0) {
        setError(`No traces found containing operation "${op}" in the last hour.`);
        return;
      }
      const traces: SpanRow[][] = await Promise.all(ids.map((id) => fetchTrace(id)));
      setSampleSize(traces.length);
      setRoot(mergeFlamegraphs(traces));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Flamegraph</h1>
        <p className="text-sm text-muted-foreground">
          Aggregated call tree across up to {SAMPLE_TRACES} recent traces containing the chosen operation. Click a frame to zoom.
        </p>
      </div>

      <div className="flex flex-wrap items-center gap-2">
        <Input
          list="operations"
          value={operation}
          onChange={(e) => setOperation(e.target.value)}
          placeholder="operation name, e.g. charge"
          className="max-w-xs"
        />
        <datalist id="operations">
          {operations.map((op) => (
            <option key={op} value={op} />
          ))}
        </datalist>
        <Button onClick={() => loadFlamegraph(operation)} disabled={loading || !operation}>
          {loading ? "Loading…" : "Build flamegraph"}
        </Button>
        {sampleSize > 0 && <span className="text-xs text-muted-foreground">merged from {sampleSize} trace(s)</span>}
      </div>

      {error && (
        <Card className="border-destructive">
          <CardContent className="p-4 text-sm text-destructive">{error}</CardContent>
        </Card>
      )}

      {root && (
        <Card>
          <CardContent className="p-4">
            <Flamegraph root={root} />
          </CardContent>
        </Card>
      )}
    </div>
  );
}
