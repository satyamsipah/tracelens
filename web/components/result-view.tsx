"use client";

import Link from "next/link";
import { useMemo } from "react";
import { Table, TBody, TD, TH, THead, TR } from "@/components/ui/table";
import { Tabs, TabsList, TabsTrigger, TabsContent } from "@/components/ui/tabs";
import type { QueryResult } from "@/lib/types";

function cellText(v: unknown): string {
  if (v === null || v === undefined) return "";
  if (typeof v === "object") return JSON.stringify(v);
  return String(v);
}

/**
 * A generic bar chart over the result's first numeric column, one bar per
 * row, labelled by the first non-numeric column. The DSL has no explicit
 * time-bucketing stage (see internal/query/ast.go), so "time-series chart"
 * for an arbitrary query is, honestly, this: a chart of whatever the
 * aggregation actually grouped by -- which for `by (operation)` is exactly
 * the count/latency-per-operation bar chart a dashboard would want, and for
 * a query with no aggregation at all just falls back to the table.
 */
function BarChart({ result }: { result: QueryResult }) {
  const numericIdx = result.Columns.findIndex((_, i) => result.Rows.every((r) => typeof r[i] === "number"));
  const labelIdx = result.Columns.findIndex((_, i) => i !== numericIdx && typeof result.Rows[0]?.[i] !== "number");

  if (numericIdx === -1) {
    return <p className="p-4 text-sm text-muted-foreground">No numeric column to chart.</p>;
  }

  const values = result.Rows.map((r) => Number(r[numericIdx]) || 0);
  const max = Math.max(...values, 1);

  return (
    <div className="space-y-1.5 p-4">
      {result.Rows.map((row, i) => {
        const label = labelIdx >= 0 ? cellText(row[labelIdx]) : `#${i + 1}`;
        const value = values[i];
        return (
          <div key={i} className="flex items-center gap-2 text-xs">
            <div className="w-32 shrink-0 truncate text-right text-muted-foreground" title={label}>
              {label}
            </div>
            <div className="h-4 flex-1 rounded bg-muted">
              <div
                className="h-4 rounded bg-primary"
                style={{ width: `${Math.max((value / max) * 100, 2)}%` }}
                title={String(value)}
              />
            </div>
            <div className="w-16 shrink-0 font-mono">{value.toLocaleString(undefined, { maximumFractionDigits: 2 })}</div>
          </div>
        );
      })}
    </div>
  );
}

export function ResultView({ result }: { result: QueryResult }) {
  const rowCount = result.Rows.length;
  const traceIdIdx = result.Columns.indexOf("trace_id");

  const chartEligible = useMemo(
    () => result.Columns.some((_, i) => result.Rows.length > 0 && typeof result.Rows[0][i] === "number"),
    [result],
  );

  return (
    <Tabs defaultValue="table">
      <div className="flex items-center justify-between border-b px-4 py-2">
        <span className="text-xs text-muted-foreground">{rowCount} row(s)</span>
        <TabsList>
          <TabsTrigger value="table">Table</TabsTrigger>
          <TabsTrigger value="chart">Chart</TabsTrigger>
        </TabsList>
      </div>
      <TabsContent value="table">
        <Table>
          <THead>
            <TR>
              {result.Columns.map((c) => (
                <TH key={c}>{c}</TH>
              ))}
            </TR>
          </THead>
          <TBody>
            {result.Rows.map((row, i) => (
              <TR key={i}>
                {row.map((cell, j) =>
                  j === traceIdIdx ? (
                    <TD key={j} className="font-mono">
                      <Link href={`/traces/${cellText(cell)}`} className="text-primary underline">
                        {cellText(cell)}
                      </Link>
                    </TD>
                  ) : (
                    <TD key={j} className="font-mono">
                      {cellText(cell)}
                    </TD>
                  ),
                )}
              </TR>
            ))}
          </TBody>
        </Table>
      </TabsContent>
      <TabsContent value="chart">
        {chartEligible ? <BarChart result={result} /> : <p className="p-4 text-sm text-muted-foreground">No numeric column to chart.</p>}
      </TabsContent>
    </Tabs>
  );
}
