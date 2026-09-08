"use client";

import Link from "next/link";
import { Fragment, useEffect, useState } from "react";
import { Card, CardContent } from "@/components/ui/card";
import { Table, TBody, TD, TH, THead, TR } from "@/components/ui/table";
import { fetchLogInstances, fetchLogTemplates } from "@/lib/api";
import type { LogInstance, LogTemplateStat } from "@/lib/types";
import { formatTimestamp } from "@/lib/utils";

export default function LogsPage() {
  const [templates, setTemplates] = useState<LogTemplateStat[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [expanded, setExpanded] = useState<number | null>(null);
  const [instances, setInstances] = useState<LogInstance[] | null>(null);

  useEffect(() => {
    fetchLogTemplates()
      .then(setTemplates)
      .catch((e) => setError(e instanceof Error ? e.message : String(e)));
  }, []);

  async function toggle(templateID: number) {
    if (expanded === templateID) {
      setExpanded(null);
      return;
    }
    setExpanded(templateID);
    setInstances(null);
    setInstances(await fetchLogInstances(templateID));
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Log explorer</h1>
        <p className="text-sm text-muted-foreground">
          Grouped by Drain template ({"internal/logs"}), not raw body -- click a row to see instances.
        </p>
      </div>

      {error && (
        <Card className="border-destructive">
          <CardContent className="p-4 text-sm text-destructive">{error}</CardContent>
        </Card>
      )}

      {!error && templates && templates.length === 0 && (
        <Card>
          <CardContent className="p-4 text-sm text-muted-foreground">No log templates in the last hour.</CardContent>
        </Card>
      )}

      {!error && templates && templates.length > 0 && (
        <Card>
          <Table>
            <THead>
              <TR>
                <TH>Template</TH>
                <TH>Occurrences</TH>
                <TH>First seen</TH>
                <TH>Last seen</TH>
              </TR>
            </THead>
            <TBody>
              {templates.map((t) => (
                <Fragment key={t.TemplateID}>
                  <TR className="cursor-pointer" onClick={() => toggle(t.TemplateID)}>
                    <TD className="font-mono">{t.TemplateText}</TD>
                    <TD>{t.Count.toLocaleString()}</TD>
                    <TD>{formatTimestamp(t.FirstSeen)}</TD>
                    <TD>{formatTimestamp(t.LastSeen)}</TD>
                  </TR>
                  {expanded === t.TemplateID && (
                    <tr>
                      <td colSpan={4} className="bg-muted/30 p-3">
                        {instances === null ? (
                          <p className="text-xs text-muted-foreground">Loading…</p>
                        ) : instances.length === 0 ? (
                          <p className="text-xs text-muted-foreground">No instances in the last 24h.</p>
                        ) : (
                          <ul className="space-y-1.5 text-xs">
                            {instances.map((inst, i) => (
                              <li key={i} className="flex items-start justify-between gap-3 rounded border bg-card p-2">
                                <div>
                                  <div className="text-muted-foreground">
                                    {inst.ServiceName} · {inst.SeverityText} · {formatTimestamp(inst.Timestamp)}
                                  </div>
                                  <div className="break-words font-mono">{inst.Body}</div>
                                </div>
                                {inst.TraceID && (
                                  <Link href={`/traces/${inst.TraceID}`} className="shrink-0 whitespace-nowrap text-primary underline">
                                    jump to trace
                                  </Link>
                                )}
                              </li>
                            ))}
                          </ul>
                        )}
                      </td>
                    </tr>
                  )}
                </Fragment>
              ))}
            </TBody>
          </Table>
        </Card>
      )}
    </div>
  );
}
