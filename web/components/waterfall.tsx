"use client";

import { scaleLinear } from "d3-scale";
import { useEffect, useMemo, useRef, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { fetchTraceLogs } from "@/lib/api";
import type { LogInstance, SpanRow, SpanTreeNode } from "@/lib/types";
import { buildSpanTree, computeCriticalPath, flattenTree, traceBounds } from "@/lib/trace-tree";
import { formatDurationNS, formatTimestamp } from "@/lib/utils";

const ROW_HEIGHT = 26;
const OVERSCAN = 10;

/** requestAnimationFrame-based FPS counter -- "measure frame rate" per the
 * brief, not just an assumption that virtualization keeps it smooth. */
function useFPS(active: boolean) {
  const [fps, setFps] = useState(60);
  useEffect(() => {
    if (!active) return;
    let frames = 0;
    let raf = 0;
    let last = performance.now();
    const tick = (now: number) => {
      frames++;
      if (now - last >= 500) {
        setFps(Math.round((frames * 1000) / (now - last)));
        frames = 0;
        last = now;
      }
      raf = requestAnimationFrame(tick);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [active]);
  return fps;
}

function statusColor(status: string): string {
  if (status === "error") return "bg-destructive";
  return "bg-primary";
}

export function Waterfall({ traceId, spans }: { traceId: string; spans: SpanRow[] }) {
  const roots = useMemo(() => buildSpanTree(spans), [spans]);
  const rows = useMemo(() => flattenTree(roots), [roots]);
  const criticalPath = useMemo(() => computeCriticalPath(roots), [roots]);
  const { start, end } = useMemo(() => traceBounds(spans), [spans]);

  const xScale = useMemo(() => scaleLinear().domain([start, end]).range([0, 100]), [start, end]);

  const [selected, setSelected] = useState<SpanTreeNode | null>(null);
  const [logs, setLogs] = useState<LogInstance[] | null>(null);
  const [scrolling, setScrolling] = useState(false);
  const fps = useFPS(scrolling);

  const containerRef = useRef<HTMLDivElement>(null);
  const [scrollTop, setScrollTop] = useState(0);
  const [viewportH, setViewportH] = useState(600);

  useEffect(() => {
    if (containerRef.current) setViewportH(containerRef.current.clientHeight);
  }, []);

  useEffect(() => {
    fetchTraceLogs(traceId)
      .then(setLogs)
      .catch(() => setLogs([]));
  }, [traceId]);

  const scrollTimer = useRef<ReturnType<typeof setTimeout> | null>(null);
  function handleScroll(e: React.UIEvent<HTMLDivElement>) {
    setScrollTop(e.currentTarget.scrollTop);
    setScrolling(true);
    if (scrollTimer.current) clearTimeout(scrollTimer.current);
    scrollTimer.current = setTimeout(() => setScrolling(false), 600);
  }

  // Virtualisation: only the rows within the scrolled viewport (+ overscan)
  // are ever mounted. Deep traces (500+ spans) would otherwise mount that
  // many DOM nodes at once, which is exactly what tanks frame rate on
  // scroll -- see useFPS above for the number this keeps honest.
  const firstVisible = Math.max(0, Math.floor(scrollTop / ROW_HEIGHT) - OVERSCAN);
  const lastVisible = Math.min(rows.length, Math.ceil((scrollTop + viewportH) / ROW_HEIGHT) + OVERSCAN);
  const visibleRows = rows.slice(firstVisible, lastVisible);

  return (
    <div className="flex gap-4">
      <div className="min-w-0 flex-1">
        <div className="mb-2 flex items-center justify-between">
          <div className="flex items-center gap-2 text-xs text-muted-foreground">
            <span>{rows.length} spans</span>
            <Badge variant="muted">critical path highlighted</Badge>
          </div>
          {scrolling && <Badge variant={fps < 50 ? "error" : "default"}>{fps} fps</Badge>}
        </div>

        <div
          ref={containerRef}
          onScroll={handleScroll}
          className="relative h-[600px] overflow-y-auto rounded-md border"
          style={{ contain: "strict" }}
        >
          <div style={{ height: rows.length * ROW_HEIGHT, position: "relative" }}>
            {visibleRows.map((node, i) => {
              const rowIndex = firstVisible + i;
              const span = node.span;
              const spanStart = new Date(span.timestamp).getTime();
              const spanEnd = spanStart + span.duration_ns / 1e6;
              const left = xScale(spanStart);
              const width = Math.max(xScale(spanEnd) - left, 0.3);
              const onCriticalPath = criticalPath.has(span.span_id);
              const isSelected = selected?.span.span_id === span.span_id;

              return (
                <button
                  key={span.span_id}
                  onClick={() => setSelected(node)}
                  className={`absolute left-0 flex w-full items-center text-left ${isSelected ? "bg-muted" : ""}`}
                  style={{ top: rowIndex * ROW_HEIGHT, height: ROW_HEIGHT }}
                >
                  <div
                    className="truncate pr-2 text-xs"
                    style={{ paddingLeft: node.depth * 14 + 4, width: 260, flexShrink: 0 }}
                    title={`${span.service_name} · ${span.span_name}`}
                  >
                    <span className="text-muted-foreground">{span.service_name}</span> {span.span_name}
                  </div>
                  <div className="relative h-4 flex-1">
                    <div
                      className={`absolute h-4 rounded-sm ${statusColor(span.status_code)} ${onCriticalPath ? "ring-2 ring-accent" : ""}`}
                      style={{ left: `${left}%`, width: `${width}%`, opacity: onCriticalPath ? 1 : 0.75 }}
                      title={formatDurationNS(span.duration_ns)}
                    />
                  </div>
                  <div className="w-20 shrink-0 pl-2 text-right font-mono text-[11px] text-muted-foreground">
                    {formatDurationNS(span.duration_ns)}
                  </div>
                </button>
              );
            })}
          </div>
        </div>
      </div>

      <div className="w-80 shrink-0 space-y-3 rounded-md border p-3 text-xs">
        {!selected ? (
          <p className="text-muted-foreground">Click a span to see its attributes, events, and linked logs.</p>
        ) : (
          <SpanDetails node={selected} logs={logs} />
        )}
      </div>
    </div>
  );
}

function SpanDetails({ node, logs }: { node: SpanTreeNode; logs: LogInstance[] | null }) {
  const span = node.span;
  const events = (span["events.name"] ?? []).map((name, i) => ({
    name,
    timestamp: span["events.timestamp"]?.[i] ?? "",
    attributes: span["events.attributes"]?.[i] ?? {},
  }));
  const spanLogs = (logs ?? []).filter((l) => l.SpanID === span.span_id);

  return (
    <div className="space-y-4">
      <div>
        <div className="font-semibold">{span.span_name}</div>
        <div className="text-muted-foreground">{span.service_name}</div>
        <div className="mt-1 flex flex-wrap gap-1">
          <Badge variant={span.status_code === "error" ? "error" : "muted"}>{span.status_code}</Badge>
          <Badge variant="muted">{span.span_kind}</Badge>
          <Badge variant="muted">{formatDurationNS(span.duration_ns)}</Badge>
          {span.sampling_weight !== 1 && <Badge variant="muted">weight {span.sampling_weight.toFixed(1)}</Badge>}
        </div>
        <div className="mt-1 text-muted-foreground">{formatTimestamp(span.timestamp)}</div>
      </div>

      <Section title="Span attributes" entries={span.span_attributes} />
      <Section title="Resource attributes" entries={span.resource_attributes} />

      {events.length > 0 && (
        <div>
          <div className="mb-1 font-semibold uppercase text-muted-foreground">Events</div>
          <ul className="space-y-1.5">
            {events.map((ev, i) => (
              <li key={i} className="rounded border p-1.5">
                <div className="font-medium">{ev.name}</div>
                <div className="text-muted-foreground">{formatTimestamp(ev.timestamp)}</div>
              </li>
            ))}
          </ul>
        </div>
      )}

      <div>
        <div className="mb-1 font-semibold uppercase text-muted-foreground">Linked logs</div>
        {logs === null ? (
          <p className="text-muted-foreground">Loading…</p>
        ) : spanLogs.length === 0 ? (
          <p className="text-muted-foreground">None for this span.</p>
        ) : (
          <ul className="space-y-1.5">
            {spanLogs.map((l, i) => (
              <li key={i} className="rounded border p-1.5">
                <div className="text-muted-foreground">
                  {l.SeverityText} · {formatTimestamp(l.Timestamp)}
                </div>
                <div className="break-words">{l.Body}</div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}

function Section({ title, entries }: { title: string; entries: Record<string, string> }) {
  const keys = Object.keys(entries ?? {});
  if (keys.length === 0) return null;
  return (
    <div>
      <div className="mb-1 font-semibold uppercase text-muted-foreground">{title}</div>
      <dl className="space-y-0.5">
        {keys.map((k) => (
          <div key={k} className="flex gap-2">
            <dt className="shrink-0 text-muted-foreground">{k}</dt>
            <dd className="truncate font-mono">{entries[k]}</dd>
          </div>
        ))}
      </dl>
    </div>
  );
}
