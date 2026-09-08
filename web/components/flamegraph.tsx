"use client";

import { useMemo, useState } from "react";
import { formatDurationNS } from "@/lib/utils";
import type { FlameNode } from "@/lib/flamegraph";

const ROW_HEIGHT = 24;

function colorFor(node: FlameNode): string {
  if (node.errorCount > 0) return "bg-destructive/80";
  return "bg-primary/80";
}

interface Rect {
  node: FlameNode;
  depth: number;
  x0: number; // fraction [0,1]
  x1: number;
}

/** Lays out the icicle chart below `focus`, focus itself occupying the full
 * width of row 0 -- the click-to-zoom behaviour reuses this by just
 * re-rooting the layout at whatever node was clicked. */
function layout(focus: FlameNode, maxDepth = 12): Rect[] {
  const rects: Rect[] = [{ node: focus, depth: 0, x0: 0, x1: 1 }];
  const walk = (node: FlameNode, depth: number, x0: number, x1: number) => {
    if (depth >= maxDepth) return;
    const total = node.value || node.children.reduce((s, c) => s + c.value, 0) || 1;
    let cursor = x0;
    for (const child of node.children) {
      const width = ((x1 - x0) * child.value) / total;
      rects.push({ node: child, depth: depth + 1, x0: cursor, x1: cursor + width });
      walk(child, depth + 1, cursor, cursor + width);
      cursor += width;
    }
  };
  walk(focus, 0, 0, 1);
  return rects;
}

export function Flamegraph({ root }: { root: FlameNode }) {
  const [focus, setFocus] = useState<FlameNode>(root);
  const [breadcrumb, setBreadcrumb] = useState<FlameNode[]>([root]);
  const rects = useMemo(() => layout(focus), [focus]);
  const maxDepth = useMemo(() => rects.reduce((m, r) => Math.max(m, r.depth), 0), [rects]);

  function zoomTo(node: FlameNode) {
    const idx = breadcrumb.indexOf(node);
    if (idx >= 0) {
      setBreadcrumb(breadcrumb.slice(0, idx + 1));
    } else {
      setBreadcrumb([...breadcrumb, node]);
    }
    setFocus(node);
  }

  return (
    <div>
      <div className="mb-2 flex flex-wrap items-center gap-1 text-xs">
        {breadcrumb.map((node, i) => (
          <span key={i} className="flex items-center gap-1">
            {i > 0 && <span className="text-muted-foreground">/</span>}
            <button className="rounded px-1.5 py-0.5 hover:bg-muted" onClick={() => zoomTo(node)}>
              {node.name === "(root)" ? "all traces" : node.name}
            </button>
          </span>
        ))}
      </div>

      <div className="relative w-full rounded-md border" style={{ height: (maxDepth + 1) * ROW_HEIGHT }}>
        {rects.map((r, i) => (
          <button
            key={i}
            onClick={() => zoomTo(r.node)}
            className={`absolute overflow-hidden border-r border-background text-left text-[11px] text-white ${colorFor(r.node)} ${
              r.node === focus ? "" : "hover:brightness-110"
            }`}
            style={{
              left: `${r.x0 * 100}%`,
              width: `${Math.max((r.x1 - r.x0) * 100, 0.05)}%`,
              top: r.depth * ROW_HEIGHT,
              height: ROW_HEIGHT - 1,
            }}
            title={`${r.node.name} · ${formatDurationNS(r.node.value)} · ${r.node.count} instance(s)`}
          >
            <span className="block truncate px-1.5 leading-6">{r.node.name}</span>
          </button>
        ))}
      </div>
    </div>
  );
}
