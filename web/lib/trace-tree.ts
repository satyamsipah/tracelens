import type { SpanRow, SpanTreeNode } from "./types";

const ZERO_SPAN_ID = "0".repeat(16); // 8 bytes hex

/**
 * Mirrors internal/sampling.BuildTree's shape client-side: index by span_id,
 * link to parent, root = all-zero parent_span_id. Orphans (a non-zero parent
 * that isn't present) are appended as extra roots rather than dropped, so a
 * partial/late-arriving trace still renders something sensible.
 */
export function buildSpanTree(spans: SpanRow[]): SpanTreeNode[] {
  const byId = new Map<string, SpanTreeNode>();
  for (const span of spans) {
    byId.set(span.span_id, { span, children: [], depth: 0 });
  }

  const roots: SpanTreeNode[] = [];
  for (const span of spans) {
    const node = byId.get(span.span_id)!;
    if (span.parent_span_id === ZERO_SPAN_ID) {
      roots.push(node);
      continue;
    }
    const parent = byId.get(span.parent_span_id);
    if (!parent) {
      roots.push(node); // orphan
      continue;
    }
    parent.children.push(node);
  }

  const assignDepth = (node: SpanTreeNode, depth: number) => {
    node.depth = depth;
    node.children.sort((a, b) => a.span.timestamp.localeCompare(b.span.timestamp));
    node.children.forEach((c) => assignDepth(c, depth + 1));
  };
  roots.sort((a, b) => a.span.timestamp.localeCompare(b.span.timestamp));
  roots.forEach((r) => assignDepth(r, 0));
  return roots;
}

/** Flattens the tree into display order (depth-first, pre-order). */
export function flattenTree(roots: SpanTreeNode[]): SpanTreeNode[] {
  const out: SpanTreeNode[] = [];
  const visit = (node: SpanTreeNode) => {
    out.push(node);
    node.children.forEach(visit);
  };
  roots.forEach(visit);
  return out;
}

function spanEnd(span: SpanRow): number {
  return new Date(span.timestamp).getTime() * 1e6 + span.duration_ns; // ns since epoch, approx
}

/**
 * The same simplified critical-path heuristic as
 * internal/sampling/tree.go's criticalChain: at each level, follow whichever
 * child's interval ends latest. Returns the set of span_ids on the chain.
 */
export function computeCriticalPath(roots: SpanTreeNode[]): Set<string> {
  let best: SpanTreeNode[] = [];
  let bestDuration = -1;

  for (const root of roots) {
    const chain: SpanTreeNode[] = [root];
    let cur = root;
    while (cur.children.length > 0) {
      let next = cur.children[0];
      let latestEnd = spanEnd(next.span);
      for (const c of cur.children) {
        const end = spanEnd(c.span);
        if (end > latestEnd) {
          next = c;
          latestEnd = end;
        }
      }
      chain.push(next);
      cur = next;
    }
    const total = chain.reduce((sum, n) => sum + n.span.duration_ns, 0);
    if (total > bestDuration) {
      bestDuration = total;
      best = chain;
    }
  }
  return new Set(best.map((n) => n.span.span_id));
}

export function traceBounds(spans: SpanRow[]): { start: number; end: number } {
  if (spans.length === 0) return { start: 0, end: 1 };
  let start = new Date(spans[0].timestamp).getTime();
  let end = start + spans[0].duration_ns / 1e6;
  for (const s of spans) {
    const st = new Date(s.timestamp).getTime();
    const en = st + s.duration_ns / 1e6;
    if (st < start) start = st;
    if (en > end) end = en;
  }
  return { start, end: Math.max(end, start + 1) };
}
