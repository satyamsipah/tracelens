"use client";

import { drag } from "d3-drag";
import {
  forceCenter,
  forceCollide,
  forceLink,
  forceManyBody,
  forceSimulation,
  type SimulationLinkDatum,
  type SimulationNodeDatum,
} from "d3-force";
import { scaleLinear, scaleSqrt } from "d3-scale";
import { select } from "d3-selection";
import { zoom } from "d3-zoom";
import { useEffect, useMemo, useRef, useState } from "react";
import { Badge } from "@/components/ui/badge";
import { formatDurationNS, formatPercent } from "@/lib/utils";
import type { ServiceEdgeStat, ServiceGraph } from "@/lib/types";

interface Node extends SimulationNodeDatum {
  id: string;
  traffic: number;
  criticality: number;
  cutVertex: boolean;
}
interface Link extends SimulationLinkDatum<Node> {
  source: string | Node;
  target: string | Node;
  edge: ServiceEdgeStat;
}

const WIDTH = 760;
const HEIGHT = 520;

function errorColor(rate: number): string {
  // green -> amber -> red as error rate rises from 0 to >=10%.
  if (rate >= 0.1) return "#dc2626";
  if (rate >= 0.02) return "#f59e0b";
  return "#16a34a";
}

export function ServiceMap({ graph }: { graph: ServiceGraph }) {
  const svgRef = useRef<SVGSVGElement>(null);
  const [selectedEdge, setSelectedEdge] = useState<ServiceEdgeStat | null>(null);

  const trafficByNode = useMemo(() => {
    const m = new Map<string, number>();
    for (const n of graph.Nodes) m.set(n, 0);
    for (const e of graph.Edges) {
      m.set(e.Callee, (m.get(e.Callee) ?? 0) + e.Calls);
    }
    return m;
  }, [graph]);

  const sizeScale = useMemo(() => {
    const max = Math.max(...Array.from(trafficByNode.values()), 1);
    return scaleSqrt().domain([0, max]).range([10, 34]);
  }, [trafficByNode]);

  useEffect(() => {
    if (!svgRef.current) return;

    const nodes: Node[] = graph.Nodes.map((id) => ({
      id,
      traffic: trafficByNode.get(id) ?? 0,
      criticality: graph.Criticality[id] ?? 0,
      cutVertex: !!graph.CutVertices[id],
    }));
    const nodeById = new Map(nodes.map((n) => [n.id, n]));
    const links: Link[] = graph.Edges.filter((e) => nodeById.has(e.Caller) && nodeById.has(e.Callee)).map((e) => ({
      source: e.Caller,
      target: e.Callee,
      edge: e,
    }));

    const svg = select(svgRef.current);
    svg.selectAll("*").remove();
    const root = svg.append("g");

    svg.call(
      zoom<SVGSVGElement, unknown>()
        .scaleExtent([0.3, 3])
        .on("zoom", (event) => root.attr("transform", event.transform)),
    );

    const widthScale = scaleLinear()
      .domain([0, Math.max(...links.map((l) => l.edge.Calls), 1)])
      .range([1.5, 8]);

    const link = root
      .append("g")
      .selectAll<SVGLineElement, Link>("line")
      .data(links)
      .join("line")
      .attr("stroke", (d) => errorColor(d.edge.ErrorRate))
      .attr("stroke-width", (d) => widthScale(d.edge.Calls))
      .attr("stroke-opacity", 0.7)
      .style("cursor", "pointer")
      .on("click", (_event, d) => setSelectedEdge(d.edge));

    const node = root
      .append("g")
      .selectAll<SVGGElement, Node>("g")
      .data(nodes)
      .join("g")
      .style("cursor", "grab")
      .call(
        drag<SVGGElement, Node>()
          .on("start", (event, d) => {
            if (!event.active) simulation.alphaTarget(0.3).restart();
            d.fx = d.x;
            d.fy = d.y;
          })
          .on("drag", (event, d) => {
            d.fx = event.x;
            d.fy = event.y;
          })
          .on("end", (event, d) => {
            if (!event.active) simulation.alphaTarget(0);
            d.fx = null;
            d.fy = null;
          }),
      );

    node
      .append("circle")
      .attr("r", (d) => sizeScale(d.traffic))
      .attr("fill", (d) => (d.cutVertex ? "#7c3aed" : "#3b82f6"))
      .attr("fill-opacity", 0.85)
      .attr("stroke", "#fff")
      .attr("stroke-width", 1.5);

    node
      .append("text")
      .text((d) => d.id)
      .attr("text-anchor", "middle")
      .attr("dy", (d) => sizeScale(d.traffic) + 12)
      .attr("font-size", 11)
      .attr("fill", "currentColor");

    const simulation = forceSimulation<Node>(nodes)
      .force(
        "link",
        forceLink<Node, Link>(links)
          .id((d) => d.id)
          .distance(120),
      )
      .force("charge", forceManyBody().strength(-260))
      .force("center", forceCenter(WIDTH / 2, HEIGHT / 2))
      .force(
        "collide",
        forceCollide<Node>().radius((d) => sizeScale(d.traffic) + 16),
      )
      .on("tick", () => {
        link
          .attr("x1", (d) => (d.source as Node).x ?? 0)
          .attr("y1", (d) => (d.source as Node).y ?? 0)
          .attr("x2", (d) => (d.target as Node).x ?? 0)
          .attr("y2", (d) => (d.target as Node).y ?? 0);
        node.attr("transform", (d) => `translate(${d.x ?? 0},${d.y ?? 0})`);
      });

    return () => {
      simulation.stop();
    };
  }, [graph, trafficByNode, sizeScale]);

  return (
    <div className="flex gap-4">
      <div className="min-w-0 flex-1">
        <svg ref={svgRef} viewBox={`0 0 ${WIDTH} ${HEIGHT}`} className="w-full rounded-md border bg-muted/20 text-foreground" />
        <div className="mt-2 flex flex-wrap gap-3 text-xs text-muted-foreground">
          <span className="flex items-center gap-1">
            <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: "#3b82f6" }} /> service
          </span>
          <span className="flex items-center gap-1">
            <span className="inline-block h-2.5 w-2.5 rounded-full" style={{ background: "#7c3aed" }} /> cut vertex (single point of failure)
          </span>
          <span>node size = traffic</span>
          <span>edge colour = error rate</span>
        </div>
      </div>

      <div className="w-72 shrink-0 space-y-3 rounded-md border p-3 text-xs">
        {graph.Cycles && graph.Cycles.length > 0 && (
          <div>
            <div className="mb-1 font-semibold uppercase text-destructive">Cycles detected</div>
            {graph.Cycles.map((cycle, i) => (
              <div key={i} className="text-muted-foreground">
                {cycle.join(" → ")}
              </div>
            ))}
          </div>
        )}

        {!selectedEdge ? (
          <p className="text-muted-foreground">Click an edge to see its latency distribution.</p>
        ) : (
          <div className="space-y-2">
            <div className="font-semibold">
              {selectedEdge.Caller} → {selectedEdge.Callee}
            </div>
            <Badge variant={selectedEdge.ErrorRate > 0.02 ? "error" : "muted"}>{formatPercent(selectedEdge.ErrorRate)} errors</Badge>
            <div>{selectedEdge.Calls.toLocaleString(undefined, { maximumFractionDigits: 1 })} calls</div>
            <LatencyBars p50={selectedEdge.P50} p95={selectedEdge.P95} p99={selectedEdge.P99} />
          </div>
        )}
      </div>
    </div>
  );
}

function LatencyBars({ p50, p95, p99 }: { p50: number; p95: number; p99: number }) {
  const max = Math.max(p50, p95, p99, 1);
  const rows = [
    { label: "p50", value: p50 },
    { label: "p95", value: p95 },
    { label: "p99", value: p99 },
  ];
  return (
    <div className="space-y-1">
      {rows.map((r) => (
        <div key={r.label} className="flex items-center gap-2">
          <div className="w-8 text-muted-foreground">{r.label}</div>
          <div className="h-3 flex-1 rounded bg-muted">
            <div className="h-3 rounded bg-primary" style={{ width: `${Math.max((r.value / max) * 100, 3)}%` }} />
          </div>
          <div className="w-14 shrink-0 text-right font-mono">{formatDurationNS(r.value)}</div>
        </div>
      ))}
    </div>
  );
}
