import Link from "next/link";
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from "@/components/ui/card";
import { fetchHealth, fetchServices } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function OverviewPage() {
  const [health, services] = await Promise.allSettled([fetchHealth(), fetchServices()]);

  const h = health.status === "fulfilled" ? health.value : null;
  const svcCount = services.status === "fulfilled" ? services.value.length : 0;

  const tiles = [
    { label: "Spans/sec", value: h ? h.ingestion_rate_per_sec.spans?.toFixed(1) ?? "0" : "—" },
    { label: "Services seen (30d)", value: svcCount.toString() },
    { label: "In-flight traces", value: h ? Math.round(h.inflight_traces).toString() : "—" },
    { label: "Log templates", value: h ? Math.round(h.templates_total).toString() : "—" },
  ];

  const cards = [
    { href: "/query", title: "Query explorer", desc: "Run the DSL, toggle EXPLAIN to see the logical/physical plan side by side." },
    { href: "/services", title: "Service map", desc: "Force-directed dependency graph, sized by traffic, coloured by error rate." },
    { href: "/flamegraph", title: "Flamegraph", desc: "Aggregated call tree across recent traces of one operation." },
    { href: "/logs", title: "Log explorer", desc: "Template-grouped log lines with jump-to-trace links." },
    { href: "/health", title: "System health", desc: "Ingestion, queues, sampler memory, storage and compression." },
  ];

  return (
    <div className="space-y-8">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">TraceLens</h1>
        <p className="mt-1 max-w-2xl text-sm text-muted-foreground">
          An OTel-compatible observability platform with a hand-written query DSL, planner, and optimiser underneath.
          Pick a trace id in the query explorer to see the waterfall, or jump straight to the service map.
        </p>
      </div>

      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        {tiles.map((t) => (
          <Card key={t.label}>
            <CardContent className="p-4">
              <div className="text-2xl font-bold">{t.value}</div>
              <div className="text-xs text-muted-foreground">{t.label}</div>
            </CardContent>
          </Card>
        ))}
      </div>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-2 lg:grid-cols-3">
        {cards.map((c) => (
          <Link key={c.href} href={c.href}>
            <Card className="h-full transition-colors hover:border-primary">
              <CardHeader>
                <CardTitle>{c.title}</CardTitle>
                <CardDescription>{c.desc}</CardDescription>
              </CardHeader>
            </Card>
          </Link>
        ))}
      </div>

      {health.status === "rejected" && (
        <p className="text-sm text-destructive">
          Couldn&apos;t reach the query API ({String(health.reason)}). Is <code>cmd/query</code> running?
        </p>
      )}
    </div>
  );
}
