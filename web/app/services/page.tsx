import { Card, CardContent } from "@/components/ui/card";
import { ServiceMap } from "@/components/service-map";
import { fetchServiceGraph } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function ServicesPage({ searchParams }: { searchParams: { window?: string } }) {
  const window = searchParams.window ?? "1h";
  let graph;
  let error: string | null = null;
  try {
    graph = await fetchServiceGraph(window);
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }

  const windows = ["15m", "1h", "6h", "24h"];

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold tracking-tight">Service map</h1>
          <p className="text-sm text-muted-foreground">Derived from the service_edges rollup -- never a per-request scan of raw spans.</p>
        </div>
        <div className="flex gap-1">
          {windows.map((w) => (
            <a
              key={w}
              href={`/services?window=${w}`}
              className={`rounded-md border px-2.5 py-1 text-xs ${w === window ? "bg-primary text-primary-foreground" : "hover:bg-muted"}`}
            >
              {w}
            </a>
          ))}
        </div>
      </div>

      {error && (
        <Card className="border-destructive">
          <CardContent className="p-4 text-sm text-destructive">{error}</CardContent>
        </Card>
      )}

      {!error && graph && graph.Nodes.length === 0 && (
        <Card>
          <CardContent className="p-4 text-sm text-muted-foreground">
            No cross-service calls observed in the last {window}. Generate some traffic with <code>make loadgen</code>.
          </CardContent>
        </Card>
      )}

      {!error && graph && graph.Nodes.length > 0 && (
        <Card>
          <CardContent className="p-4">
            <ServiceMap graph={graph} />
          </CardContent>
        </Card>
      )}
    </div>
  );
}
