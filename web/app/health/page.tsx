import { Badge } from "@/components/ui/badge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Table, TBody, TD, TH, THead, TR } from "@/components/ui/table";
import { fetchHealth } from "@/lib/api";

export const dynamic = "force-dynamic";
export const revalidate = 0;

function StatTile({ label, value, sub }: { label: string; value: string; sub?: string }) {
  return (
    <Card>
      <CardContent className="p-4">
        <div className="text-2xl font-bold">{value}</div>
        <div className="text-xs text-muted-foreground">{label}</div>
        {sub && <div className="mt-1 text-xs text-muted-foreground">{sub}</div>}
      </CardContent>
    </Card>
  );
}

export default async function HealthPage() {
  let h;
  let error: string | null = null;
  try {
    h = await fetchHealth();
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }

  if (error || !h) {
    return (
      <Card className="border-destructive">
        <CardContent className="p-4 text-sm text-destructive">{error}</CardContent>
      </Card>
    );
  }

  const totalDrops = Object.values(h.drops_per_sec).reduce((a, b) => a + b, 0);

  return (
    <div className="space-y-6">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">System health</h1>
        <p className="text-sm text-muted-foreground">Live from Prometheus and ClickHouse system.parts -- refresh to update.</p>
      </div>

      <div className="grid grid-cols-2 gap-4 sm:grid-cols-4">
        <StatTile label="Spans/sec" value={h.ingestion_rate_per_sec.spans?.toFixed(1) ?? "0"} />
        <StatTile label="Logs/sec" value={h.ingestion_rate_per_sec.logs?.toFixed(1) ?? "0"} />
        <StatTile label="Metric points/sec" value={h.ingestion_rate_per_sec.metrics?.toFixed(1) ?? "0"} />
        <StatTile label="Drops/sec (all reasons)" value={totalDrops.toFixed(2)} />
      </div>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Queue depth vs capacity</CardTitle>
          </CardHeader>
          <CardContent className="space-y-2 p-4">
            {Object.keys(h.queue_capacity).map((signal) => {
              const depth = h.queue_depth[signal] ?? 0;
              const capacity = h.queue_capacity[signal] || 1;
              const pct = Math.min((depth / capacity) * 100, 100);
              return (
                <div key={signal}>
                  <div className="flex justify-between text-xs">
                    <span className="capitalize text-muted-foreground">{signal}</span>
                    <span className="font-mono">
                      {Math.round(depth)} / {Math.round(capacity)}
                    </span>
                  </div>
                  <div className="h-2 rounded bg-muted">
                    <div className={`h-2 rounded ${pct > 80 ? "bg-destructive" : "bg-primary"}`} style={{ width: `${pct}%` }} />
                  </div>
                </div>
              );
            })}
          </CardContent>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Tail sampler memory</CardTitle>
          </CardHeader>
          <CardContent className="grid grid-cols-2 gap-3 p-4 text-sm">
            <div>
              <div className="text-lg font-semibold">{Math.round(h.inflight_traces).toLocaleString()}</div>
              <div className="text-xs text-muted-foreground">in-flight traces</div>
            </div>
            <div>
              <div className="text-lg font-semibold">{(h.inflight_bytes / 1024).toFixed(1)} KiB</div>
              <div className="text-xs text-muted-foreground">in-flight bytes</div>
            </div>
            <div>
              <div className="text-lg font-semibold">{h.forced_decisions_per_sec.toFixed(2)}/s</div>
              <div className="text-xs text-muted-foreground">forced decisions</div>
            </div>
            <div>
              <div className="text-lg font-semibold">{h.evicted_traces_per_sec.toFixed(2)}/s</div>
              <div className="text-xs text-muted-foreground">evicted traces</div>
            </div>
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Drops, by signal and reason</CardTitle>
        </CardHeader>
        <CardContent className="flex flex-wrap gap-2 p-4">
          {Object.entries(h.drops_per_sec)
            .filter(([, rate]) => rate > 0)
            .map(([key, rate]) => (
              <Badge key={key} variant="error">
                {key}: {rate.toFixed(3)}/s
              </Badge>
            ))}
          {Object.values(h.drops_per_sec).every((r) => r === 0) && <Badge variant="muted">no drops</Badge>}
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader>
            <CardTitle>Storage size, by table (active parts)</CardTitle>
          </CardHeader>
          <Table>
            <THead>
              <TR>
                <TH>Table</TH>
                <TH>Size on disk</TH>
                <TH>Rows</TH>
              </TR>
            </THead>
            <TBody>
              {(h.storage ?? []).map((s) => (
                <TR key={s.Table}>
                  <TD className="font-mono">{s.Table}</TD>
                  <TD>{s.SizeOnDisk}</TD>
                  <TD>{s.Rows.toLocaleString()}</TD>
                </TR>
              ))}
            </TBody>
          </Table>
        </Card>

        <Card>
          <CardHeader>
            <CardTitle>Compression ratio, by table (measured)</CardTitle>
          </CardHeader>
          <Table>
            <THead>
              <TR>
                <TH>Table</TH>
                <TH>Raw</TH>
                <TH>Compressed</TH>
                <TH>Ratio</TH>
              </TR>
            </THead>
            <TBody>
              {(h.compression ?? []).map((c) => (
                <TR key={c.Table}>
                  <TD className="font-mono">{c.Table}</TD>
                  <TD>{(c.RawBytes / 1024 / 1024).toFixed(1)} MiB</TD>
                  <TD>{(c.CompressedBytes / 1024 / 1024).toFixed(1)} MiB</TD>
                  <TD className="font-semibold">{c.Ratio}x</TD>
                </TR>
              ))}
            </TBody>
          </Table>
        </Card>
      </div>
    </div>
  );
}
