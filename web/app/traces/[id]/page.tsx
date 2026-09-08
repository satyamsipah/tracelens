import Link from "next/link";
import { Card, CardContent } from "@/components/ui/card";
import { Waterfall } from "@/components/waterfall";
import { fetchTrace } from "@/lib/api";

export const dynamic = "force-dynamic";

export default async function TracePage({ params }: { params: { id: string } }) {
  let spans;
  let error: string | null = null;
  try {
    spans = await fetchTrace(params.id);
  } catch (e) {
    error = e instanceof Error ? e.message : String(e);
  }

  return (
    <div className="space-y-4">
      <div>
        <h1 className="text-2xl font-bold tracking-tight">Trace</h1>
        <p className="font-mono text-sm text-muted-foreground">{params.id}</p>
      </div>

      {error && (
        <Card className="border-destructive">
          <CardContent className="p-4 text-sm text-destructive">
            {error}
            <div className="mt-2">
              <Link href="/query" className="underline">
                Try the query explorer instead
              </Link>
            </div>
          </CardContent>
        </Card>
      )}

      {!error && spans && spans.length === 0 && (
        <Card>
          <CardContent className="p-4 text-sm text-muted-foreground">No spans found for this trace id.</CardContent>
        </Card>
      )}

      {!error && spans && spans.length > 0 && (
        <Card>
          <CardContent className="p-4">
            <Waterfall traceId={params.id} spans={spans} />
          </CardContent>
        </Card>
      )}
    </div>
  );
}
