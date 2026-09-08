import { Badge } from "@/components/ui/badge";
import type { ExplainResult } from "@/lib/types";

function PlanPane({ title, plan }: { title: string; plan: string }) {
  return (
    <div className="min-w-0 flex-1">
      <div className="mb-1 text-xs font-semibold uppercase text-muted-foreground">{title}</div>
      <pre className="overflow-auto rounded-md border bg-muted/40 p-3 text-xs leading-5">{plan}</pre>
    </div>
  );
}

/**
 * The logical plan (straight out of Build()) and the optimised plan (after
 * all five passes) side by side, so the effect of predicate/partition/limit/
 * projection pushdown is visible as a plan-text diff rather than something
 * you have to take on faith -- e.g. the optimised Scan node grows a
 * `predicate=`, `range=`, and `columns=` annotation the logical one never
 * has.
 */
export function ExplainView({ result }: { result: ExplainResult }) {
  return (
    <div className="space-y-4">
      <div className="flex flex-wrap items-center gap-2">
        <Badge>{result.EstimatedRows.toLocaleString()} rows estimated</Badge>
        <Badge variant="muted">{result.Args.length} bound parameter(s)</Badge>
      </div>

      <div className="flex flex-col gap-4 lg:flex-row">
        <PlanPane title="Logical plan" plan={result.LogicalPlan} />
        <PlanPane title="Optimised plan" plan={result.OptimizedPlan} />
      </div>

      <div>
        <div className="mb-1 text-xs font-semibold uppercase text-muted-foreground">Compiled SQL</div>
        <pre className="overflow-auto rounded-md border bg-muted/40 p-3 text-xs leading-5">{result.SQL}</pre>
      </div>

      {result.ClickHouseExplain && result.ClickHouseExplain.length > 0 && (
        <div>
          <div className="mb-1 text-xs font-semibold uppercase text-muted-foreground">ClickHouse EXPLAIN</div>
          <pre className="overflow-auto rounded-md border bg-muted/40 p-3 text-xs leading-5">
            {result.ClickHouseExplain.join("\n")}
          </pre>
        </div>
      )}
    </div>
  );
}
