# Kubernetes deployment

Two paths to the same thing:

```bash
# Helm (the source of truth)
helm install tracelens deploy/helm/tracelens \
  --namespace tracelens --create-namespace \
  --set clickhouse.addr=clickhouse.data.svc:9000 \
  --set kafka.brokers=redpanda.data.svc:9092 \
  --set clickhouse.existingSecret=tracelens-clickhouse \
  --set web.publicAPIBase=https://api.example.com

# Plain kubectl (rendered from the chart by `make k8s-manifests`)
kubectl create namespace tracelens
kubectl -n tracelens apply -f deploy/k8s/manifests.yaml
```

`manifests.yaml` is generated. Edit the chart and re-render; never edit it
directly.

ClickHouse and Redpanda are **not** included. Both are stateful systems
whose real operation (replication, backups, upgrades, storage classes)
belongs to a purpose-built operator or a managed service, not to a
transitively-owned subchart of an application chart. Point
`clickhouse.addr` and `kafka.brokers` at what you already run.

---

## The one thing to get right: trace_id affinity

**The assembler has no Service, and that is deliberate. Do not add one to
route spans through.**

The assembler is the tail sampler. It holds every span of a trace in local
memory until that trace's sampling decision fires. That only works if all
spans of a trace reach the *same* replica.

That affinity comes from **Kafka partitioning by trace_id** — nothing
Kubernetes does provides it. The collector keys each span record with the
raw 16-byte trace_id, Kafka's partitioner hashes that key, and a
consumer-group member owns whole partitions. A trace therefore lands
entirely on one assembler, by construction.

Put a Service (or any load balancer) in front of the assembler and push
spans through it instead, and it round-robins a single trace's spans across
replicas. What breaks, concretely:

- **Each replica decides on a fragment.** One trace becomes N partial
  traces, each independently judged "complete enough" to emit.
- **Error traces get dropped invisibly.** The replica that never received
  the `ERROR` span sees a clean trace and drops it under a probabilistic
  policy. Nothing records that the full trace ever existed, so the
  false negative is undetectable after the fact.
- **Every aggregate double-counts.** Each fragment carries its own
  `sampling_weight`, so weighted counts over the sampled data are simply
  wrong (CLAUDE.md principle 6).
- **The bounded buffer fills with fragments that never complete**, forcing
  eviction of healthy traces. Principle 2's hard cap turns from a
  safeguard into a liability.

The insidious part is that **nothing looks broken**. Ingestion succeeds,
rows land, dashboards populate. It only shows up as sampling decisions that
are quietly wrong. This is why the invariant is structural here (no Service
exists to misuse) rather than merely documented.

Two corollaries:

1. **`assembler.replicaCount` must never exceed `kafka.partitions`.** Kafka
   gives each partition to exactly one group member; extra replicas idle,
   holding memory. The HPA template caps `maxReplicas` at the partition
   count for this reason.
2. **Changing `kafka.partitions` is a migration, not a knob.** Kafka's
   partitioner is `hash(key) % N`, so changing N rehashes every trace_id and
   splits any trace in flight across the change — reintroducing exactly the
   failure above, once, at cutover.

The collector, query API, and web UI have none of these constraints. All
three are stateless and scale freely behind ordinary Services.

---

## Autoscaling the assembler on consumer lag

The assembler's cost is dominated by *waiting* — on the decision window, on
ClickHouse — so CPU stays flat while the backlog grows. CPU-based
autoscaling would never fire. Lag is the signal that actually means
"behind", which is why `assembler.autoscaling` targets
`tracelens_consumer_lag_seconds`.

That HPA reads an **external metric**, which needs an adapter serving
`external.metrics.k8s.io`. This chart installs neither; pick one:

**Option A — prometheus-adapter** (keeps the HPA in the chart):

```bash
helm install prometheus-adapter prometheus-community/prometheus-adapter \
  --set prometheus.url=http://prometheus.monitoring.svc \
  --set 'rules.external[0].seriesQuery=tracelens_consumer_lag_seconds{topic!=""}' \
  --set 'rules.external[0].name.as=tracelens_consumer_lag_seconds' \
  --set 'rules.external[0].metricsQuery=max(<<.Series>>{<<.LabelMatchers>>}) by (topic)'

helm upgrade tracelens deploy/helm/tracelens --set assembler.autoscaling.enabled=true
```

**Option B — KEDA** (usually simpler): its Kafka scaler reads lag from the
broker directly, so no Prometheus is involved at all. Leave
`assembler.autoscaling.enabled=false` and apply a `ScaledObject`:

```yaml
apiVersion: keda.sh/v1alpha1
kind: ScaledObject
metadata:
  name: tracelens-assembler
spec:
  scaleTargetRef:
    name: tracelens-assembler
  minReplicaCount: 2
  maxReplicaCount: 12          # never above kafka.partitions
  cooldownPeriod: 600          # rebalances are costly; shed replicas slowly
  triggers:
    - type: kafka
      metadata:
        bootstrapServers: redpanda.data.svc:9092
        consumerGroup: tracelens-assembler
        topic: spans
        lagThreshold: "10000"
```

Whichever you choose, **scale down slowly**. Every scale event triggers a
consumer-group rebalance that briefly stops consumption on the partitions
that move. Shedding replicas eagerly after a burst costs throughput exactly
when the backlog is still draining — hence the 600s stabilization window in
both configurations above.

---

## Secrets

`secrets.create=true` (the default) is a convenience for demos. A password
passed via `--set` lands in your shell history *and* in Helm's release
history in-cluster.

For anything real, create the Secret out of band and reference it:

```bash
kubectl -n tracelens create secret generic tracelens-clickhouse \
  --from-literal=clickhouse-password="$(openssl rand -base64 32)"

helm upgrade tracelens deploy/helm/tracelens \
  --set secrets.create=false \
  --set clickhouse.existingSecret=tracelens-clickhouse
```

The password is only ever injected via `secretKeyRef` — it is never
rendered into a manifest, where it would show up in `kubectl get -o yaml`
and in whatever GitOps repo holds the output.

---

## Probes

`/healthz` and `/readyz` answer deliberately different questions:

| Probe | Endpoint | Checks | Failure means |
|---|---|---|---|
| liveness | `/healthz` | process is not wedged | container is **killed** |
| readiness | `/readyz` | ClickHouse and/or Redpanda reachable | pod leaves Service endpoints |

Liveness never consults a downstream, on purpose. A liveness failure kills
the container — so wiring ClickHouse into it would turn one dependency's
outage into a crash-loop across the whole fleet. Readiness failure is
recoverable on its own the moment the dependency returns.

Per component: the collector's readiness pings Kafka, the assembler's pings
both ClickHouse and Kafka, the query API's pings ClickHouse. Results are
cached for 2s so a large replica count polling on a 5s kubelet cadence does
not turn into proportional `Ping` load on the dependencies.

---

## Images

The chart uses the **distroless** (`slim`) image variant: no shell, no
package manager, ~2MB base. Kubernetes `httpGet` probes are executed by the
kubelet over the network, so nothing needs to exist inside the container to
support them.

| Image | alpine | distroless |
|---|---|---|
| assembler | 50.6MB | **36.8MB** |
| collector | 41.9MB | **28.1MB** |
| query | 35.1MB | **21.4MB** |

Docker Compose keeps using the alpine variant, because a Compose
healthcheck runs its command *inside* the container and therefore needs a
shell and `wget`.
