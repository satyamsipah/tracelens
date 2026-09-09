# Deployment

Three targets, in increasing order of effort:

| Target | What it is | Guide |
|---|---|---|
| Single VPS | Everything on one box, Caddy for TLS | [below](#single-vps) |
| Kubernetes | Helm chart or rendered manifests | [deploy/k8s/README.md](../deploy/k8s/README.md) |
| Live public demo | Fly.io + ClickHouse Cloud + Vercel | [below](#live-public-demo) |

---

## Single VPS

Assumes a 4-core / 8GB box with Docker installed, and two DNS records
already pointing at it.

```bash
git clone https://github.com/satyamsipah/tracelens && cd tracelens
cp deploy/env.prod.example .env
$EDITOR .env                       # set passwords + domains (see comments)
docker compose -f deploy/docker-compose.prod.yml up -d
docker compose -f deploy/docker-compose.prod.yml --profile demo up -d   # optional traffic
```

Differences from the dev stack worth knowing:

- **Nothing but Caddy publishes a port.** ClickHouse, Redpanda, Prometheus
  and Grafana are `expose:`-only, reachable on the compose network and
  nowhere else. Reach Grafana over an SSH tunnel:
  `ssh -N -L 3000:localhost:3000 user@vps`.
- **No default credentials.** Every secret uses `${VAR:?message}`, so
  compose refuses to start rather than falling back to a dev password.
- **`TRACELENS_IMAGE_TAG` is required, and `latest` is not the default** —
  a restart silently picking up a new build is how a "nothing changed"
  outage happens.
- Memory limits are set on every service. The assembler's limit must stay
  above `TRACELENS_BUFFER_MAX_BYTES` with headroom.

---

## Live public demo

**Everything here needs accounts I cannot create for you.** Each step gives
the exact command; run them yourself so credentials never pass through
anything but your own shell.

Architecture: Fly.io runs the Go services and Redpanda, ClickHouse Cloud
holds the data, Vercel serves the UI.

```
 instrumented demo app (Fly)
        │ OTLP gRPC
        ▼
 collector (Fly, public :4317)  ──▶ Redpanda (Fly, private)
                                          │
                                          ▼
                                   assembler (Fly, private)
                                          │
                                          ▼
                              ClickHouse Cloud (managed)
                                          ▲
                                          │
 UI (Vercel) ──────────▶ query API (Fly, public HTTPS)
```

### Step 0 — install the CLIs

```bash
brew install flyctl        # or: curl -L https://fly.io/install.sh | sh
npm i -g vercel            # already installed on this machine
```

### Step 1 — log in (you run these)

```bash
fly auth login             # opens a browser
vercel login               # opens a browser
```

Then create a ClickHouse Cloud service at
<https://clickhouse.cloud> (free tier is enough for a demo). From its
connection dialog, note:

- host, e.g. `abc123.us-east-1.aws.clickhouse.cloud`
- port `9440` (native protocol, TLS)
- username (`default`) and the generated password

> The native port is **9440 with TLS**, not 9000 in the clear. That is why
> `TRACELENS_CLICKHOUSE_TLS=true` appears in the Fly configs — without it
> the connection fails at the handshake.

### Step 2 — publish images

Images come from the GitHub Actions release workflow (below). To push a set
by hand:

```bash
# Image tags carry NO leading "v": the git tag v1.0.0 publishes 1.0.0,
# because that is what docker/metadata-action's semver pattern produces.
export TAG=1.0.0
for c in collector assembler query demo; do
  docker buildx build --target slim --build-arg TARGET=./cmd/$c \
    --platform linux/amd64,linux/arm64 --push \
    -t ghcr.io/satyamsipah/tracelens-$c:$TAG -f deploy/Dockerfile .
done
```

(`demo` builds from `./demo`, not `./cmd/demo` — adjust `TARGET`.)

### Make the GHCR packages public (one-time, manual)

GHCR packages are **private on first publish**. Until you change that, Fly
machines cannot pull them and neither can anyone reading this repo.

This cannot be scripted. There is **no REST endpoint for package
visibility** — the Packages API can read, delete and restore packages, but
not change visibility — so it is a web-UI action regardless of token scopes.

For each of `tracelens-collector`, `tracelens-assembler`, `tracelens-query`
and `tracelens-demo`:

1. Open <https://github.com/satyamsipah?tab=packages> and click the package.
2. Click **Package settings** (the gear on the right).
3. Scroll to **Danger Zone** → **Change visibility**.
4. Choose **Public**, type the package name to confirm.

While you are there, **Manage Actions access** → add the `tracelens`
repository with **Write** if you want future workflow runs to keep pushing
without re-authorising.

> **This is one-way.** GitHub does not allow a public package to be made
> private again. That is fine for a public demo, but worth knowing before
> clicking.

### Step 3 — Redpanda

```bash
fly apps create tracelens-redpanda
fly volumes create redpanda_data --app tracelens-redpanda --region iad --size 10
fly deploy --app tracelens-redpanda --config deploy/fly/redpanda.fly.toml
```

### Step 4 — collector

```bash
fly apps create tracelens-collector
fly deploy --app tracelens-collector --config deploy/fly/collector.fly.toml
```

### Step 5 — assembler (needs the ClickHouse credentials from step 1)

```bash
fly apps create tracelens-assembler
fly secrets set --app tracelens-assembler \
  TRACELENS_CLICKHOUSE_ADDR="<your-host>.clickhouse.cloud:9440" \
  TRACELENS_CLICKHOUSE_USER="default" \
  TRACELENS_CLICKHOUSE_PASSWORD="<password>"
fly deploy --app tracelens-assembler --config deploy/fly/assembler.fly.toml
```

The assembler runs the ClickHouse migrations on first start, so this is
also what creates the schema. Watch it with `fly logs -a tracelens-assembler`.

### Step 6 — query API

```bash
fly apps create tracelens-query
fly secrets set --app tracelens-query \
  TRACELENS_CLICKHOUSE_ADDR="<your-host>.clickhouse.cloud:9440" \
  TRACELENS_CLICKHOUSE_USER="default" \
  TRACELENS_CLICKHOUSE_PASSWORD="<password>"
fly deploy --app tracelens-query --config deploy/fly/query.fly.toml
```

Verify: `curl https://tracelens-query.fly.dev/api/services` → `[]` (empty
until traffic flows, which is the next step).

### Step 7 — demo traffic

```bash
fly apps create tracelens-demo
fly deploy --app tracelens-demo --config deploy/fly/demo.fly.toml
```

The gateway self-drives at 3 rps, so within a minute
`/api/services` returns the four demo services and the UI has live traces.
This is the piece that makes the demo worth linking to — an empty
observability tool shows nothing.

### Step 8 — UI on Vercel

```bash
cd web
vercel link                # pick/create the project
vercel env add NEXT_PUBLIC_API_BASE production
#   value: https://tracelens-query.fly.dev
vercel --prod
```

`NEXT_PUBLIC_API_BASE` is inlined into the client bundle **at build time**,
so changing it later needs a redeploy, not just an env update.

### Step 9 — point the README at it

Update the live-demo link in `README.md` and the repository's Website
field:

```bash
gh repo edit --homepage "https://<your-vercel-domain>"
```

### Cost and lifetime, honestly

- Fly: five apps on `shared-cpu-1x` plus a 10GB volume. Small, not free.
  Scaling the demo to zero is not an option for the broker or assembler
  (a stopped consumer means a backlog to reprocess before anything recent
  is queryable), so this accrues continuously.
- ClickHouse Cloud free tier idles services after inactivity; the first
  query after an idle period is slow while it wakes.
- The demo generates data continuously at 3 rps. With the deployed
  sampling policy (5% baseline, 100% of errors) and the TTLs in migration
  0006, storage stays bounded without intervention.

---

## GitHub Actions

`.github/workflows/deploy.yml` runs on a `v*` tag: it builds and pushes all
four images to GHCR, then deploys each Fly app.

Required repository secret:

| Secret | How to get it |
|---|---|
| `FLY_API_TOKEN` | `fly tokens create deploy -x 999999h` |

```bash
gh secret set FLY_API_TOKEN --body "$(fly tokens create deploy -x 999999h)"
git tag v1.0.0 && git push origin v1.0.0
```

GHCR needs no secret: the workflow authenticates with the built-in
`GITHUB_TOKEN`.

---

## Cold storage tiering

`cmd/coldexport` exports partitions older than a cutoff to Parquet on
S3-compatible storage, then optionally drops them locally. See
[RUNBOOK.md](RUNBOOK.md#cold-storage-tiering) for the restore path — which
is tested, not assumed.
