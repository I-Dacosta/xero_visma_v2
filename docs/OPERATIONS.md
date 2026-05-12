# Operations

## Environment variables

| Var                                 | Required | Default                            | Notes                                                |
|-------------------------------------|----------|------------------------------------|------------------------------------------------------|
| `DATABASE_URL`                      | yes      | —                                  | `postgresql://user:pw@host:port/db`                  |
| `REDIS_URL`                         | no       | `redis://localhost:6380/0`         | token cache + rate-limit signal                      |
| `XERO_HTTP_BIND`                    | no       | `0.0.0.0:5002`                     | listen address                                       |
| `XERO_CONNECTION_TYPE`              | no       | `pkce`                             | `pkce` or `custom`                                   |
| `XERO_CLIENT_ID` / `XERO_CLIENT_SECRET` | PKCE | —                              | shared OAuth app credentials                         |
| `XERO_REDIRECT_URI`                 | PKCE     | —                                  | callback URL                                         |
| `XERO_SCOPES`                       | PKCE     | (see .env.example)                 | space-separated scopes                               |
| `XERO_CC_CLIENT_ID` / `_SECRET` / `_TENANT_ID` / `_TENANT_NAME` | custom | — | single-tenant custom-connection         |
| `XERO_ORG_N_CLIENT_ID` / `_SECRET` / `_TENANT_ID` / `_NAME` | custom (multi) | — | numbered aliases, N=1,2,…             |
| `XERO_CUSTOM_CONNECTION_ORG_INDEX`  | custom   | `1`                                | which numbered org is "primary"                      |
| `XERO_RATE_LIMIT_PER_MINUTE`        | no       | `60`                               | informational (the limiter uses Xero headers)        |
| `XERO_MAX_PAGES_PER_ENTITY`         | no       | `5000`                             | hard cap; raise for very large entities              |
| `GCP_PROJECT_ID`                    | BQ       | `prj-dw-dev`                       | enables BQ sink when all 3 BQ vars are present       |
| `BIGQUERY_DATASET` / `GCP_DATASET_ID` | BQ     | `dw_1_bronze_xero`                 |                                                      |
| `GOOGLE_APPLICATION_CREDENTIALS`    | BQ       | `/app/gcp-credentials.json`        | path to SA JSON                                      |
| `RUST_LOG`                          | no       | `info,xero_cli=debug,xero_state=debug` | tracing-subscriber filter                        |

## Local dev

```bash
docker compose up -d           # starts pg, redis, xero-server
docker compose logs -f xero-server
curl http://localhost:5002/health
```

Override file [`docker-compose.override.yml`](../docker-compose.override.yml)
remaps Postgres → `:5435` and Redis → `:6382` to avoid colliding with other
local stacks (lago, etc), and mounts the BQ SA JSON read-only.

## Production deploy (Cloud Run / GKE)

```bash
docker build -t gcr.io/prj-dw-dev/xero-service-v2:0.1.0 .
docker push     gcr.io/prj-dw-dev/xero-service-v2:0.1.0
gcloud run deploy xero-service-v2 \
  --image gcr.io/prj-dw-dev/xero-service-v2:0.1.0 \
  --region europe-north1 \
  --service-account xero-runner@prj-dw-dev.iam.gserviceaccount.com \
  --set-env-vars XERO_CONNECTION_TYPE=custom,GCP_PROJECT_ID=prj-dw-dev,BIGQUERY_DATASET=dw_1_bronze_xero \
  --set-secrets   XERO_ORG_1_CLIENT_SECRET=xero-cc-1:latest,XERO_ORG_2_CLIENT_SECRET=xero-cc-2:latest \
  --add-cloudsql-instances prj-dw-dev:europe-north1:xero-pg
```

Image is non-root (uid 10001 `xero`), uses `tini` as PID 1, exposes 5002.

## BigQuery provisioning (one-time per environment)

```bash
GCP_PROJECT_ID=prj-dw-dev \
BIGQUERY_DATASET=dw_1_bronze_xero \
BQ_LOCATION=europe-north1 \
./tooling/bq_provision.sh
```

Creates the dataset and 28 entity tables (day-partitioned on `last_seen_at`,
clustered on `tenant_id`). Idempotent.

The service account used at runtime needs:

- `roles/bigquery.dataEditor` on the dataset
- `roles/bigquery.jobUser` on the project

## Migrations

`xero db-migrate` runs at every server start and is idempotent. Migration files
are in [`migrations/`](../migrations); ordering is by filename prefix
(`0001_…`, `0002_…`, …).

## Monitoring (what to watch)

- **run_history.status = 'failed'** rate (`/tenants/:t/runs`)
- **X-DayLimit-Remaining** in tracing logs (Xero daily 5000-call cap)
- **backfill_plan.failed_chunks** trending up
- **bronze rows with bq_synced_at IS NULL** — should converge to ~0 in steady state

A future `/metrics` Prometheus endpoint is on the roadmap; for now, scrape
logs and the SQL tables.

## Common ops

```bash
# Manual sync of one entity:
curl -XPOST http://host:5002/sync/$T/invoices -H 'Content-Type: application/json' \
  -d '{"modified_after":"2026-05-04T00:00:00Z"}'

# Cancel an in-flight backfill:
curl -XPOST http://host:5002/tenants/$T/backfill/$PLAN/cancel

# Push un-synced bronze rows to BQ:
curl -XPOST http://host:5002/tenants/$T/bq/replay -d '{"limit":2000}'

# Disable a schedule:
curl -XDELETE http://host:5002/tenants/$T/schedules/$SCHED_ID
```

## Troubleshooting

| Symptom                                           | Likely cause / fix                                  |
|---------------------------------------------------|------------------------------------------------------|
| `BigQuery sink: SA JSON not found …`              | Bind-mount path wrong; check absolute path quoting    |
| `Not found: Dataset prj-dw-dev:dw_1_bronze_xero`  | Run `tooling/bq_provision.sh`                         |
| `401 PaymentServices`                             | Custom Connection can't access PaymentServices — drop |
| 429 storms on multi-pod deploy                    | Confirm Redis is reachable from all pods             |
| Bronze grows but BQ does not                      | `POST /tenants/:t/bq/replay` to bootstrap historical  |
| Cron schedule never fires                         | Check `cron_expression` syntax with `cron::Schedule::from_str` |
