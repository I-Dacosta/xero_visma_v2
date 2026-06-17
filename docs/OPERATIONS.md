# Operations

`xero_service_v2` is a **stateless one-shot CLI job**: each invocation fetches raw
Xero pages and writes them verbatim to GCS, then exits. There is no Postgres,
Redis, BigQuery client, or HTTP server, and no migrations to run. Operating it
means (1) setting the env vars, (2) scheduling the layered `xero sync` cadence,
and (3) running the one-time backfill.

## Environment variables

| Var                                 | Required | Default            | Notes                                                |
|-------------------------------------|----------|--------------------|------------------------------------------------------|
| `XERO_CONNECTION_TYPE`              | no       | `custom`           | Custom Connection is mandatory; only `custom` is accepted |
| `XERO_ORG_N_CLIENT_ID` / `_CLIENT_SECRET` / `_TENANT_ID` / `_NAME` | yes (per org) | — | numbered custom-connection blocks, N = 1, 2, … |
| `XERO_CC_CLIENT_ID` / `_CLIENT_SECRET` / `_TENANT_ID` / `_TENANT_NAME` | alt | — | single-org form (instead of `XERO_ORG_1_*`) |
| `XERO_CUSTOM_CONNECTION_ORG_INDEX`  | no       | `1`                | which numbered org is "primary"                      |
| `XERO_SCOPES`                       | no       | (see `.env.example`) | space-separated OAuth scopes for the custom connection |
| `GCS_BUCKET`                        | yes¹     | —                  | destination bucket (per env: `xero-raw-dev` / `xero-raw-prod`) |
| `GCS_PREFIX`                        | no       | `raw/xero`         | object key prefix; must not start with `_manifests`  |
| `GOOGLE_APPLICATION_CREDENTIALS`    | yes¹     | —                  | path to the GCS service-account JSON                 |
| `SYNC_WINDOW_DAYS`                  | no       | `3`                | incremental modified-window look-back                |
| `SYNC_ROLLING_FULL_TIGHT_DAYS`      | no       | `30`               | `--full-with-window` (weekly) window size            |
| `SYNC_ROLLING_FULL_WIDE_DAYS`       | no       | `90`               | rolling-full (wide, monthly) window size             |
| `SYNC_MAX_CONCURRENT`               | no       | `6`                | max concurrent (tenant, entity) tasks; clamped to 1..=6 |
| `RUST_LOG`                          | no       | `info,xero_cli=info,…` | tracing-subscriber filter                        |

¹ `GCS_BUCKET` and `GOOGLE_APPLICATION_CREDENTIALS` are required for a real
upload. A `--dry-run` writes to local disk and needs neither.

## CLI surface

```text
xero sync [flags]      Fetch raw Xero pages and land them in GCS (or local disk)
xero healthcheck       Mint a token per tenant; --check-bucket also probes GCS
```

Mode-selecting flags (precedence top→bottom): `--backfill FROM:TO`, `--reports`
(+ `--as-of`), `--where <expr>` / `--no-window`, `--business-from` /
`--business-to`, `--full-with-window`, `--full`, else incremental. Plus
`--window-days`, `--chunk-months`, `--entity <csv>`, `--tenant <csv>`,
`--max-concurrent`, `--dry-run`, `--local-dir`.

## Layered sync cadence

A single rolling pull does not keep the bucket complete on its own (hard deletes
never re-enter a modified window; rare linked changes don't bump a parent's
`UpdatedDateUTC`). Run **layered** jobs at different cadences — each lands the
same append-only GCS layout, tagged via the `x-sync-type` object metadata.

| Layer                  | Cadence  | Command                                                        | Catches |
|------------------------|----------|---------------------------------------------------------------|---------|
| incremental            | every 4h | `xero sync --window-days 3`                                    | churn, closes, voids |
| open-sweep             | daily    | `xero sync --where 'Status=="AUTHORISED"' --no-window`         | open items unchanged > 3d |
| master                 | daily    | `xero sync --full`                                             | master data (cheap, full) |
| reports                | daily    | `xero sync --reports --as-of <today>`                         | point-in-time snapshots |
| rolling-full (tight)   | weekly   | `xero sync --full-with-window`                                | recent hard-deletes, drift |
| rolling-full (wide)    | monthly  | `xero sync --business-from <today-90d> --business-to <today>` | drift in days 31–90 |
| backfill               | once     | `xero sync --backfill 2020-01-01:<today> --chunk-months 1`    | full history |

The ready-to-install schedule is in
[`deploy/crontab.example`](../deploy/crontab.example). Install it with:

```bash
# edit XERO_BIN / ENV_FILE / LOG_DIR at the top of the file first
crontab deploy/crontab.example
crontab -l        # confirm
```

## One-time backfill

Run the backfill **once** when onboarding a bucket — not on a schedule (it is
expensive and would exhaust the per-tenant rate-limit budget). Pin it to one
tenant at a time:

```bash
xero sync --tenant <tenant-uuid> \
    --backfill 2020-01-01:2026-06-17 \
    --chunk-months 1
```

The range is expanded into business-date chunks (oldest→newest) and run as one
sub-run per chunk; the per-chunk manifests are merged into a single run manifest.

## Live validation

Before wiring the scheduler, validate against a real environment in two steps.

1. **Dry-run to local disk** — proves auth + the object/metadata layout without
   touching GCS:

   ```bash
   xero sync --reports --as-of 2026-06-17 --dry-run --local-dir ./out
   find ./out -type f            # inspect the raw layout + .meta.json sidecars
   ```

2. **Real incremental window against the dev bucket** — proves the GCS upload
   path end-to-end for a single tenant:

   ```bash
   # .env must have GCS_BUCKET=xero-raw-dev + GOOGLE_APPLICATION_CREDENTIALS set
   xero healthcheck --check-bucket          # token mint + bucket auth
   xero sync --window-days 3 --tenant <tenant-uuid>
   ```

   Then confirm objects landed:

   ```bash
   gsutil ls -r gs://xero-raw-dev/raw/xero/<tenant-uuid>/2.0/ | head
   gsutil ls    gs://xero-raw-dev/_manifests/xero/$(date -u +%Y-%m-%d)/
   ```

## Production deploy

Build and push the one-shot job image, then have the host scheduler invoke it.

```bash
docker build -t gcr.io/prj-dw-dev/xero-service-v2:0.1.0 .
docker push     gcr.io/prj-dw-dev/xero-service-v2:0.1.0
```

The container's entrypoint is the `xero` binary; pass the CLI args at run time:

```bash
docker run --rm \
  --env-file /etc/xero/xero.env \
  -v /secrets/gcs-sa.json:/secrets/gcs-sa.json:ro \
  gcr.io/prj-dw-dev/xero-service-v2:0.1.0 \
  sync --window-days 3
```

Schedule it with whatever the host provides:

- **VPS / bare host** — cron via [`deploy/crontab.example`](../deploy/crontab.example)
  (host binary or `docker run`).
- **GCP** — a **Cloud Run Job** (not a Service) per layer, triggered by **Cloud
  Scheduler** on the cadence above. The job's service account needs
  `roles/storage.objectCreator` on the bucket.

The image is non-root (uid 10001 `xero`) and uses `tini` as PID 1 so SIGTERM
cancels the job cleanly. It exits 0 on success, non-zero if any layer-level check
fails (per-entity errors are isolated and recorded in the manifest, not fatal).

## GCS bucket setup (one-time per environment)

```bash
# bucket per environment
gsutil mb -l europe-north1 gs://xero-raw-dev

# runtime SA needs object-create (and object-view for healthcheck --check-bucket)
gsutil iam ch serviceAccount:xero-runner@prj-dw-dev.iam.gserviceaccount.com:objectCreator gs://xero-raw-dev
gsutil iam ch serviceAccount:xero-runner@prj-dw-dev.iam.gserviceaccount.com:objectViewer  gs://xero-raw-dev
```

Date-partitioned object keys make lifecycle rules trivial — e.g. transition raw
objects to Coldline after 90 days. Manifests live under `_manifests/xero/…`,
outside `GCS_PREFIX`.

## Monitoring (what to watch)

- **Job exit code** per layer (cron/Cloud Scheduler should alert on non-zero).
- **Per-entity errors in the run manifest** (`_manifests/xero/<date>/<run_id>.json`,
  `entities[].error`) — one entity failing does not abort the run.
- **`x-record-count` / page counts** trending to zero where you expect data, or
  spiking unexpectedly.
- **Xero rate-limit headers** in the tracing logs (`info`/`debug`) — daily
  5000-call cap, 60/min per tenant. Stagger cron minutes so layers don't collide.
- **Object freshness in GCS** — most recent `run_date` partition per endpoint
  should advance on schedule.

## Common ops

```bash
# Prove auth for every configured tenant (and bucket wiring):
xero healthcheck --check-bucket

# Re-run a single layer for one tenant after a failure (safe — append-only):
xero sync --window-days 3 --tenant <tenant-uuid>

# Re-pull a specific entity only:
xero sync --window-days 7 --entity invoices,payments --tenant <tenant-uuid>

# Inspect the latest manifest:
gsutil cat gs://xero-raw-dev/_manifests/xero/$(date -u +%Y-%m-%d)/*.json | head
```

## Troubleshooting

| Symptom                                           | Likely cause / fix                                            |
|---------------------------------------------------|--------------------------------------------------------------|
| `config error — check your .env file`             | Missing `XERO_ORG_N_*` / `XERO_CC_*`; custom connection not configured |
| `GCS config error (set GCS_BUCKET)`               | `GCS_BUCKET` unset for a real run (dry-run does not need it)  |
| `failed to build GCS sink …`                      | `GOOGLE_APPLICATION_CREDENTIALS` path wrong or SA lacks bucket access |
| `GCS_PREFIX must not start with '_manifests'`     | Manifests are reserved; choose another prefix (default `raw/xero`) |
| `requested tenant not configured: <id>`           | `--tenant` value isn't among the configured `XERO_ORG_N_*` tenants |
| `report entity X is only valid with --reports`    | Pass report entities only under `--reports`                   |
| `401` on a report/entity                          | Custom Connection grant can't reach it (Xero platform constraint); drop it |
| 429 / daily-limit storms                          | Too many layers overlapping; stagger cron minutes, lower `SYNC_MAX_CONCURRENT` |
