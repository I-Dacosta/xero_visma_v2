# xero_service_v2

Stateless **raw → GCS uploader** for Xero. The `xero` CLI fetches Xero Accounting
API pages and writes the **verbatim** response bytes to a GCS bucket (one object
per page), then exits. All dedup / merge / modeling happens downstream
(Cloud Function → BigQuery → Dataform). Multi-tenant, custom-connection auth,
driven by an external scheduler.

There is **no** Postgres, Redis, BigQuery client, or HTTP server in this service —
it is a one-shot job, not a daemon.

## At a glance

| Concern             | Choice                                                       |
|---------------------|--------------------------------------------------------------|
| Language            | Rust (workspace of 6 crates)                                 |
| Entry point         | `xero` CLI — one-shot job invoked by cron / scheduler        |
| Destination         | GCS bucket (raw verbatim pages + `.meta.json` sidecars)      |
| State               | None — append-only objects; downstream BigQuery dedups       |
| Auth                | Custom Connection (`client_credentials`) only — no PKCE      |
| Incremental         | Rolling `now − N days` window, computed per run              |
| Rate limit / retry  | Adaptive header-driven throttle + exp backoff with jitter    |
| Backfill            | `--backfill FROM:TO` expanded into business-date chunks      |
| Cadence             | Host scheduler (see [deploy/crontab.example](deploy/crontab.example)) |

## Docs

- **New architecture:** [docs/NEW_ARCHITECTURE_RAW_GCS.md](docs/NEW_ARCHITECTURE_RAW_GCS.md) — design, GCS layout, metadata, cadence rationale
- **Operations:** [docs/OPERATIONS.md](docs/OPERATIONS.md) — env vars, cadence table, deploy, backfill, live validation
- **Cron cadence:** [deploy/crontab.example](deploy/crontab.example) — the layered schedule, ready to install

## CLI surface

```text
xero sync [flags]      Fetch raw Xero pages and land them in GCS (or local disk).
xero healthcheck       Mint a token per tenant; --check-bucket also probes GCS.
```

The sync *mode* is derived from the flags (precedence top→bottom):

| Flag(s)                                | Mode             |
|----------------------------------------|------------------|
| `--backfill FROM:TO`                   | backfill (business-date chunks) |
| `--reports` (needs `--as-of`)          | report snapshots |
| `--where <expr>` / `--no-window`       | open-sweep (status filter, no window) |
| `--business-from` / `--business-to`    | rolling-full (explicit business window) |
| `--full-with-window`                   | rolling-full (configured tight window) |
| `--full`                               | master (no filter, master entities) |
| _(default)_                            | incremental (modified ≥ now − window) |

Other flags: `--window-days N`, `--chunk-months N`, `--as-of YYYY-MM-DD`,
`--entity <csv>`, `--tenant <csv>`, `--max-concurrent N`, `--dry-run`,
`--local-dir <path>`.

## GCS object layout

```text
object:   {GCS_PREFIX}/{tenant}/2.0/{endpoint}/{YYYY-MM-DD}/{ts}_{run_id_short}_p{NNN}.json
report:   {GCS_PREFIX}/{tenant}/2.0/{endpoint}/{YYYY-MM-DD}/{ts}_{run_id_short}__{period_key}.json
sidecar:  <object-key>.meta.json
manifest: _manifests/xero/{YYYY-MM-DD}/{run_id}.json      (OUTSIDE GCS_PREFIX)
```

`GCS_PREFIX` defaults to `raw/xero`. The object body is the exact bytes Xero
returned for that page (envelope intact). Custom object metadata (lowercase
string keys) records `x-vendor`, `x-tenant-id`, `x-org-name`, `x-endpoint`,
`x-api-version`, `x-sync-type`, `x-page`, `x-record-count`, `x-http-status`,
`x-run-id`, `x-synced-at`, plus window/report keys as applicable. See
[docs/NEW_ARCHITECTURE_RAW_GCS.md](docs/NEW_ARCHITECTURE_RAW_GCS.md) for the full
metadata table.

## Quick start (local dry-run, no GCS needed)

A dry-run mirrors the production object layout onto local disk — no bucket, no
GCS credentials required (only Xero custom-connection creds).

```bash
cp .env.example .env        # fill in XERO_ORG_1_* (and friends)

# Build + run a dry-run of the report snapshots into ./out
cargo run -p xero-cli -- sync --reports --as-of 2026-06-17 \
    --dry-run --local-dir ./out

# Inspect the raw object layout
find ./out -type f
```

Or via docker compose (writes to `./out`):

```bash
docker compose run --rm xero sync --reports --as-of 2026-06-17 \
    --dry-run --local-dir /out
```

## Auth in one paragraph

Custom Connection only. Each Xero organisation has its own
`client_credentials` app; configure numbered blocks
`XERO_ORG_N_{CLIENT_ID,CLIENT_SECRET,TENANT_ID,NAME}` (or the single-org
`XERO_CC_*` form). `client_credentials` issues no refresh token, so there is
nothing to persist — tokens are minted in-memory per run. PKCE is intentionally
unsupported (it would require a token store, which contradicts the stateless
model).

## What gets synced

`EntityType::all()` covers the Accounting-API entities (Invoices, Payments,
Contacts, Journals, …). The default entity set depends on the sync mode:

- **incremental / backfill** — full accounting set (reports excluded)
- **master** (`--full`) — `accounts`, `contacts`, `items`, `tax_rates`,
  `tracking_categories`, `currencies`, `organisations`, `users`, `branding_themes`
- **open-sweep** (`--where … --no-window`) — `invoices`, `bills`
- **rolling-full** — transactional entities (all minus master)
- **reports** (`--reports`) — the report endpoints (per-contact aged reports are
  excluded by default; aging is derived downstream)

See `core/crates/xero-common/src/types.rs` for the authoritative list.

## Layout

```text
xero_service_v2/
├── core/crates/
│   ├── xero-common/    # Shared types, errors, config (GCS + sync config)
│   ├── xero-auth/      # Custom-Connection client_credentials, in-memory token
│   ├── xero-client/    # REST client, retry, rate-limit, raw-page capture
│   ├── xero-gcs/       # RawSink trait, GcsRawSink, LocalDirSink, manifest
│   ├── xero-sync/      # SyncJob: fetch → upload raw (stateless)
│   └── xero-cli/       # `xero` binary: sync, healthcheck
├── deploy/             # crontab.example (layered cadence)
├── docs/               # New architecture, operations, transformation plan
├── Dockerfile          # Multi-stage one-shot job image (rust:1.80 → debian-slim)
├── docker-compose.yml  # Local one-shot / dry-run convenience
├── .env.example        # Env var template (no secrets)
└── README.md           # this file
```
