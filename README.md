# xero_service_v2

Rust-core ERP-connection service for Xero. Pulls Accounting API entities into a
local Postgres bronze layer and streams them onward to BigQuery for the
warehouse. Multi-tenant, custom-connection-aware, designed for unattended
3-year backfills and steady-state incremental sync.

## At a glance

| Concern             | Choice                                                       |
|---------------------|--------------------------------------------------------------|
| Language            | Rust (workspace of 7 crates) + small Python tooling          |
| HTTP                | Axum 0.7 on `0.0.0.0:5002`                                   |
| Source of truth     | Postgres schema `xero` (Cloud SQL in prod)                   |
| Runtime cache       | Redis (token cache + cross-pod rate-limit signal)            |
| Data warehouse      | BigQuery dataset `dw_1_bronze_xero` (one table per entity)   |
| Auth modes          | PKCE OAuth (multi-tenant) or Custom Connection (per-org)     |
| Rate limit / retry  | Adaptive header-driven throttle + exp backoff with jitter    |
| Backfill            | Plan/chunk orchestrator with `FOR UPDATE SKIP LOCKED` worker |
| Cron                | In-process daemon parsing `sync_schedule` cron expressions   |

## Docs

- **Architecture:** [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — crates, data flow, state machine
- **API reference:** [docs/API.md](docs/API.md) — every endpoint, body, response
- **Operations:** [docs/OPERATIONS.md](docs/OPERATIONS.md) — deploy, env vars, BQ provisioning, monitoring
- **Backfill guide:** [docs/BACKFILL.md](docs/BACKFILL.md) — running a 3-year backfill safely
- **Layer system (legacy):** [docs/layeSystem/](docs/layeSystem/) — Layer 1 REST / Layer 2 MCP context

## Quick start (local)

```bash
# Bring up Postgres + Redis + xero-server (compiles on first boot)
docker compose up -d

# Follow logs
docker compose logs -f xero-server

# Health
curl http://localhost:5002/health        # {"status":"ok",...}
curl http://localhost:5002/readyz        # {"postgres":"up",...}
```

The local HTTP server listens on `http://localhost:5002`. Postgres on `5435`,
Redis on `6382` (offset from defaults to avoid collisions — see
[docker-compose.override.yml](docker-compose.override.yml)).

## Auth in one paragraph

PKCE OAuth is the default for human-authorised multi-tenant flows. For
unattended ingestion set `XERO_CONNECTION_TYPE=custom` and supply either
`XERO_CC_CLIENT_ID`/`XERO_CC_CLIENT_SECRET`/`XERO_CC_TENANT_ID` or numbered
aliases (`XERO_ORG_1_*`, `XERO_ORG_2_*`, …). Select which numbered org is
"primary" with `XERO_CUSTOM_CONNECTION_ORG_INDEX`. The service registers every
configured custom-connection tenant on startup and refreshes their
client-credentials tokens via Redis.

## What gets synced

`EntityType::all()` covers 28 Accounting-API entities (Invoices, Payments,
Contacts, Journals, …). See `core/crates/xero-common/src/types.rs`. Three
entities are intentionally outside that list:

- `bills` — alias for `Invoices` with `Type=ACCPAY`
- `employees` — Payroll API, not Accounting
- `payment_services` — supported by `EntityType` but Custom-Connection grants
  cannot reach it (Xero platform constraint); the run will fail with 401

## Layout

```text
xero_service_v2/
├── core/crates/
│   ├── xero-common/    # Shared types, errors, config
│   ├── xero-auth/      # OAuth/PKCE + Custom-Connection + token cache
│   ├── xero-client/    # REST client, retry, rate-limit
│   ├── xero-state/     # Postgres + Redis; bronze, checkpoints, runs,
│   │                   #   schedules, backfill, BQ sink
│   ├── xero-sync/      # Orchestrates fetch → bronze → checkpoint advance
│   ├── xero-http/      # Axum router, handlers, backfill + cron daemons
│   └── xero-cli/       # `xero` binary: serve, db-migrate, healthcheck
├── migrations/         # sqlx migrations (0001…0008)
├── tooling/            # bq_provision.sh and future utilities
├── docs/               # Architecture, API, ops, backfill, layer system
├── Dockerfile          # Multi-stage prod image (rust:1.80 → debian-slim)
├── docker-compose.yml  # Local dev stack
└── README.md           # this file
```
