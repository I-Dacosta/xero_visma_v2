# HTTP API reference

Base URL: `http://localhost:5002` (dev) or whatever `XERO_HTTP_BIND` is in prod.

All response bodies are JSON. Error shape: `{"error": "<message>"}`.

## Health

| Method | Path        | Notes                                       |
|--------|-------------|---------------------------------------------|
| GET    | `/health`   | Lightweight liveness probe                  |
| GET    | `/readyz`   | Readiness: checks Postgres reachability     |

## Auth (PKCE only — Custom Connection skips these)

| Method | Path                      | Notes                                                  |
|--------|---------------------------|--------------------------------------------------------|
| GET    | `/api/xero/login`         | Redirect to Xero authorise endpoint                    |
| GET    | `/api/xero/callback`      | OAuth redirect target; exchanges code for tokens       |

## Sync — single entity

`POST /sync/:tenant/:entity`

```json
{
  "modified_after":      "2026-05-04T00:00:00Z",
  "modified_before":     "2026-05-11T00:00:00Z",
  "business_date_after":  "2026-05-04",
  "business_date_before": "2026-05-11",
  "job_type":            "manual",
  "triggered_by":        "ops-jane",
  "advance_watermark":   false
}
```

All fields optional. Use either `modified_*` (incremental) or `business_date_*`
(backfill) — never both populated. Response: `{status, run_id, trigger_id}`.

## Sync — batch (multiple entities, one trigger_id)

`POST /tenants/:tenant/sync/batch`

```json
{
  "entities": ["invoices","payments","journals"],
  "business_date_after":  "2026-05-04",
  "business_date_before": "2026-05-11",
  "advance_watermark": false
}
```

Response: `{status, trigger_id, results: [{entity, run_id, status, error?}, ...]}`.

## Bronze + observability

| Method | Path                                        | Returns                                   |
|--------|---------------------------------------------|-------------------------------------------|
| GET    | `/tenants/:tenant/bronze/summary`           | per-entity row counts + `distinct_runs`   |
| GET    | `/tenants/:tenant/checkpoints`              | watermarks per entity                     |
| GET    | `/tenants/:tenant/runs?limit=50&entity=…`   | recent `sync_run` rows                    |

## Schedules

| Method | Path                                              | Notes                                |
|--------|---------------------------------------------------|--------------------------------------|
| POST   | `/tenants/:tenant/schedules`                      | Body `{name, cron_expression, entities, from_date?, to_date?}` |
| GET    | `/tenants/:tenant/schedules`                      | List enabled schedules               |
| POST   | `/tenants/:tenant/schedules/:id/trigger`          | Manual fire (optional date overrides)|
| DELETE | `/tenants/:tenant/schedules/:id`                  | Soft-disable                         |

The **cron daemon** fires schedules automatically; the trigger endpoint is for
on-demand replays.

## Backfill

| Method | Path                                          | Purpose                                |
|--------|-----------------------------------------------|----------------------------------------|
| POST   | `/tenants/:tenant/backfill`                   | Start a plan (creates chunks)          |
| GET    | `/tenants/:tenant/backfill`                   | List recent plans                      |
| GET    | `/tenants/:tenant/backfill/:plan_id`          | Plan + chunk states                    |
| POST   | `/tenants/:tenant/backfill/:plan_id/cancel`   | Mark pending chunks as skipped         |
| POST   | `/backfill/run-next`                          | (Admin) Synchronously drain one chunk  |

Start-plan body:

```json
{
  "entities":        ["invoices","payments","journals","bank_transactions"],
  "start_date":      "2023-05-11",
  "end_date":        "2026-05-11",
  "chunk_size_days": 30,
  "triggered_by":    "ops-jane"
}
```

Returns `{status, plan, chunks_created}`. The worker picks up pending chunks
automatically — no further calls needed.

## BigQuery replay

`POST /tenants/:tenant/bq/replay`

```json
{ "entity_type": "bank_transactions", "limit": 500 }
```

Pushes bronze rows whose `bq_synced_at IS NULL` to BigQuery and marks them.
Use to bootstrap warehouse from historical bronze data, or recover after a
BQ outage. Response: `{status, candidates, accepted, remaining}`.
