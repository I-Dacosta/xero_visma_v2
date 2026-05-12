# Architecture

## Crate map

```
xero-cli ──► xero-http ──► xero-sync ──► xero-client ──► Xero REST API
                │              │              │
                │              │              └──► xero-auth (token cache)
                │              ▼
                │           xero-state ──► Postgres (bronze, runs, …)
                │                       └► Redis (tokens, rate-limit signal)
                │                       └► BigQuery (streaming inserts)
                ▼
              xero-state (admin commands)
```

| Crate          | Responsibility                                                                |
|----------------|--------------------------------------------------------------------------------|
| `xero-common`  | `EntityType`, `TenantId`, `AppConfig`, `Error`, `Result`                       |
| `xero-auth`    | PKCE flow, custom-connection client-credentials, Redis-backed `TokenCache`     |
| `xero-client`  | Xero REST GETs, pagination, retry, rate-limit coordinator                      |
| `xero-state`   | Postgres CRUD for every persistent table; BigQuery sink                        |
| `xero-sync`    | `SyncExecutor::run_with_options` — fetch → bronze → checkpoint                 |
| `xero-http`    | Axum router, handlers, **backfill worker**, **cron daemon**                    |
| `xero-cli`     | `xero` binary entrypoint (`serve`, `db-migrate`, `db-check`, `healthcheck`)    |

## Data flow — one sync run

```
HTTP POST /sync/:t/:e
        │
        ▼
xero-http::sync_handler ──► xero-sync::run_with_options
                                    │
                                    │  1. start_run() ─ run_history row
                                    │  2. load checkpoint, derive modified_after
                                    │  3. xero-client::fetch_*  (pages w/ retry)
                                    │  4. local_bronze::upsert_records
                                    │       └─► BqSink::insert (best effort)
                                    │  5. checkpoint::upsert
                                    │       └─► compute_next_watermark(...)
                                    │  6. finish_run() ─ status + counts
                                    ▼
                            run_history + bronze + checkpoint updated
```

## Persistent tables (Postgres schema `xero`)

| Table                    | Purpose                                                |
|--------------------------|--------------------------------------------------------|
| `tenants`                | Registered Xero organisations                          |
| `oauth_tokens`           | (Optional) extra OAuth token persistence               |
| `sync_checkpoint`        | Per-(tenant, entity) `last_modified_watermark`         |
| `sync_run`               | Run-history audit log (status, counts, error_message)  |
| `local_bronze_record`    | Idempotent record store (PK `(tenant, entity, id)`)    |
| `sync_schedule`          | Cron entries; the daemon fires these                   |
| `backfill_plan`          | A "sync N entities from date A to B in K-day chunks"   |
| `backfill_chunk`         | One unit of work; worker picks via SKIP LOCKED          |

## Checkpoint logic — `compute_next_watermark`

| Run shape                              | `advance_watermark` | Result                                |
|----------------------------------------|---------------------|----------------------------------------|
| `modified_*` only                      | n/a                 | candidate = `modified_before` or `now()`; monotonic |
| `business_date_*` only                 | `false` (default)   | watermark untouched (gap-safe)         |
| `business_date_*` only                 | `true`              | candidate = `business_date_before` 00:00 UTC; monotonic |

`last_sync_at` always updates on a successful run, regardless of mode.

## Rate-limit + retry (xero-client)

Two layers:

- **Local (in-process):** `TenantRateLimiter` caps in-flight concurrency at 6/tenant,
  reads `X-MinLimit-Remaining` and proactively sleeps when remaining ≤ 5.
- **Distributed (cross-pod):** `RedisRateLimitCoordinator` publishes the
  `Retry-After` on every 429 to a key `xero_rl:{tenant}:pause_until` with
  matching `PX`. All pods consult that key before issuing.

`retry::exp_backoff` is base 500ms × 2^attempt with 25 % jitter, capped at 30 s.
Max 5 attempts per request. 4xx (non-429) is terminal.

## Backfill worker (xero-http/backfill.rs)

1. `claim_next_pending_chunk` — `SELECT … FOR UPDATE SKIP LOCKED LIMIT 1`
2. Build `RunOptions` with the chunk's `business_date_*` window
3. `SyncExecutor::run_with_options` (same path as the HTTP handlers)
4. On success → `mark_chunk_succeeded(run_id)`; on error → `mark_chunk_failed(err)`
5. Plan-level counters and final status updated via `bump_plan_counters_and_finalise`

## Cron daemon (xero-http/cron_daemon.rs)

Tokio task ticks every 60 s. For each enabled schedule:

1. Parse `cron_expression` via the `cron` crate
2. Compute next-fire after `last_triggered_at` (or `created_at` if never fired)
3. If `now ≥ next-fire`, **atomic CAS** on `last_triggered_at` (`IS NOT DISTINCT FROM`)
   so only one replica wins
4. Get token → run every entity with default `RunOptions` (incremental from checkpoint)
