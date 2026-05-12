# 3-year backfill runbook

A safe, resumable procedure for bulk-importing historical Xero data into the
warehouse.

## Capacity math

Xero quotas (per tenant):

- 60 calls / minute
- 5000 calls / day
- 10 concurrent (app-wide)

A monthly chunk for one entity is typically **2–10 paginated calls**. So
36 monthly chunks × 28 entities ≈ **~3000 calls per tenant for a 3y backfill** —
fits inside a single day's quota with comfortable headroom.

`MAX_PAGES_PER_ENTITY` defaults to 5000 = 500 000 rows per fetch call. A
monthly chunk should never approach this for any realistic tenant.

## Before you start

1. **Provision BigQuery** (one-time per env):
   ```bash
   ./tooling/bq_provision.sh
   ```

2. **Confirm the SA has perms** in `prj-dw-dev`:
   - `roles/bigquery.dataEditor` on `dw_1_bronze_xero`
   - `roles/bigquery.jobUser` on the project

3. **Confirm Redis is reachable** if running >1 replica (cross-pod 429 signal).

4. **Pick the entities you actually want**:
   - Recommended core: `invoices`, `payments`, `bank_transactions`, `credit_notes`,
     `journals`, `manual_journals`, `purchase_orders`, `quotes`
   - Master data refreshes itself naturally: `contacts`, `accounts`, `items`,
     `tax_rates`, `tracking_categories`
   - Skip `payment_services` if using Custom Connection — it 401s

## Step 1 — create the plan

```bash
T=9dc5d3f0-68b1-4811-a38d-9efbb5990604   # AU
curl -XPOST http://host:5002/tenants/$T/backfill \
  -H 'Content-Type: application/json' \
  -d '{
    "entities": [
      "invoices","payments","bank_transactions","bank_transfers",
      "credit_notes","journals","manual_journals","purchase_orders","quotes"
    ],
    "start_date": "2023-05-11",
    "end_date":   "2026-05-11",
    "chunk_size_days": 30,
    "triggered_by": "ops-3y-backfill"
  }'
```

Response includes `plan_id` and `chunks_created` (~9 entities × 37 sub-windows
= ~333 chunks). The background worker starts picking them up immediately.

## Step 2 — monitor

```bash
# Plan + per-chunk status
curl http://host:5002/tenants/$T/backfill/$PLAN_ID | jq '.plan'

# Per-entity bronze row counts
curl http://host:5002/tenants/$T/bronze/summary | jq

# Recent run failures
curl 'http://host:5002/tenants/$T/runs?limit=200' \
  | jq '.runs | map(select(.status=="failed")) | .[] | {entity_type, error_message}'
```

The worker logs every chunk to `xero_http::backfill`. Tail with:

```bash
docker compose logs -f xero-server | grep backfill
```

## Step 3 — handle failures

A chunk that fails is retried up to `max_attempts=3` automatically. Terminal
failures stay in `backfill_chunk` with `status='failed'` and an error message.

To re-try after fixing the root cause:

```sql
UPDATE xero.backfill_chunk
SET status='pending', attempt_count=0, error_message=NULL
WHERE plan_id='<plan_id>' AND status='failed';
```

The worker will pick them up on its next poll (~5 s).

## Step 4 — verify warehouse

Streaming inserts to BQ happen inline with bronze writes. To confirm parity:

```sql
-- BigQuery
SELECT COUNT(*) FROM `prj-dw-dev.dw_1_bronze_xero.Invoices`
WHERE tenant_id = '9dc5d3f0-...';

-- Postgres
SELECT COUNT(*) FROM xero.local_bronze_record
WHERE tenant_id = '9dc5d3f0-...' AND entity_type = 'invoices';
```

Numbers should match. If BQ trails Postgres, replay the gap:

```bash
curl -XPOST http://host:5002/tenants/$T/bq/replay -d '{"limit":5000}'
```

Repeat until `remaining=0`.

## Step 5 — switch to incremental

After backfill completes, the daily cron (`sync_schedule`) takes over with
default `RunOptions` (no `business_date_*`), which advances the modified-time
watermark monotonically. The first cron run after the backfill will pick up
anything modified in the gap between the backfill's `end_date` and `now()`.

## Recovering from a partial run

`backfill_chunk` is the source of truth. Cancel and restart with a tighter
window:

```bash
curl -XPOST http://host:5002/tenants/$T/backfill/$PLAN_ID/cancel
curl -XPOST http://host:5002/tenants/$T/backfill \
  -d '{"entities":["invoices"],"start_date":"2024-01-01","end_date":"2024-06-01","chunk_size_days":30}'
```

The orchestrator is idempotent — re-running a chunk re-upserts the same bronze
rows by `(tenant, entity, record_id)` PK.
