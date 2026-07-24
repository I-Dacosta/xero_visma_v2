# Phase 7 — Testing & Deployment

## Status

Active.

## Scope

End-to-end verification and production deployment of xero_service_v2.  
Covers: unit tests, integration tests, HTTP-layer smoke tests, BigQuery parity checks,
backfill verification, edge-case matrix, and the step-by-step deploy checklist for Cloud Run.

---

## 7.1  Test Layers

| Layer | What it covers | Tool |
|---|---|---|
| Unit | Pure logic: watermark rules, config parsing, backfill chunk enumeration, rate-limit math | `cargo test --lib` |
| Integration | DB-level: bronze upsert idempotency, checkpoint CAS, backfill state machine | `cargo test --test integration` (needs Postgres) |
| HTTP smoke | API contract: request/response shapes, auth rejection, 404 routing | `curl` / `httpie` against a running container |
| BQ parity | Row counts match between Postgres bronze and BigQuery tables | `bq query` + `psql` |
| Load / rate-limit | 429 broadcast reaches all pods; adaptive throttle activates | Redis + multi-replica compose override |

---

## 7.2  Unit Test Checklist

Run from the workspace root:

```bash
cargo test 2>&1 | tail -30
```

### 7.2.1  Must-pass suites

| Suite | Location | Key assertions |
|---|---|---|
| `compute_next_watermark` | `xero-sync/src/lib.rs` | modified-only advances to `modified_before`; business-date without opt-in keeps `existing_wm`; monotonicity on both paths; first-ever run seeds watermark |
| `AppConfig::from_env` | `xero-common/src/config.rs` | PKCE mode loads `XERO_CLIENT_ID`; custom mode loads `XERO_ORG_N_*`; org-index selector picks correct tenant; multi-org collects all connections; missing required var returns `Error::Config` |
| `enumerate_chunks` | `xero-state/src/backfill.rs` | 30-day window produces 1 chunk; 31-day produces 2; multiple entities multiply chunk count; start=end produces 0 chunks |
| `exp_backoff` | `xero-client/src/retry.rs` | attempt 0 ≤ 500 ms; attempt 4 ≤ 30 s (cap); jitter ensures two calls differ |
| `DateWindow` | `xero-client/src/lib.rs` | `start < end` accepted; `start == end` rejected; `start > end` rejected |

### 7.2.2  Edge cases in unit tests

```rust
// Watermark must not regress even if modified_before is in the past
let prev = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
let old  = Utc.with_ymd_and_hms(2026, 5, 1, 0, 0, 0).unwrap();
let next = compute_next_watermark(Some(prev), &opt_modified(Some(old)));
assert_eq!(next, Some(prev));   // must keep the later watermark

// Business-date advance_watermark with business_date_before == existing_wm (boundary)
let same = Utc.with_ymd_and_hms(2026, 5, 11, 0, 0, 0).unwrap();
let opts = opt_business_date(NaiveDate::from_ymd_opt(2026, 5, 4).unwrap(),
                             NaiveDate::from_ymd_opt(2026, 5, 11).unwrap(), true);
let next = compute_next_watermark(Some(same), &opts);
assert_eq!(next, Some(same));   // equal is fine, no regression

// XERO_CUSTOM_CONNECTION_ORG_INDEX = 0 must be rejected
let err = AppConfig::from_pairs([
    ("DATABASE_URL", "postgresql://localhost/xero"),
    ("XERO_CONNECTION_TYPE", "custom"),
    ("XERO_CUSTOM_CONNECTION_ORG_INDEX", "0"),
    ("XERO_ORG_1_CLIENT_ID", "id"),
    ("XERO_ORG_1_CLIENT_SECRET", "secret"),
    ("XERO_ORG_1_TENANT_ID", "tenant"),
]).unwrap_err();
assert!(err.to_string().contains("positive integer"));

// duplicate XERO_CC_* + XERO_ORG_1_* with same tenant_id must not double-count
let cfg = AppConfig::from_pairs([
    ("DATABASE_URL", "postgresql://localhost/xero"),
    ("XERO_CONNECTION_TYPE", "custom"),
    ("XERO_CC_CLIENT_ID", "id"),
    ("XERO_CC_CLIENT_SECRET", "secret"),
    ("XERO_CC_TENANT_ID", "t1"),
    ("XERO_ORG_1_CLIENT_ID", "id"),
    ("XERO_ORG_1_CLIENT_SECRET", "secret"),
    ("XERO_ORG_1_TENANT_ID", "t1"),  // same tenant
]).unwrap();
assert_eq!(cfg.xero_cc_connections.len(), 1);  // deduped by tenant_id
```

---

## 7.3  Integration Test Setup

Integration tests require a live Postgres instance. Use the compose stack:

```bash
# Start only Postgres and Redis (skip the Rust server for now)
docker compose up -d xero-postgres xero-redis

# Run migrations then tests
DATABASE_URL="postgresql://xero_user:xero_dev_password@localhost:5435/xero_v2" \
REDIS_URL="redis://localhost:6382/0" \
cargo test --test integration -- --test-threads=1 2>&1 | tail -40
```

`--test-threads=1` is required: integration tests share a single schema and run cleanup
between cases via `TRUNCATE … RESTART IDENTITY CASCADE`.

### 7.3.1  Integration test cases

These live in `tests/integration/` (create this directory if tests are added):

**Bronze idempotency**
```
Given: upsert_records called with records R for (tenant, entity)
When:  same call repeated with same records
Then:  row count unchanged; no error; `last_seen_at` updated; `first_seen_at` unchanged
```

**Checkpoint monotonicity under concurrent writers**
```
Given: two tokio tasks both calling checkpoint::upsert with (tenant, entity) simultaneously
When:  both complete without error
Then:  checkpoint row exists exactly once; `last_modified_watermark` = max of the two
```

**Backfill claim exclusivity**
```
Given: 10 pending chunks in backfill_chunk for plan P
When:  10 concurrent callers all call claim_next_pending_chunk simultaneously
Then:  each caller receives a distinct chunk (no double-claim); total claimed = 10
```

**Cron CAS — only one pod fires**
```
Given: two identical cron records with same last_triggered_at = T0 (simulates two pods)
When:  both call the CAS UPDATE simultaneously with expected = T0, new = T1
Then:  exactly one UPDATE returns rows_affected=1; the other returns 0
```

**Replay endpoint**
```
Given: N bronze records with bq_synced_at IS NULL (BQ sink inactive / offline)
When:  replay_bq_pending called with limit=N
Then:  returns (candidates=N, accepted=N) when BQ sink mock returns Ok; 
       bq_synced_at set for all accepted rows
```

### 7.3.2  Edge cases

| Scenario | Expected behaviour |
|---|---|
| `upsert_records` with empty `records` vec | Returns `BronzeStats { skipped_invalid: 0 }`, no DB write, no error |
| `upsert_records` where all records lack a recognised PK field | All counted as `skipped_invalid`; run still succeeds with `loaded_count=0` |
| `claim_next_pending_chunk` with no pending chunks | Returns `Ok(None)` |
| `mark_chunk_failed` at `attempt_count = max_attempts` | Sets `status = 'terminal_failed'`, does not reset to pending |
| `checkpoint::upsert` with `last_modified_watermark = None` | Stores NULL; next load returns `None`; does not panic |
| `replay_bq_pending` with BQ returning partial error | Logs warning, returns `(candidates=N, accepted=M)`; rows not accepted keep `bq_synced_at = NULL` |

---

## 7.4  HTTP Smoke Tests

Run against a fully booted stack (`docker compose up -d`):

```bash
H=http://localhost:5002
T_AU=9dc5d3f0-68b1-4811-a38d-9efbb5990604
T_NZ=<nz-tenant-uuid>
```

### 7.4.1  Health & auth guard

```bash
# 1. Health check — must return 200 {status:"ok"}
curl -sf $H/health | jq .

# 2. Unknown tenant — must return 404
curl -sf $H/tenants/00000000-0000-0000-0000-000000000000/bronze/summary \
  && echo "FAIL: expected 404" || echo "PASS: got non-200"

# 3. Malformed UUID in path — must return 400 or 422
curl -o /dev/null -w "%{http_code}" $H/tenants/not-a-uuid/bronze/summary
# expected: 400 or 422

# 4. Missing Content-Type on POST — must return 415 or 422
curl -sf -XPOST $H/tenants/$T_AU/sync/invoices -d '{}' \
  -w "\nHTTP %{http_code}\n"
```

### 7.4.2  Sync smoke (custom-connection mode)

```bash
# 5. Trigger incremental sync — invoices
curl -sf -XPOST $H/tenants/$T_AU/sync/invoices \
  -H 'Content-Type: application/json' \
  -d '{"triggered_by":"phase7-smoke"}' | jq '{run_id, status}'

# 6. Verify run landed in history
curl -sf "$H/tenants/$T_AU/runs?limit=5" | jq '.runs[0] | {entity_type, status, records_seen}'

# 7. Verify bronze count increased or stayed same (idempotent re-run)
BEFORE=$(psql $DATABASE_URL -tAc "SELECT count(*) FROM xero.local_bronze_record WHERE tenant_id='$T_AU' AND entity_type='invoices'")
curl -sf -XPOST $H/tenants/$T_AU/sync/invoices \
  -H 'Content-Type: application/json' \
  -d '{"triggered_by":"phase7-idempotency-check"}' > /dev/null
AFTER=$(psql $DATABASE_URL -tAc "SELECT count(*) FROM xero.local_bronze_record WHERE tenant_id='$T_AU' AND entity_type='invoices'")
[ "$AFTER" -ge "$BEFORE" ] && echo "PASS: idempotent ($BEFORE → $AFTER)" || echo "FAIL: count regressed"
```

### 7.4.3  Backfill smoke

```bash
# 8. Start a narrow 30-day backfill (invoices only, low quota burn)
PLAN=$(curl -sf -XPOST $H/tenants/$T_AU/backfill \
  -H 'Content-Type: application/json' \
  -d '{
    "entities":["invoices"],
    "start_date":"2026-04-01",
    "end_date":"2026-05-01",
    "chunk_size_days":30,
    "triggered_by":"phase7-smoke"
  }' | jq -r '.plan_id')
echo "Plan: $PLAN"

# 9. Poll until complete
for i in $(seq 1 30); do
  STATUS=$(curl -sf $H/tenants/$T_AU/backfill/$PLAN | jq -r '.plan.status')
  echo "[$i] $STATUS"
  [ "$STATUS" = "completed" ] && break
  sleep 10
done

# 10. Verify no terminal_failed chunks
curl -sf $H/tenants/$T_AU/backfill/$PLAN | \
  jq '.chunks | map(select(.status == "terminal_failed")) | length'
# expected: 0
```

### 7.4.4  BQ replay smoke

```bash
# 11. Replay any pending bronze rows to BQ
curl -sf -XPOST $H/tenants/$T_AU/bq/replay \
  -H 'Content-Type: application/json' \
  -d '{"limit":500}' | jq '{candidates, accepted, remaining}'

# 12. After replay, pending count should be 0 (or close if BQ had transient errors)
psql $DATABASE_URL -c "
  SELECT count(*) AS still_pending
  FROM xero.local_bronze_record
  WHERE bq_synced_at IS NULL AND tenant_id = '$T_AU';"
```

### 7.4.5  Edge cases in HTTP tests

| Test | Curl snippet | Expected |
|---|---|---|
| Sync with unknown entity | `POST /tenants/$T/sync/not_an_entity` | 400 / 404 |
| Backfill with `end_date < start_date` | `{"start_date":"2026-05-01","end_date":"2026-04-01",…}` | 422 |
| Backfill with `chunk_size_days=0` | `{"chunk_size_days":0,…}` | 422 |
| Cancel already-completed plan | `POST /tenants/$T/backfill/$COMPLETED_PLAN/cancel` | 409 or 200 no-op |
| `GET /tenants/$T/backfill/$NONEXISTENT_PLAN` | `GET` with random UUID | 404 |
| Schedule delete non-existent | `DELETE /tenants/$T/schedules/00000000-…` | 404 |
| BQ replay with `limit=0` | `{"limit":0}` | 200 `{candidates:0}` or 422 |

---

## 7.5  BigQuery Parity Verification

After a full sync or backfill run, row counts in Postgres and BigQuery must match.

### 7.5.1  Postgres side

```sql
-- Per-entity bronze counts for a tenant
SELECT entity_type, COUNT(*) AS pg_rows
FROM xero.local_bronze_record
WHERE tenant_id = '9dc5d3f0-68b1-4811-a38d-9efbb5990604'
GROUP BY entity_type
ORDER BY entity_type;

-- How many rows are waiting for BQ sync?
SELECT COUNT(*) AS pending_bq
FROM xero.local_bronze_record
WHERE tenant_id = '9dc5d3f0-68b1-4811-a38d-9efbb5990604'
  AND bq_synced_at IS NULL;
```

### 7.5.2  BigQuery side

```sql
-- Run in the BQ console or `bq query`
SELECT COUNT(*) AS bq_rows
FROM `prj-dw-dev.dw_1_bronze_xero.Invoices`
WHERE tenant_id = '9dc5d3f0-68b1-4811-a38d-9efbb5990604';
```

### 7.5.3  Parity check script

```bash
TENANT=9dc5d3f0-68b1-4811-a38d-9efbb5990604
PROJECT=prj-dw-dev
DATASET=dw_1_bronze_xero

for ENTITY in invoices payments bank_transactions credit_notes journals \
              manual_journals purchase_orders quotes bank_transfers; do
  BQ_TABLE=$(echo "$ENTITY" | sed 's/_\([a-z]\)/\U\1/g;s/^\([a-z]\)/\U\1/')  # snake→PascalCase
  PG_COUNT=$(psql $DATABASE_URL -tAc \
    "SELECT count(*) FROM xero.local_bronze_record WHERE tenant_id='$TENANT' AND entity_type='$ENTITY'")
  BQ_COUNT=$(bq query --nouse_legacy_sql --quiet --format=csv \
    "SELECT COUNT(*) FROM \`$PROJECT.$DATASET.$BQ_TABLE\` WHERE tenant_id='$TENANT'" \
    | tail -1)
  DIFF=$((PG_COUNT - BQ_COUNT))
  STATUS=$( [ "$DIFF" -le 0 ] && echo "PASS" || echo "WARN lag=$DIFF" )
  printf "%-30s  pg=%-6s  bq=%-6s  %s\n" "$ENTITY" "$PG_COUNT" "$BQ_COUNT" "$STATUS"
done
```

Expected: `WARN lag=N` only during active backfill (streaming latency ≤5 min). After
`POST /bq/replay`, all entities should show `PASS`.

### 7.5.4  Duplicate detection

```sql
-- Should return 0 rows. Any row here is a PK violation in the bronze layer.
SELECT tenant_id, entity_type, record_id, COUNT(*) AS n
FROM xero.local_bronze_record
GROUP BY tenant_id, entity_type, record_id
HAVING COUNT(*) > 1;
```

```sql
-- BigQuery equivalent — insertId deduplication window is ~1 min.
-- Check for logical duplicates outside that window:
SELECT record_id, COUNT(*) AS n
FROM `prj-dw-dev.dw_1_bronze_xero.Invoices`
WHERE tenant_id = '9dc5d3f0-68b1-4811-a38d-9efbb5990604'
GROUP BY record_id
HAVING COUNT(*) > 1
LIMIT 20;
```

---

## 7.6  Watermark & Gap Verification

After backfill + incremental cron, verify there are no temporal gaps in coverage.

```sql
-- Last watermark per (tenant, entity)
SELECT tenant_id, entity_type,
       last_modified_watermark,
       last_sync_at,
       records_seen
FROM xero.checkpoint
ORDER BY tenant_id, entity_type;
```

Verify:
- `last_modified_watermark` is not NULL for any entity that has run in incremental mode.
- `last_modified_watermark` is monotonically advancing (compare across runs via `run_history`).
- `last_sync_at` ≤ now and ≥ most recent `finished_at` in `run_history`.

```sql
-- Check that watermark never went backwards (should return 0 rows)
WITH ranked AS (
  SELECT run_id, tenant_id, entity_type, started_at,
         LAG(started_at) OVER (PARTITION BY tenant_id, entity_type ORDER BY started_at) AS prev_started
  FROM xero.run_history
  WHERE status = 'succeeded'
)
SELECT * FROM ranked WHERE prev_started > started_at;
-- Expected: 0 rows
```

---

## 7.7  Rate-Limit & Retry Verification

### 7.7.1  Single-pod throttle

Watch logs during a sync that approaches the per-minute limit:

```bash
docker compose logs -f xero-server | grep -E '(rate_limit|429|X-MinLimit|backoff|retry)'
```

Expected log sequence on a 429:
```
WARN xero_client: 429 received tenant=<T> entity=invoices attempt=1
INFO xero_client: rate-limit pause published tenant=<T> dur=60s
INFO xero_client: sleeping for retry-after tenant=<T> secs=60
INFO xero_client: retry attempt=2 ...
```

### 7.7.2  Multi-replica Redis broadcast

To verify that a 429 pause from replica A reaches replica B:

```bash
# Inspect the Redis key after a 429 is triggered
redis-cli -p 6382 KEYS 'xero_rl:*'
redis-cli -p 6382 GET  'xero_rl:9dc5d3f0-68b1-4811-a38d-9efbb5990604:pause_until'
redis-cli -p 6382 PTTL 'xero_rl:9dc5d3f0-68b1-4811-a38d-9efbb5990604:pause_until'
# Expected: key exists, TTL > 0 during pause window
```

### 7.7.3  Adaptive throttle (X-MinLimit-Remaining ≤ 5)

In logs, look for:
```
WARN xero_client: proactive throttle MinLimit-Remaining=3 tenant=<T> sleeping=Xs
```

This fires before a 429 and is the preferred path. If it never appears on large syncs,
check that the response-header reading in `rate_limit.rs` `update_from_headers` is wired
to the correct header name (`X-MinLimit-Remaining`).

---

## 7.8  Deployment Checklist

### 7.8.1  Pre-deploy

- [ ] `cargo test` — all unit tests pass
- [ ] `cargo clippy -- -D warnings` — zero warnings
- [ ] `cargo audit` — no RUSTSEC advisories (run `cargo install cargo-audit` if missing)
- [ ] `.env` and `app/gcp-credentials.json` are NOT committed (`git status` clean on those paths)
- [ ] `GOOGLE_APPLICATION_CREDENTIALS` secret is in Secret Manager, not baked into the image
- [ ] BQ dataset `dw_1_bronze_xero` exists in target project (`tooling/bq_provision.sh` was run)
- [ ] Postgres Cloud SQL instance is running and accessible from Cloud Run VPC connector
- [ ] Redis (Memorystore or equivalent) is reachable from Cloud Run

### 7.8.2  Build & push

```bash
cd "/Volumes/Lagring/Aquatiq/Aquatiq integrasjonen /apps/xero_service/xero_service_v2"

# Build (multi-stage Dockerfile strips debug symbols, produces ~50 MB binary)
docker build \
  --build-arg RUST_BUILD_PROFILE=release \
  -t gcr.io/prj-dw-dev/xero-service-v2:$(git rev-parse --short HEAD) \
  .

# Push
docker push gcr.io/prj-dw-dev/xero-service-v2:$(git rev-parse --short HEAD)
```

### 7.8.3  Cloud Run deploy

```bash
IMAGE=gcr.io/prj-dw-dev/xero-service-v2:$(git rev-parse --short HEAD)

gcloud run deploy xero-service-v2 \
  --image $IMAGE \
  --region europe-north1 \
  --service-account xero-runner@prj-dw-dev.iam.gserviceaccount.com \
  --set-env-vars "XERO_CONNECTION_TYPE=custom,\
GCP_PROJECT_ID=prj-dw-dev,\
BIGQUERY_DATASET=dw_1_bronze_xero,\
GOOGLE_APPLICATION_CREDENTIALS=/app/gcp-credentials.json,\
REDIS_URL=redis://<memorystore-ip>:6379/0,\
RUST_LOG=info" \
  --set-secrets "DATABASE_URL=xero-db-url:latest,\
XERO_ORG_1_CLIENT_ID=xero-org1-client-id:latest,\
XERO_ORG_1_CLIENT_SECRET=xero-org1-client-secret:latest,\
XERO_ORG_1_TENANT_ID=xero-org1-tenant-id:latest,\
XERO_ORG_2_CLIENT_ID=xero-org2-client-id:latest,\
XERO_ORG_2_CLIENT_SECRET=xero-org2-client-secret:latest,\
XERO_ORG_2_TENANT_ID=xero-org2-tenant-id:latest" \
  --add-cloudsql-instances prj-dw-dev:europe-north1:xero-pg \
  --min-instances 1 \
  --max-instances 3 \
  --concurrency 80 \
  --memory 512Mi \
  --cpu 1
```

`--min-instances 1` prevents cold-start latency for cron ticks. `--max-instances 3` stays
within Xero's 10-concurrent-connection limit with headroom.

### 7.8.4  Post-deploy verification

```bash
SERVICE_URL=$(gcloud run services describe xero-service-v2 \
  --region europe-north1 --format='value(status.url)')

# Health check
curl -sf $SERVICE_URL/health | jq .

# Migrations ran (check logs)
gcloud run services logs read xero-service-v2 --region europe-north1 --limit=50 \
  | grep -E '(migration|db-migrate|applied|up to date)'

# Trigger one sync to confirm auth end-to-end
curl -sf -XPOST "$SERVICE_URL/tenants/$T_AU/sync/contacts" \
  -H 'Content-Type: application/json' \
  -d '{"triggered_by":"post-deploy-smoke"}' | jq .
```

---

## 7.9  Rollback Procedure

```bash
# List recent revisions
gcloud run revisions list --service xero-service-v2 --region europe-north1

# Route all traffic back to prior revision
gcloud run services update-traffic xero-service-v2 \
  --region europe-north1 \
  --to-revisions PRIOR_REVISION_ID=100
```

Database migrations are append-only and backwards-compatible with the previous binary.
No down-migration is needed for a same-day rollback.

If a migration must be rolled back, run the inverse SQL manually then redeploy the old image.

---

## 7.10  Known Edge Cases & Their Mitigations

| Edge case | Symptom | Mitigation |
|---|---|---|
| Xero `payment_services` 401 on Custom Connection | Run marked `failed`, entity_type=`payment_services` | Platform limitation; no fix. Filter out in dashboards or change status to `skipped` in codebase |
| Clock skew between replicas > 1 s | Cron fires twice in same window | CAS on `last_triggered_at` prevents double-execution; log shows `rows_affected=0` on loser |
| BigQuery streaming buffer not queryable immediately | BQ count lags PG by ≤ 5 min | Use `POST /bq/replay` as idempotent catch-up; do not alarm until lag > 30 min |
| Very large entity (>500k rows, e.g. journals) | `XERO_MAX_PAGES_PER_ENTITY` cap hit | Raise env var; or split via date-window backfill with smaller `chunk_size_days` |
| GCP SA JSON rotated / expired | BQ sink fails, `bq_synced_at` stays NULL | Replace secret in Secret Manager, redeploy; rows accumulate and replay when sink recovers |
| Postgres connection pool exhausted on burst | `PoolTimedOut` error in logs | Increase `PG_POOL_SIZE` env var; Cloud Run concurrency × replicas must not exceed Cloud SQL max_connections |
| Backfill chunk re-claimed after pod restart | Chunk stuck in `claimed` with no worker | Mark timed-out chunks back to `pending` via: `UPDATE xero.backfill_chunk SET status='pending' WHERE status='claimed' AND updated_at < NOW() - INTERVAL '10 minutes'` |
| Redis unreachable at startup | Coordinator falls back to no-op (single-pod safe) | Log line: `WARN xero_cli: Redis coordinator unavailable, falling back to local-only`. Investigate but not fatal |
| Duplicate `XERO_CC_*` + `XERO_ORG_1_*` with same tenant_id | `xero_cc_connections.len() = 1` (deduped) | Expected behaviour; confirm via `GET /health` or config debug log |
| `advance_watermark=true` on a partial backfill window | Watermark jumps past unsynced records | Caller must assert coverage before setting this flag. Never set it in automated backfill; only in manual ops |

---

## 7.11  Phase 7 Acceptance Criteria

- [ ] All unit tests pass (`cargo test` green, zero failures).
- [ ] Integration tests confirm bronze idempotency, checkpoint CAS, backfill claim exclusivity.
- [ ] HTTP smoke: health, 404 on unknown tenant, sync trigger, backfill create/poll/complete.
- [ ] Duplicate check query returns 0 rows on both Postgres and BigQuery.
- [ ] BQ parity check shows `PASS` for all 9 core entities after `POST /bq/replay`.
- [ ] Watermark is non-null and monotonically advancing for all synced entities.
- [ ] Rate-limit pause key visible in Redis after a 429 (or adaptive throttle log appears).
- [ ] Cloud Run service boots, health check returns 200, post-deploy sync smoke passes.
- [ ] Rollback verified: prior revision still serves correctly.
- [ ] All edge cases in §7.10 are documented with a mitigation; critical ones have runbook SQL.
