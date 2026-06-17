# New architecture — thin raw → GCS uploader

**Status:** proposal · **Date:** 2026-06-17

## Goal & principles

Turn the server from a stateful ETL pipeline (fetch → Postgres dedup → BigQuery
stream → checkpoints → backfill) into a **thin, stateless, append-only raw
landing job**: pull from Xero, write the verbatim API responses to GCS, stop.
All dedup / merge / modeling moves downstream (Cloud Function → BigQuery →
Dataform), per the new pipeline diagram.

Principles:

1. **No Postgres.** No checkpoints, no run-history table, no bronze table.
2. **As raw as possible.** Persist the exact bytes Xero returned, one object per
   API page. No re-serialization, no per-record unpacking, no envelope stripping.
3. **Stateless & idempotent-by-append.** Each run computes its own window
   (`now − N days`); re-runs just write new objects. Downstream BigQuery dedups.
4. **Custom-connection only.** `client_credentials` issues no refresh token →
   nothing to persist. Drop PKCE (it needs a token store).

---

## Before → after

| Concern | Today | New |
|---|---|---|
| Fetch | `xero-client` paginated GET + retry + rate-limit | **kept** (+ raw-bytes path) |
| Auth | PKCE + custom-connection + token store | **custom-connection only**, in-memory token |
| Dedup / merge | `local_bronze::upsert_records` (Postgres) | **removed** → downstream BigQuery |
| Warehouse load | `bq_sink` BigQuery streaming insert | **removed** → GCS object write |
| Incremental state | `checkpoint` watermark (Postgres) | **removed** → rolling `now − N days` |
| Audit | `run_history` (Postgres) | **run manifest** object in GCS + logs |
| Backfill | DB-driven plan/chunks | **CLI flag** with explicit date range |
| Storage | Postgres + BigQuery | **GCS bucket** (`gcloud-storage`) |
| Entry point | axum server + cron daemon | **CLI one-shot job** (cron/scheduler); thin HTTP optional |

---

## New crate map

```
core/crates/
  xero-common     KEEP (trim)   config (drop pg/redis/bq/pkce), types (EntityType unchanged), error
  xero-auth       KEEP (trim)   custom_connection.rs + token.rs; drop pkce.rs + token persistence
  xero-client     KEEP (+add)   fetch + rate_limit + retry; ADD raw-page capture
  xero-gcs        NEW           RawSink trait, GcsRawSink, LocalDirSink, path + metadata builders
  xero-sync       REWRITE       SyncJob: fetch → upload raw; no StateStore
  xero-cli        KEEP (trim)   `sync` (primary), `healthcheck`; drop db-check/db-migrate
  xero-http       OPTIONAL      /healthz + POST /sync trigger only; or delete entirely
  xero-state      DELETE        Postgres + BigQuery (local_bronze, bq_sink, checkpoint,
                                run_history, backfill, sync_schedule, tenant)
```

---

## Data flow

```
                     ┌──────────── one-shot job (cron / Cloud Scheduler / hostinger timer) ─────────────┐
                     │                                                                                   │
  XERO_ORG_N_*  ──▶  xero-auth (client_credentials, in-mem token)                                        │
                     │                                                                                   │
  for each (tenant, entity):                                                                             │
        xero-client.fetch_raw_pages(token, entity, modified_after = now−Nd)   ── rate-limited, retried   │
                     │   yields Vec<RawPage{ page, body: Bytes, status }>                                 │
                     ▼                                                                                    │
        xero-gcs.RawSink.put_raw(key, body, metadata)  ── one object per page, verbatim bytes            │
                     │                                                                                    │
                     ▼                                                                                    │
   gs://xero-raw-{env}/raw/xero/{tenant}/2.0/{endpoint}/{date}/{ts}_{run_id}_p{page}.json                │
                     │   + sidecar .meta.json   + _manifests/.../{run_id}.json                           │
                     └────────────────────────────────────────────────────────────────────────────────┘
                                                   │
                       (downstream, separate)      ▼
                       Cloud Function → BigQuery external/staging table → Dataform (dedup/ODS/mart) → BI
```

---

## GCS layout

Bucket **per environment** (simplifies IAM + lifecycle): `xero-raw-prod`, `xero-raw-dev`.

```
gs://{GCS_BUCKET}/{GCS_PREFIX}/{tenant_id}/2.0/{endpoint}/{run_date}/{ts}_{run_id}_p{page}.json
```

- `GCS_PREFIX` default `raw/xero`
- `tenant_id` — Xero tenant UUID (from `XERO_ORG_N_TENANT_ID`)
- `2.0` — Xero Accounting API version (`api.xro/2.0`, hard-coded today)
- `endpoint` — `entity.as_str()` (snake_case, slash-free; `report_profit_and_loss`,
  **not** `Reports/ProfitAndLoss` — the slash would create a phantom folder; reuse
  the existing `sanitize_table_segment` logic)
- `run_date` — UTC `YYYY-MM-DD` of the run (for reports: the as-of date)
- filename — `{ISO8601-compact}_{run_id-short}_p{page:03}.json`

Example:
```
gs://xero-raw-prod/raw/xero/e0c3a1b2-.../2.0/invoices/2026-06-17/20260617T030001Z_5f3c9a_p001.json
```

**Object body = the verbatim Xero HTTP response bytes for that page** — keeps the
`{"Id":..,"Status":"OK","Invoices":[...]}` envelope intact. Maximally raw.

**Sidecar** `....json.meta.json` duplicates the custom metadata as a queryable
artifact (survives copies that strip GCS metadata).

**Custom object metadata** (all string values, lowercase keys):
```
x-vendor            xero
x-tenant-id         e0c3a1b2-...
x-org-name          Aquatiq Australia Pty Ltd
x-endpoint          invoices
x-api-version       2.0
x-sync-type         incremental | open-sweep | rolling-full | master | backfill | report-snapshot
x-modified-after    2026-06-14T03:00:00Z      # modified-window lower bound (incremental, now−3d)
x-business-from     2026-03-19                # business-date window (rolling-full/backfill)
x-business-to       2026-06-17
x-where             Status=="AUTHORISED"      # status/where filter, if any (open-sweep)
x-page              1
x-record-count      137                       # parsed for the stop-decision anyway
x-http-status       200
x-run-id            5f3c9a2e-...              # correlation key across the run
x-synced-at         2026-06-17T03:00:01Z
```

**Run manifest** (replaces `run_history`), one per run:
```
gs://{GCS_BUCKET}/_manifests/xero/{run_date}/{run_id}.json
{ run_id, started_at, finished_at, window_days,
  entities: [ { tenant, endpoint, pages, records, termination, error? } ] }
```

---

## Client change — raw page capture (`xero-client`)

Today `fetch_records_with_query` parses each page into `Value` and **merges** all
pages into one `Vec<Value>` ([lib.rs:182](../core/crates/xero-client/src/lib.rs)).
Add a parallel method that keeps the bytes:

```rust
/// One raw API page: verbatim response body + just enough parsed metadata
/// to drive the pagination stop-decision and object metadata.
pub struct RawPage {
    pub page: u32,
    pub body: bytes::Bytes,   // exact bytes Xero returned
    pub http_status: u16,
    pub record_count: usize,  // len of the entity array on this page
    pub fetched_at: chrono::DateTime<chrono::Utc>,
}

impl XeroApiClient {
    /// Same pagination loop as `fetch_with_extras_tracked`, but yields each
    /// page's raw bytes instead of merging parsed records. Stop-decision still
    /// parses (empty-page / offset-not-advancing), but persists `resp.bytes()`.
    pub async fn fetch_raw_pages(
        &self,
        access_token: &str,
        entity: &EntityType,
        modified_after: Option<DateTime<Utc>>,
        extras: &ExtraQuery,
    ) -> Result<(Vec<RawPage>, PaginationOutcome)> { /* ... */ }
}
```

`rate_limit` + `retry` are reused unchanged. Reports use the single-shot path
(`fetch_report`) wrapped into one `RawPage`.

> Rate limiter: the in-memory per-tenant limiter (`for_tenant`) stays. The Redis
> coordinator was for multi-instance coordination — a single scheduled job doesn't
> need it, so `deadpool-redis` can be dropped.

---

## Sync orchestration (`xero-sync` rewrite)

```rust
pub struct SyncJob {
    auth:   Arc<MultiTenantCustomConnectionClient>,
    sink:   Arc<dyn RawSink>,
    window: chrono::Duration,   // e.g. 3 days
}

impl SyncJob {
    pub async fn run(
        &self,
        run_id: Uuid,
        run_date: NaiveDate,
        tenants: &[String],
        entities: &[EntityType],
        max_concurrent: usize,        // clamp to <= 6 per Xero limits
    ) -> RunManifest { /* per-(tenant,entity) isolation; collect outcomes */ }

    async fn run_entity(
        &self, run_id: Uuid, run_date: NaiveDate,
        tenant_id: &str, entity: &EntityType,
    ) -> Result<EntityOutcome> {
        let token = self.auth.fetch_token_for_tenant(tenant_id).await?;
        // Custom Connection: do NOT send xero-tenant-id header.
        let api = XeroApiClient::new_with_tenant_header(tenant_id.to_owned(), false);

        let modified_after = (!entity.is_report())
            .then(|| Utc::now() - self.window);

        let (pages, outcome) =
            api.fetch_raw_pages(&token.access_token, entity, modified_after, &ExtraQuery::default()).await?;

        for p in &pages {
            let key  = gcs::object_key(tenant_id, entity, run_date, run_id, p.page);
            let meta = gcs::metadata(tenant_id, entity, modified_after, run_id, p);
            self.sink.put_raw(&key, &p.body, &meta).await?;
        }
        Ok(EntityOutcome::from(entity, &pages, outcome))
    }
}
```

No `StateStore`, no checkpoint, no watermark math, no `run_history` DB calls.
`compute_next_watermark` and all of `xero-state` are deleted.

---

## GCS sink (`xero-gcs` — new crate)

```rust
#[async_trait]
pub trait RawSink: Send + Sync {
    async fn put_raw(&self, key: &str, body: &[u8], meta: &ObjectMeta) -> Result<()>;
}

/// Production: writes to a GCS bucket with full custom metadata.
pub struct GcsRawSink { client: gcloud_storage::client::Client, bucket: String }

/// Dry-run / local "machine calls API" phase: mirrors the same key layout on disk.
pub struct LocalDirSink { root: PathBuf }
```

**Dependency:** `gcloud-storage` = "1.3" (yoshidan's crate; it was named
`google-cloud-storage` until Google claimed that name for their different official
SDK — pin `gcloud-storage`). Full control over `Object.metadata`;
auth via `GOOGLE_APPLICATION_CREDENTIALS`, same service account as today, needs
`roles/storage.objectCreator` on the bucket). Alternative: `object_store` (simpler,
but weaker user-metadata ergonomics). Both also write the `.meta.json` sidecar.

---

## Entry points

**Primary — CLI one-shot job**, driven by external scheduler (cron / systemd timer
/ Cloud Scheduler). Matches the diagram's "local machine calls API … job pushed to
hostinger server".

```
# incremental (modified ≥ now−3d) — every 4h  (no --entity = default entity set)
xero sync --window-days 3
xero sync --window-days 3 --entity invoices,payments

# open-items sweep (status filter, no modified window) — daily
xero sync --entity invoices,bills --where 'Status=="AUTHORISED"' --no-window

# rolling full tight (last 30d business-date) — weekly
xero sync --entity invoices,journals --business-from 2026-05-18 --business-to 2026-06-17
# rolling full wide (last 90d business-date) — monthly
xero sync --entity invoices,journals --business-from 2026-03-19 --business-to 2026-06-17

# master data refresh (small, no filter) — daily
xero sync --entity accounts,contacts,items,tax_rates,tracking_categories --full

# one-time backfill (business-date chunks, oldest→newest)
xero sync --tenant <uuid> --backfill 2020-01-01:2026-06-17 --chunk-months 1

# reports (snapshot params, not a window) — daily
xero sync --reports --as-of 2026-06-17

# dry-run to local disk (LocalDirSink, no GCS)
xero sync --window-days 3 --dry-run --local-dir ./out
xero healthcheck
```

**Optional — thin HTTP** (`xero-http` trimmed to `/healthz` + `POST /sync`) only if
you want on-demand triggering. Otherwise delete the crate; cron + CLI is simpler.

---

## Config / env

Remove: `DATABASE_URL`, `REDIS_URL`, `BIGQUERY_DATASET`, `GCP_PROJECT_ID` (BQ),
all PKCE (`XERO_CLIENT_ID/SECRET/REDIRECT_URI`) and hybrid flags.

Keep: `XERO_ORG_N_{CLIENT_ID,CLIENT_SECRET,TENANT_ID,NAME}`, `XERO_SCOPES`,
`XERO_CONNECTION_TYPE=custom`, `XERO_MAX_PAGES_PER_ENTITY`, `RUST_LOG`.

Add:
```
GCS_BUCKET=xero-raw-prod
GCS_PREFIX=raw/xero                       # optional, default raw/xero
GOOGLE_APPLICATION_CREDENTIALS=/secrets/gcs-sa.json
SYNC_WINDOW_DAYS=3                        # incremental modified-window
SYNC_ROLLING_FULL_TIGHT_DAYS=30          # rolling-full (tight), weekly
SYNC_ROLLING_FULL_WIDE_DAYS=90           # rolling-full (wide), monthly
SYNC_ENTITIES=                            # optional allowlist; empty = all
SYNC_MAX_CONCURRENT=6                     # Xero per-tenant ceiling
```

---

## Sync layers & cadence

A single rolling pull does **not** keep state complete on its own:

- An item modified before the window and unchanged since is *correct* in GCS (its
  last snapshot is its current state — Xero bumps `UpdatedDateUTC` on every change,
  so any real change re-enters the window, including the eventual close/void), **but**
- **hard deletes** never re-appear in a modified window, and rare linked changes may
  not propagate to a parent's `UpdatedDateUTC` → drift.

So run **layered** jobs, all landing the same append-only GCS layout (tagged via
`x-sync-type`). Filter *type* matters: `modified` (UpdatedDateUTC) catches changes;
`business-date` (Date) enumerates a range so downstream can diff presence and detect
deletes. Two nested rolling-full windows give graduated freshness cheaply — a tight
window often, a wide window occasionally.

| Layer | Filter | Scope | Cadence | Catches |
|---|---|---|---|---|
| `incremental` | `modified ≥ now−3d` | transactional (+ master) | every 4h | churn, closes, voids |
| `open-sweep` | `Status=="AUTHORISED"` (no modified window) | invoices, bills | daily | open items unchanged > 3d |
| `rolling-full` (tight) | business-date last 30d | transactional | weekly | recent hard-deletes, drift |
| `rolling-full` (wide) | business-date last 90d | transactional | monthly | drift in days 31–90 |
| `master` | none (small tables) | master data | daily | everything (cheap) |
| `reports` | as-of params (no window) | report entities | daily | point-in-time snapshots |
| `backfill` | business-date chunks | all | once | full history |
| `deep-reconcile` | none, all history | transactional | yearly *(optional — or drop)* | delete/drift on >90d-old records |

`incremental` excludes reports (separate `reports` job — no `modified` filter).
`open-sweep` is just AR/AP (`AUTHORISED` only means "open" for invoices/bills).

Why no all-history `--full` on a schedule: old closed transactions are immutable, so
re-pulling years weekly is wasted API budget (100+ pages/entity vs ~5–15 for a 90-day
window) and risks rate limits (60/min, 5000/day per tenant). All-history runs **once**
at backfill.

**Window vs cadence — don't conflate:** `90d` is a *window size* (which dates a run
pulls); `yearly`/`monthly` is a *cadence* (how often it runs). `rolling-full (wide)`
= last-90-days, monthly. `deep-reconcile` = all-history, yearly — its only marginal
value is catching deletes on records **older than 90 days**, which barely happens
(Xero doesn't hard-delete posted transactions, and voids/edits bump `UpdatedDateUTC`
so `incremental` already catches them). Default: drop it; add back only if downstream
detects drift.

### Entity tiers

- **Master data** — `accounts`, `contacts`, `items`, `tax_rates`,
  `tracking_categories`, `currencies`, `organisations`, `users`, `branding_themes`.
  Small / low-cardinality → `--full` (no filter) is cheap; refresh daily.
- **Transactional** — `invoices`, `bills`, `bank_transactions`, `journals`,
  `payments`, `credit_notes`, `purchase_orders`, … Large over history → never
  unfiltered on a schedule; use incremental + open-sweep + rolling-full.

> **Aging note:** prefer computing AR/AP aging downstream from the raw
> invoices/credit-notes/payments you already land, rather than the per-contact aged
> reports (which require fan-out). See Reports below.

---

## Reports

The 8 `Report*` entities are point-in-time snapshots with date params — **not** a
`modified_after` stream. Run them as a **separate job** (`xero sync --reports`),
daily, with their own param/path model. Reuse the existing
`resolve_report_params` / `validate_report_params` ([client lib.rs:1095](../core/crates/xero-client/src/lib.rs)).

**Two dates per report — capture both:**

| Concept | Meaning | Where |
|---|---|---|
| snapshot time | when pulled | `run_date` partition + `x-synced-at` + filename ts |
| business period | what it covers (`date` or `fromDate/toDate`) | filename `period_key` + metadata |

```
raw/xero/{tenant}/2.0/{report_endpoint}/{run_date}/{ts}_{run_id}__{period_key}.json
# as-of:   .../report_balance_sheet/2026-06-17/...__asof-2026-06-17.json
# period:  .../report_profit_and_loss/2026-06-17/...__2026-06-01_2026-06-17.json
```

Metadata adds: `x-sync-type=report-snapshot`, `x-report`, `x-report-date`
(as-of) **or** `x-report-from`/`x-report-to` (period), `x-report-params` (full
sorted param signature = reproducible key). Store Xero's **verbatim report JSON**
as the body (drop the old `wrap_report_record` envelope — params/run-date live in
metadata).

**Param profiles** (fix the current `fromDate=today,toDate=today` default, which is a
useless single-day P&L):

| Family | Reports | Default params (run-date relative) |
|---|---|---|
| as-of | BalanceSheet, TrialBalance, ExecutiveSummary | `date = today` |
| period | ProfitAndLoss, BankSummary | `fromDate = month start`, `toDate = today` (MTD); + previous full month |
| budget | BudgetSummary | `date = today`, `periods=3`, `timeframe=1` |
| per-contact | AgedReceivables/PayablesByContact | requires `contactId` → **see decision** |

**Decision — per-contact aged reports:** they require a `contactId`, so org coverage
means fan-out over contacts.
- **Default (recommended):** don't call these endpoints — derive aging in Dataform
  from raw `invoices`/`credit_notes`/`payments` (`DueDate` + outstanding `AmountDue`
  vs snapshot date). No fan-out, no extra calls.
- **Only if you must match Xero's exact aged figures:** fan out, but only over
  contacts with non-zero `Balances.*.Outstanding` (already in the `contacts` you
  sync), daily; path gets a `__c-{contactId}` segment.

---

## Cross-cutting

- **Reports:** see the Reports section above.
- **Idempotency:** append-only; re-running any layer writes new objects (new
  ts/run_id). Downstream dedup must pick latest by `(id, UpdatedDateUTC)` **across
  all folders / all time**, not within a single window — otherwise a long-open item
  whose latest snapshot is old gets dropped. Do **not** make keys
  deterministic-overwrite — that fights "raw".
- **Failure isolation:** one entity failing doesn't abort the run (like today's
  `run_many`); the manifest records per-entity errors. Re-run re-fetches the whole
  window — cheap and safe.
- **Lifecycle:** date-partitioned paths make GCS lifecycle rules trivial (e.g.
  move to Coldline after 90d). Keep raw forever or per retention policy.

---

## Concrete deletions

- Delete crate `core/crates/xero-state/` entirely.
- Delete `core/crates/xero-auth/src/pkce.rs` and PKCE token persistence.
- Drop deps: `sqlx`, `deadpool-redis`, `gcp-bigquery-client` (workspace + crates).
- Remove DB migrations, `db-check` / `db-migrate` CLI commands.
- Remove HTTP routes: checkpoints, runs, backfill (DB), `bq/replay`.

## Rollout

1. Add `xero-gcs` with `LocalDirSink` + `GcsRawSink`; unit-test path/metadata builders.
2. Add `fetch_raw_pages` to `xero-client` (reuse pagination/retry/rate-limit).
3. Rewrite `xero-sync` → `SyncJob`; add `sync` CLI with the layer flags
   (`--window-days`, `--where/--no-window`, `--business-from/--business-to`,
   `--full`, `--backfill`, `--reports`). Start with `--dry-run`.
4. Fix `default_report_params`: period reports → MTD + previous-month, not
   `today→today`. Land reports via the two-date path/metadata model.
5. Validate object layout/metadata against a real tenant in `xero-raw-dev`.
6. Wire the scheduler: incremental 4h · open-sweep + master daily · rolling-full
   weekly · reports daily · backfill once.
7. Delete `xero-state`, PKCE, DB/BQ deps and routes.
8. Build the downstream BigQuery external table + Dataform dedup — global
   latest-by-`(id, UpdatedDateUTC)` + business-date delete diff (the "Gael trick").
