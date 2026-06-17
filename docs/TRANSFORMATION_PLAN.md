# Transformation plan — multi-agent execution

**Status:** awaiting confirmation · **Date:** 2026-06-17
**Design source of truth:** [NEW_ARCHITECTURE_RAW_GCS.md](NEW_ARCHITECTURE_RAW_GCS.md)
**Grounding:** 9-agent read-only code inventory (workflow `wf_f7220dca-a75`).

## Requirements (restated)

Convert `xero_service_v2` from a stateful pipeline
(fetch → Postgres dedup → BigQuery streaming → checkpoints/run-history/backfill)
into a **thin, stateless, append-only raw → GCS uploader**:

- No Postgres, no BigQuery sink. Storage = GCS objects (verbatim Xero response bytes, one per page).
- Custom-connection (`client_credentials`) auth only; drop PKCE + token persistence.
- Layered sync: incremental (modified-3d, 4h) · open-sweep (daily) · rolling-full 30d/90d (weekly/monthly) · master daily · reports daily · backfill once.
- Dedup/merge/modeling move downstream (BigQuery + Dataform) — **out of scope for this server.**
- Execute as a one-shot CLI job under an external scheduler.

## Guiding constraint — compile-order DAG

```
        ┌───────────── Wave A (additive, parallel, each leaves workspace GREEN) ─────────────┐
        │  xero-gcs (NEW)      xero-client (+raw fns)      xero-common (+config/helpers, NO removals) │
        └───────────────────────────────┬───────────────────────────────────────────────────┘
                                         │  freezes the interface contracts
        ┌───────────── Wave B (cutover, parallel-within-wave, integrated → GREEN) ───────────┐
        │  xero-sync REWRITE → SyncJob                                                         │
        │  xero-cli  → `sync` command (drop db-*)            ← all consume Wave-A contracts     │
        │  xero-auth → drop PKCE + token store                                                 │
        │  xero-http → DELETE (or thin)                                                         │
        │  xero-common → REMOVE dropped config fields                                           │
        │  xero-state → DELETE crate                                                            │
        │  workspace Cargo → members/deps (serialized, single owner)                            │
        └───────────────────────────────┬───────────────────────────────────────────────────┘
        ┌───────────── Wave C (verify) ──┴──────────────────────────────────────────────────┐
        │  integration build-fix · `sync --dry-run --local-dir` validation · review/security  │
        └─────────────────────────────────────────────────────────────────────────────────────┘
```

Why these boundaries (from the blast-radius inventory):
- `xero-state` is imported by `xero-sync`, `xero-http` (lib/backfill/cron_daemon), `xero-cli` → **all must stop importing it before deletion**.
- `xero-sync` rewrite breaks `xero-http` + `xero-cli` → they land in the **same wave**.
- `xero-gcs` must exist + compile before the `xero-sync` rewrite consumes `RawSink`.
- `xero-client` raw-page additions are **independent** (old `fetch_*` stay until `xero-sync` is rewritten).
- `xero-common` config *removals* ripple to every crate → do them in Wave B; only *additions* in Wave A.
- Root `Cargo.toml` is a single shared file → its edits are **serialized through one owner** (no parallel collisions).

## Frozen interface contracts (Wave A output — every Wave B agent codes against these)

```rust
// xero-gcs
#[async_trait] pub trait RawSink: Send + Sync {
    async fn put_raw(&self, key: &str, body: &[u8], meta: &ObjectMeta) -> Result<()>;
}
pub struct GcsRawSink { /* client + bucket */ }   pub struct LocalDirSink { root: PathBuf }
pub struct ObjectMeta(pub BTreeMap<String, String>);
pub enum SyncType { Incremental, OpenSweep, RollingFull, Master, Backfill, ReportSnapshot }
pub fn object_key(prefix,&str; tenant,&str; endpoint,&str; run_date,NaiveDate; ts,&str; run_id_short,&str; page,u32) -> String;
pub fn report_object_key(/* …same… */ period_key: &str) -> String;
pub fn metadata(/* typed args */) -> ObjectMeta;
pub fn sidecar_key(object_key: &str) -> String;  pub fn sidecar_bytes(&ObjectMeta) -> Vec<u8>;
pub struct RunManifest { /* run_id, started/finished, mode, entities: Vec<EntityOutcome> */ }
pub struct EntityOutcome { /* tenant, endpoint, pages, records, termination, error */ }
pub fn manifest_key(run_date: NaiveDate, run_id: Uuid) -> String;   // OUTSIDE GCS_PREFIX

// xero-client
pub struct RawPage { pub page: u32, pub body: bytes::Bytes, pub http_status: u16,
                     pub record_count: usize, pub fetched_at: DateTime<Utc> }
impl XeroApiClient {
    pub async fn fetch_raw_pages(&self, token:&str, entity:&EntityType,
        modified_after:Option<DateTime<Utc>>, extras:&ExtraQuery) -> Result<(Vec<RawPage>, PaginationOutcome)>;
    pub async fn fetch_report_raw(&self, token:&str, entity:&EntityType, extras:&ExtraQuery) -> Result<RawPage>;
}

// xero-common
impl EntityType { pub fn is_master(&self)->bool; pub fn master_data()->&'static[EntityType]; pub fn open_status()->&'static[EntityType]; }
pub struct GcsConfig  { pub bucket:String, pub prefix:String }
pub struct SyncConfig { pub window_days:i64, pub rolling_full_tight_days:i64, pub rolling_full_wide_days:i64, pub max_concurrent:usize }

// xero-sync
pub enum SyncMode { Incremental, OpenSweep{ where_clause:String }, RollingFull{ days:i64 },
                    Master, Backfill{ from:NaiveDate, to:NaiveDate }, Reports{ as_of:NaiveDate } }
pub struct SyncJob { /* auth: Arc<MultiTenantCustomConnectionClient>, sink: Arc<dyn RawSink>, cfg: SyncConfig */ }
impl SyncJob { pub async fn run(&self, run_id:Uuid, run_date:NaiveDate,
        tenants:&[String], entities:&[EntityType], mode:SyncMode, max_concurrent:usize) -> RunManifest; }
```

## Agent roster

| ID | Agent (owner crate) | Wave | Type | Writes to |
|----|---------------------|------|------|-----------|
| **A1** | gcs-builder (`xero-gcs`) | A | implementer (Rust) | new `core/crates/xero-gcs/**` |
| **A2** | client-raw (`xero-client`) | A | implementer (Rust) | `xero-client/src/lib.rs` (+ `bytes`) |
| **A3** | common-additions (`xero-common`) | A | implementer (Rust) | `xero-common/src/{types,config}.rs` (additive) |
| **B1** | sync-rewrite (`xero-sync`) | B | implementer (Rust) | `xero-sync/src/lib.rs` |
| **B2** | cli-sync (`xero-cli`) | B | implementer (Rust) | `xero-cli/src/main.rs` |
| **B3** | auth-trim (`xero-auth`) | B | implementer (Rust) | `xero-auth/src/**` (del `pkce.rs`) |
| **B4** | http-cut (`xero-http`) | B | implementer (Rust) | delete crate / thin it |
| **B5** | common-removals (`xero-common`) | B | implementer (Rust) | `xero-common/src/config.rs` |
| **B6** | state-delete (`xero-state`) | B | implementer (Rust) | delete `core/crates/xero-state/**` |
| **B7** | workspace-cargo (root) | B | implementer (Rust) | root `Cargo.toml` (serialized) |
| **R1** | rust-reviewer | per wave | `rust-reviewer` | — (read-only) |
| **R2** | build-fixer | B/C | `rust-build-resolver` | integration fixes |
| **R3** | security + code review | C | `security-reviewer`, `code-reviewer` | — (read-only) |

## Wave A — foundations (parallel, additive, non-breaking)

> Run A1/A2/A3 concurrently (disjoint files). Root `Cargo.toml` member/dep wiring for `xero-gcs` is applied by **B7** at the start of Wave B; in Wave A, A1 builds its crate standalone via `--manifest-path`.

**A1 — gcs-builder.** Create `core/crates/xero-gcs` per the Wave-A contracts above. Modules: `error.rs` (`GcsError` via thiserror), `key.rs` (`object_key`/`report_object_key` + `sanitize_segment` **ported verbatim from `xero-state/src/bq_sink.rs:199`** incl. its tests — no output may contain `/`), `meta.rs` (`ObjectMeta` BTreeMap, `SyncType`, `metadata()`, `sidecar_*`), `manifest.rs` (`RunManifest`/`EntityOutcome`/`manifest_key` — key sits OUTSIDE `GCS_PREFIX`, i.e. `_manifests/xero/{run_date}/{run_id}.json`), `sink.rs` (`RawSink` trait, `GcsRawSink` over the **yoshidan `google-cloud-storage`** crate built from `GOOGLE_APPLICATION_CREDENTIALS`, `LocalDirSink` via `tokio::fs`). Both sinks write body + `.meta.json` sidecar. `write_manifest(&dyn RawSink, &RunManifest)` is a free fn. Files <400 lines each. **Acceptance:** `cargo build/test -p xero-gcs` green; key/meta builders 100% unit-tested; `LocalDirSink` round-trips an object + sidecar to a `tempfile` dir.

**A2 — client-raw.** Add `RawPage` + `fetch_raw_pages` + `fetch_report_raw` to `xero-client/src/lib.rs`, reusing the existing pagination loop, `retry`, `rate_limit`, `ExtraQuery`/`extra_query_pairs`, and the `If-Modified-Since` header. Capture `resp.bytes()` verbatim per page; still parse minimally for the stop-decision (`should_stop_page_pagination`) and `record_count`. Leave all existing `fetch_*` intact. Add `bytes` dep. **Acceptance:** `cargo build/test -p xero-client` green; a unit test asserts raw bytes are preserved and pagination terminates with the right `TerminationReason`.

**A3 — common-additions.** Add (do **not** remove anything yet): `EntityType::is_master()`, `master_data()` (exactly the 9: accounts, contacts, items, tax_rates, tracking_categories, currencies, organisations, users, branding_themes — employees excluded), `open_status()` (invoices, bills); and the `GcsConfig`/`SyncConfig` structs + their env parsing (`GCS_BUCKET`, `GCS_PREFIX`, `SYNC_WINDOW_DAYS`, `SYNC_ROLLING_FULL_TIGHT_DAYS`, `SYNC_ROLLING_FULL_WIDE_DAYS`, `SYNC_MAX_CONCURRENT`). **Acceptance:** `cargo build/test -p xero-common` green; tier-helper + config-parse unit tests pass.

## Wave B — cutover (parallel within wave, integrated to GREEN)

> All B agents target the **same compiling end-state** and code against the frozen contracts. B7 owns the root `Cargo.toml` exclusively. Recommended: run B agents in **git worktrees** (isolation) then integrate, or land them as one coordinated branch with B7 applying Cargo edits last. Finish with R2 (build-fixer).

**B1 — sync-rewrite.** Replace `SyncExecutor`/`RunOptions`/`compute_next_watermark` with `SyncJob` + `SyncMode`. `run_entity` = mint token (custom-connection, no tenant header) → `fetch_raw_pages`/`fetch_report_raw` → `sink.put_raw(object_key, body, metadata)` per page → collect `EntityOutcome`. `run` fans out per (tenant,entity) with `max_concurrent.clamp(1,6)`, isolates failures, returns `RunManifest`. Compute `modified_after`/business-date window/where-filter/as-of per `SyncMode`. Delete every `xero_state` import (checkpoint, run_history, local_bronze, StateStore). **Acceptance:** `cargo build -p xero-sync` green against xero-gcs + xero-client; watermark/manifest unit tests.

**B2 — cli-sync.** Rework `xero-cli/src/main.rs`: drop `DbCheck`/`DbMigrate`; build a one-shot `sync` command with flags `--window-days --where/--no-window --business-from/--business-to --full --backfill RANGE --chunk-months --reports --as-of --entity --tenant --dry-run --local-dir --max-concurrent`. Construct `MultiTenantCustomConnectionClient` + `RawSink` (`GcsRawSink` prod / `LocalDirSink` for `--dry-run`) and invoke `SyncJob::run`. CLI expands `--backfill`/`--chunk-months` into business-date chunks (oldest→newest), calling `run` per chunk. Keep `healthcheck` (token-fetch + optional bucket HEAD). Remove pg/redis/sink-DB setup. **Acceptance:** `cargo build -p xero-cli` green; `sync --dry-run --local-dir ./out` writes the expected key layout.

**B3 — auth-trim.** Delete `xero-auth/src/pkce.rs` + any token persistence; keep `custom_connection.rs` + `token.rs` (in-memory). Remove the `pkce`/`PkceChallenge` public surface and `base64`/`sha2` deps. **Acceptance:** `cargo build -p xero-auth` green; custom-connection token test passes; no PKCE symbols exported.

**B4 — http-cut.** **Default: delete the `xero-http` crate** (routes all depend on deleted things: SyncExecutor, PKCE login/callback, backfill DB, bq/replay, readyz/Postgres). If the [decision](#decisions-to-confirm) is "keep thin", instead reduce to `/healthz` + `POST /sync` only. **Acceptance:** workspace builds without it; no dangling references.

**B5 — common-removals.** Remove dropped config: `pg_dsn`/`DATABASE_URL`, `redis_url`, BigQuery vars, PKCE (`xero_client_id/secret/redirect_uri`) + hybrid flags. Keep custom-connection `XERO_ORG_N_*`, `XERO_SCOPES`; keep parsing `XERO_CONNECTION_TYPE` but treat non-`custom` as a config error. Drop the now-dead `Error::StateStore` variant if unused. **Acceptance:** `cargo build -p xero-common` green; config tests updated.

**B6 — state-delete.** Delete `core/crates/xero-state/` entirely (after B1/B2/B4 stop importing it). Salvage nothing into it; `RunManifest` lives in xero-gcs, chunk enumeration moves to the CLI. **Acceptance:** directory gone, no `xero_state`/`xero-state` references remain (`grep` clean).

**B7 — workspace-cargo.** Root `Cargo.toml`: add member `core/crates/xero-gcs`, remove `core/crates/xero-state`; `[workspace.dependencies]` (`bytes`, `xero-gcs` path already added in Wave A; `gcloud-storage` lives in xero-gcs's own Cargo.toml), drop `sqlx`, `deadpool-redis`, `gcp-bigquery-client`, `base64`, `sha2`, and `cron` (if http deleted). **Acceptance:** `cargo build` (whole workspace) green; `cargo tree` shows no openssl-sys (rustls only) and none of the dropped deps.

## Wave C — verify & handoff

- **R2 build-fixer** (`rust-build-resolver`): make the whole workspace + `cargo clippy -- -D warnings` + `cargo test` green after integration.
- **Dry-run validation:** `sync --dry-run --local-dir ./out` for one tenant; eyeball object layout + `.meta.json` + manifest; then a real `xero-raw-dev` bucket run.
- **R3 review:** `code-reviewer` + `security-reviewer` (credential handling, no secrets logged, SA scope = `storage.objectCreator`).
- **Downstream (separate, not this server):** BigQuery external table over the bucket + Dataform dedup (global latest-by-`(id, UpdatedDateUTC)` + business-date delete diff) — the "Gael trick".

## Risks (high / medium)

| Sev | Risk | Mitigation |
|-----|------|------------|
| ~~HIGH~~ RESOLVED | Wrong GCS crate (yoshidan vs official) | Confirmed at Wave A: use `gcloud-storage` = "1.3" (yoshidan, renamed; defaults `rustls-tls`+`auth`). `google-cloud-storage` 1.x is Google's different official SDK. Pinned. |
| MED | TLS feature clash (workspace = rustls; GCS crate may pull native-tls/openssl) | `default-features=false`, features `["auth","rustls-tls"]`; `cargo tree` to confirm no openssl-sys |
| MED | Manifest written through prefix-aware key builder → mis-placed | `manifest_key` takes bucket-relative path (no `GCS_PREFIX`); unit-test it never starts with prefix |
| MED | `sanitize` drift lets a slash create phantom GCS folders | Port `sanitize_table_segment` verbatim + copy its tests; assert no `/` in output |
| MED | Deployment/monitoring depends on `serve`/`/readyz`/Postgres | Confirm hostinger unit + monitors before deleting xero-http (decision below) |
| MED | Wave B parallel agents collide on root `Cargo.toml` | B7 is the sole owner; other agents never touch root Cargo |

## Decisions to confirm

1. **xero-http: delete entirely (default) or keep a thin `/healthz` + `POST /sync`?**
   Delete = simplest (CLI + external cron). Keep = on-demand trigger + a liveness endpoint for hostinger. *Needs your ops view (does anything hit HTTP today?).*
2. **GCS crate: yoshidan `google-cloud-storage` (default — best custom-metadata ergonomics) or `object_store`?**
3. **Per-contact aged reports: derive aging downstream (default) or fan-out the report API?** (already leaning derive-downstream).
4. **deep-reconcile layer: drop (default) or keep yearly?**

Defaults will be used for anything you don't override.

## Complexity & sequencing

- **Wave A:** Medium — 3 agents parallel. ~the bulk of net-new code (xero-gcs).
- **Wave B:** High — 7 agents, interdependent, one integration point. The cutover.
- **Wave C:** Medium — build-fix + validation + review.
- Each wave ends at a compiling, tested workspace. Work on a branch off current HEAD (working tree already carries pre-existing uncommitted Phase 1–3 changes — branch keeps this separate).
