# Bug E lessons applied to xero_service_v2

**Date:** 2026-05-20
**Status:** Loader rewrite shipped pre-launch. Bronze dataset `dw_1_bronze_xero` not yet receiving writes.
**Cross-reference:** `/apps/visma_service/docs/BUG_E_GL_DUPE_LEAK_SOURCE.md` (closed 2026-05-19, commit `f458f45`).

## What Bug E was, briefly

Visma v1's hourly BigQuery sync produced ~12,000 duplicate rows per hour in `dw_1_bronze_visma.visma_general_ledger_transactions`. Root cause: the smart-MERGE join condition included `target.businessUnitId = source.businessUnitId`, but `businessUnitId` was on `BLOCKED_SOURCE_FIELDS` so `source.businessUnitId` was always NULL → `NULL = X` evaluates NULL (false) in BigQuery → JOIN never matched → every source row fell through to `WHEN NOT MATCHED THEN INSERT`.

A simpler way to say it: **the production MERGE used SQL `=` for key-equality, which is NULL-unsafe. Any NULL on either side silently breaks the join.**

## What was wrong with xero_service_v2's loader (pre-fix)

`tooling/src/xero_tooling/bigquery/loader.py` had two problems that would have reproduced Bug E in xero bronze immediately on first sync:

1. **No MERGE at all.** Docstring claimed "Merge a list of records into BigQuery via MERGE statement", but the implementation called `client.insert_rows_json(full_table, records)` — pure streaming append. Every hourly sync would have written another full copy.
2. **No primary key handling.** Records were not deduplicated. No `--primary-key` argument in the CLI. The loader couldn't have built a correct MERGE even if asked to.

Xero v2 is pre-launch: `dw_1_bronze_xero` doesn't have tables yet, so no production damage has been done. Fixing pre-launch keeps the radar clean.

## What changed

`tooling/src/xero_tooling/bigquery/loader.py` rewritten to mirror the pattern that `visma_service_v2/tooling/.../bigquery/loader.py` already uses. Concretely:

| Aspect | Before (broken) | After (matches visma v2) |
|---|---|---|
| Write mechanism | `client.insert_rows_json` (streaming) | `client.load_table_from_json` (batch) into a per-run temp table |
| Temp table write disposition | n/a | `WRITE_TRUNCATE` |
| Primary key | not supported | `--primary-key` arg, comma-separated; dotted paths for nested fields |
| Source dedup | none | `_dedupe_records_by_pk` in Python; raises on same-PK-different-payload |
| MERGE JOIN | n/a | `T.field IS NOT DISTINCT FROM S.field` per PK part — **NULL-safe** |
| WHEN MATCHED | n/a | unconditional `UPDATE SET T.col = S.col` for every non-PK column |
| WHEN NOT MATCHED | n/a | `INSERT (...) VALUES (...)` over all temp-table columns |
| Temp table cleanup | n/a | `finally: client.delete_table(temp_id, not_found_ok=True)` |
| Observability | only "Loaded N rows" log | per-MERGE `affected / inserted / updated` log via `merge_job.dml_stats` |

## Why each choice matters

- **Batch load + WRITE_TRUNCATE** — eliminates BigQuery streaming-buffer effects. Streaming-inserted rows are not reliably visible to subsequent DML for a few minutes. Batch loads commit immediately to managed storage.
- **`IS NOT DISTINCT FROM`** — `NULL IS NOT DISTINCT FROM NULL` is `TRUE`. Compare to `NULL = NULL` which is `NULL` (falsy). Choosing the NULL-safe operator means a single optional / sometimes-missing key column cannot silently break the JOIN for every row. This is the one-character (well, 24-character) defence that would have prevented v1 Bug E.
- **Python pre-dedup** — BigQuery MERGE raises if multiple source rows match one target row. Pre-deduping in Python surfaces upstream "same PK, different payload" bugs at a call site you can debug, instead of as an opaque MERGE failure.
- **Finally-block staging cleanup** — failed runs do not leave orphan `*_tmp_<hex>` tables in the dataset.
- **`dml_stats` logging** — gives you the smoking-gun signal at runtime if MERGE ever stops matching. Visma v1 Bug E was localised in a few hours once `inserted = source_count, updated = 0` showed up in logs.

## What this loader does NOT do (intentional)

- **No `lastModifiedDateTime > target.lastModifiedDateTime` predicate on WHEN MATCHED.** v1's predicate was the reason the bug took weeks to surface — when source.lmd == target.lmd, the predicate is false and the matched row no-ops. That's correct behaviour in isolation but contributed to v1's debugging difficulty. Xero records get a full upsert on every match; idempotent and easy to reason about.
- **No business-unit auto-injection.** v1's `tier1_identity.normalize_logical_primary_keys` silently appended `tenantId` + `businessUnitId` to every endpoint's primary_key list and only stripped one of them later, which was exactly how v1 Bug E was created. The xero loader treats `--primary-key` as the literal, declared key set.
- **No raw_stage / shadow / normalizer features.** Visma v2 has those for its pilot-cutover process. Xero v2 doesn't need them.

## Action items before xero v2 launch

1. Decide the `--primary-key` for each Xero entity (e.g. `invoiceID` for invoices, `contactID` for contacts, `accountID` for accounts). The Xero Accounting API uses GUIDs which are convenient single-field keys.
2. Wire the Rust sync executor (`xero-sync` crate) to subprocess this loader with the right `--primary-key` per endpoint. Mirror visma v2's `bq_writer.rs` pattern.
3. Provision `dw_1_bronze_xero` and confirm one round-trip end-to-end before turning on the cron.
4. Re-read this doc before adding any "smart" primary-key logic — the temptation to auto-inject tenant_id is exactly how v1 went wrong.
