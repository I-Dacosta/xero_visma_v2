# Deprecated docs

Everything in this folder describes an **earlier design** of the Xero ingestion service (`xero_service_v2`): a stateful pipeline with an HTTP API, Postgres-based dedup, BigQuery streaming, checkpoints, and an agent/MCP "layer system" built on top of it.

That design was replaced on **2026-06-17** by a thin, stateless raw→GCS uploader (proposed in `docs/NEW_ARCHITECTURE_RAW_GCS.md`) — the architecture the root [`README.md`](../../README.md) describes as current today.

**Nothing in this folder reflects the system as it exists now.** Kept for historical reference only — do not use it to understand or operate the current pipeline.

## Where the current docs are instead

| Doc | Covers |
|---|---|
| [`docs/NEW_ARCHITECTURE_RAW_GCS.md`](../NEW_ARCHITECTURE_RAW_GCS.md) | Current Rust ingestion service — design, GCS layout, metadata |
| [`docs/TRANSFORMATION_PLAN.md`](../TRANSFORMATION_PLAN.md) | Execution plan for the pivot to that design |
| [`docs/OPERATIONS.md`](../OPERATIONS.md) | Running the current ingestion service (env vars, cadence, deploy) |
| [`docs/DWH_ARCHITECTURE.md`](../DWH_ARCHITECTURE.md) | Current warehouse pipeline: GCS → staging → ODS → mart. Actively maintained — start here for anything BigQuery/Dataform related |
| [`docs/STAGING_XERO.md`](../STAGING_XERO.md) | Current Xero API payload/field reference, used to write every parser in `etl/xero/` |

## What each file here covered

| File | What it covered |
|---|---|
| `ARCHITECTURE.md` | Old crate map — `xero-http` server, `xero-sync`, checkpoint-based sync |
| `API.md` | HTTP endpoint reference for the old `xero-http` server |
| `BACKFILL.md` | Backfill runbook for the old stateful (Postgres + BigQuery streaming) pipeline |
| `BUG_E_LESSONS_APPLIED.md` | A 2026-05-20 bug-fix write-up; cross-references a sibling app (`visma_service`) not present in this repo |
| `layerSystem.md` | Index into the `layeSystem/` docs below (the folder name is a typo for "layerSystem" — left as-is; this is a historical file, not something to fix) |
| `layeSystem/README.md` | Index for the two-layer design |
| `layeSystem/LayerSystem.md` | Full overview of the two-layer design: deterministic ERP sync vs. agent-facing tools |
| `layeSystem/Layer1/layer1_erp_connection_api.md` | Layer 1 — the ERP connection/sync layer |
| `layeSystem/Layer2/layer2_agentic_mcp_tools.md` | Layer 2 — MCP and agentic tools built on top of Layer 1 |
| `phase6_rest_role_definition.md` | Old build-out, phase 6 — REST role definition |
| `phase7_testing_and_deployment.md` | Old build-out, phase 7 — testing & deployment |
