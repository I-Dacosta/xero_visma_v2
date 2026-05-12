# Phase 6 — REST Role Definition

## Status

Complete.

## Decision

Xero REST is the authoritative integration role for xero_service_v2.

Layer 1 owns the deterministic ERP connection path and remains responsible for production sync, write-back, checkpointing, retry behavior, and warehouse lineage. Layer 2 can expose Xero capabilities to agents through MCP and prompt-driven tools, but it cannot replace Layer 1 for scheduled sync or durable state transitions.

## Why REST Owns the Core Role

Xero's official OpenAPI repository is the canonical schema and endpoint source. The official Python SDK is generated from those specs and provides the v1-compatible behavior reference for OAuth, token refresh, Accounting API calls, and local test patterns. The Rust implementation can either call REST directly through `reqwest` or selectively adopt patterns from `xero-rs`, but the service boundary stays REST-first.

## Responsibilities

| Area | REST Layer Responsibility |
|---|---|
| Auth | OAuth2 PKCE, refresh, scoped access tokens, tenant mapping |
| Reads | Accounting REST endpoints with pagination and incremental watermarks |
| Writes | Explicit, audited, tenant-scoped write-back operations |
| State | Postgres tenants, checkpoints, run history, token fallback |
| Rate limits | Tenant-aware throttling and retry/backoff for Xero limits |
| Warehouse | Validated bronze writes and checkpoint advancement after durable success |
| Observability | Run IDs, tenant IDs, entity names, upstream endpoint, status, error class |

## Non-Responsibilities

REST Layer 1 does not own:

- natural-language prompt design
- agent planning loops
- MCP client configuration
- interactive assistant UX
- unaudited ad-hoc writes

Those belong to Layer 2, subject to the safety boundary in the layer system docs.

## Phase 6 Acceptance Criteria

- Layer 1 and Layer 2 boundaries are documented.
- REST remains the authoritative source for sync and write-back.
- Layer 2 MCP/agent tools are documented as controlled business tooling, not checkpoint owners.
- External source material is linked for OpenAPI, SDK, Rust client reference, MCP server, prompt library, and agent toolkit.
- README includes Phase 6 status and links to the layer system.
