# Layer 1 — ERP Connection API

## Role

Layer 1 is the authoritative ERP integration layer for Xero. It follows the same operational role as `xero_service` v1: authenticate to Xero, call the Accounting REST API, paginate results, respect rate limits, load records, and advance checkpoints only after successful processing.

## Local Implementation

| Crate / folder | Responsibility |
|---|---|
| `xero-auth` | OAuth2 PKCE helpers, token exchange/refresh, Redis token cache |
| `xero-client` | REST API calls to `https://api.xero.com/api.xro/2.0` |
| `xero-state` | Postgres tenants, OAuth fallback, checkpoints, run history |
| `xero-sync` | Sync orchestration and checkpoint update path |
| `xero-http` | Health/readiness and future trigger endpoints |
| `xero-cli` | Operator commands: migrate, check, serve, healthcheck |
| `tooling/` | Python BigQuery loader and future normalizers |

## External References

| Source | Layer 1 Use |
|---|---|
| Xero OpenAPI | Canonical endpoint/schema source for REST coverage and generated SDK parity |
| xero-python | v1-compatible behavior reference for OAuth, token refresh, Accounting API methods, pagination, and local testing patterns |
| xero-rs | Rust client design reference: `Client`, `XeroEndpoint`, OAuth key pairs, permissions/scopes, and entity modules |

## REST Contract

All Layer 1 REST calls must include:

- `Authorization: Bearer <access_token>`
- `Xero-Tenant-Id: <tenant_id>` for organisation-scoped calls
- `Accept: application/json`
- `If-Modified-Since` when running incremental sync

The `tenant_id` is runtime tenant context, not a fixed deployment environment variable. In `xero_service_v2`, it is derived from the OAuth-connected organisation and the tenant-scoped sync/auth flow rather than from a global `XERO_TENANT_ID` setting.

Layer 1 must handle:

- OAuth token refresh before expiry
- `304 Not Modified` as an empty incremental result
- `401` as authentication failure
- `403` as insufficient scope or tenant access
- `429` as rate-limit backoff/retry
- network/transient failures with retry policy

## Entity Coverage

Current Rust enum coverage:

- accounts
- bank_transactions
- bank_transfers
- contacts
- credit_notes
- invoices
- items
- journals
- manual_journals
- payments
- purchase_orders
- tax_rates
- tracking_categories

## Checkpoint Rule

Checkpoint update is allowed only after records are fetched, validated, and durably written. A failed or partially written run must leave the previous checkpoint intact.

## Warehouse Rule

Layer 1 owns bronze/warehouse writes. Layer 2 may request a read, draft, or approved write action, but it does not directly advance sync checkpoints or rewrite warehouse lineage.
