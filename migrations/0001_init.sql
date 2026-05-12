-- xero_service_v2 — schema bootstrap
-- Schema: xero (separate from any v1 tables)

CREATE SCHEMA IF NOT EXISTS xero;

-- ── Tenant registry ───────────────────────────────────────────────────────────
-- One row per Xero organisation we sync.
CREATE TABLE IF NOT EXISTS xero.tenants (
    tenant_id      TEXT PRIMARY KEY,          -- Xero organisation UUID
    tenant_name    TEXT,
    short_code     TEXT,
    tenant_type    TEXT NOT NULL DEFAULT 'ORGANISATION',
    is_active      BOOLEAN NOT NULL DEFAULT TRUE,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed the Aquatiq organisation; fill tenant_id after first OAuth exchange.
INSERT INTO xero.tenants (tenant_id, tenant_name)
VALUES ('PENDING_OAUTH_EXCHANGE', 'Aquatiq AS')
ON CONFLICT (tenant_id) DO NOTHING;

-- ── OAuth token store ─────────────────────────────────────────────────────────
-- Postgres fallback for when Redis is cold/empty.
CREATE TABLE IF NOT EXISTS xero.oauth_tokens (
    tenant_id      TEXT PRIMARY KEY REFERENCES xero.tenants (tenant_id),
    access_token   TEXT NOT NULL,
    refresh_token  TEXT NOT NULL,
    expires_at     TIMESTAMPTZ NOT NULL,
    scopes         TEXT[] NOT NULL DEFAULT '{}',
    created_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
