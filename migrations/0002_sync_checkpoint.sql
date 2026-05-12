-- Per-entity sync checkpoint.
-- Composite key: (tenant_id, entity_type) — one watermark per entity.

CREATE TABLE IF NOT EXISTS xero.sync_checkpoint (
    tenant_id                TEXT NOT NULL REFERENCES xero.tenants (tenant_id),
    entity_type              TEXT NOT NULL,

    -- REST watermark: Xero `ModifiedAfter` header value
    last_modified_watermark  TIMESTAMPTZ,

    last_sync_at             TIMESTAMPTZ,
    records_seen             BIGINT NOT NULL DEFAULT 0,

    created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    PRIMARY KEY (tenant_id, entity_type)
);

CREATE INDEX IF NOT EXISTS sync_checkpoint_entity_idx
    ON xero.sync_checkpoint (entity_type);

CREATE INDEX IF NOT EXISTS sync_checkpoint_tenant_idx
    ON xero.sync_checkpoint (tenant_id);
