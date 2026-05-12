CREATE TABLE IF NOT EXISTS xero.sync_schedule (
    schedule_id        UUID PRIMARY KEY,
    tenant_id          TEXT NOT NULL REFERENCES xero.tenants (tenant_id),
    name               TEXT NOT NULL,
    cron_expression    TEXT NOT NULL,
    entities           JSONB NOT NULL,
    from_date          DATE NULL,
    to_date            DATE NULL,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    disabled_at        TIMESTAMPTZ NULL,
    last_triggered_at  TIMESTAMPTZ NULL,
    CONSTRAINT sync_schedule_entities_is_array CHECK (
        jsonb_typeof(entities) = 'array' AND jsonb_array_length(entities) > 0
    )
);

CREATE INDEX IF NOT EXISTS sync_schedule_tenant_created_idx
    ON xero.sync_schedule (tenant_id, created_at DESC);
