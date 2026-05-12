-- Run history: one row per sync job execution.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t
        JOIN   pg_namespace n ON n.oid = t.typnamespace
        WHERE  t.typname = 'run_status' AND n.nspname = 'xero'
    ) THEN
        CREATE TYPE xero.run_status AS ENUM (
            'running',
            'succeeded',
            'failed',
            'cancelled'
        );
    END IF;
END
$$;

CREATE TABLE IF NOT EXISTS xero.sync_run (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id       TEXT NOT NULL REFERENCES xero.tenants (tenant_id),
    entity_type     TEXT NOT NULL,
    job_type        TEXT NOT NULL DEFAULT 'incremental',  -- 'full' | 'incremental'

    status          xero.run_status NOT NULL DEFAULT 'running',

    records_fetched BIGINT NOT NULL DEFAULT 0,
    records_loaded  BIGINT NOT NULL DEFAULT 0,
    records_failed  BIGINT NOT NULL DEFAULT 0,

    error_message   TEXT,

    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,

    triggered_by    TEXT NOT NULL DEFAULT 'scheduler'   -- 'scheduler' | 'manual' | 'cli'
);

CREATE INDEX IF NOT EXISTS sync_run_tenant_entity_idx
    ON xero.sync_run (tenant_id, entity_type, started_at DESC);

CREATE INDEX IF NOT EXISTS sync_run_status_idx
    ON xero.sync_run (status);
