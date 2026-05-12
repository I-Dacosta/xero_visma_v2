ALTER TABLE xero.sync_run
    ADD COLUMN IF NOT EXISTS trigger_id UUID NULL;

CREATE INDEX IF NOT EXISTS sync_run_trigger_idx
    ON xero.sync_run (tenant_id, trigger_id, started_at DESC)
    WHERE trigger_id IS NOT NULL;
