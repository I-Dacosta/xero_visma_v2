-- Track whether a bronze row has been published to BigQuery.
-- NULL = not yet published (or BQ sink not configured).
-- Replayer (POST /tenants/:t/bq/replay) picks NULL rows and pushes them.

ALTER TABLE xero.local_bronze_record
    ADD COLUMN IF NOT EXISTS bq_synced_at timestamptz;

CREATE INDEX IF NOT EXISTS local_bronze_record_bq_pending_idx
    ON xero.local_bronze_record (tenant_id, entity_type)
    WHERE bq_synced_at IS NULL;
