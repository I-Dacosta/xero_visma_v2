-- Local bronze sink in Postgres for dry-run validation without BigQuery.
-- Stores latest payload per logical record key and tenant/entity.

CREATE TABLE IF NOT EXISTS xero.local_bronze_record (
    tenant_id       TEXT NOT NULL REFERENCES xero.tenants (tenant_id),
    entity_type     TEXT NOT NULL,
    record_id       TEXT NOT NULL,
    record_id_json  JSONB NOT NULL,
    payload         JSONB NOT NULL,
    first_seen_at   TIMESTAMPTZ NOT NULL,
    last_seen_at    TIMESTAMPTZ NOT NULL,
    last_run_id     UUID NOT NULL REFERENCES xero.sync_run (run_id),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (tenant_id, entity_type, record_id)
);

CREATE INDEX IF NOT EXISTS local_bronze_record_entity_idx
    ON xero.local_bronze_record (tenant_id, entity_type);

CREATE INDEX IF NOT EXISTS local_bronze_record_last_seen_idx
    ON xero.local_bronze_record (tenant_id, entity_type, last_seen_at DESC);
