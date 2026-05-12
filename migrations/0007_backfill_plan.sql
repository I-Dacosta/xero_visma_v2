-- Backfill orchestrator state.
--
-- A `backfill_plan` is a high-level request: "sync entities X, Y, Z for tenant T
-- from date A to date B in monthly chunks". It is decomposed into
-- `backfill_chunk` rows, each of which is run as a real sync (linked via
-- `sync_run.run_id`).
--
-- Chunks are picked up by the worker using SELECT … FOR UPDATE SKIP LOCKED so
-- multiple worker replicas can coordinate safely.

CREATE TABLE IF NOT EXISTS xero.backfill_plan (
    plan_id          uuid        PRIMARY KEY,
    tenant_id        text        NOT NULL REFERENCES xero.tenants(tenant_id),
    entity_types     jsonb       NOT NULL,
    start_date       date        NOT NULL,
    end_date         date        NOT NULL,
    chunk_size_days  integer     NOT NULL DEFAULT 30,
    status           text        NOT NULL DEFAULT 'pending',
    total_chunks     integer     NOT NULL DEFAULT 0,
    completed_chunks integer     NOT NULL DEFAULT 0,
    failed_chunks    integer     NOT NULL DEFAULT 0,
    triggered_by     text        NOT NULL DEFAULT 'manual',
    error_message    text,
    created_at       timestamptz NOT NULL DEFAULT now(),
    started_at       timestamptz,
    completed_at     timestamptz,
    CONSTRAINT backfill_plan_window_check CHECK (end_date > start_date),
    CONSTRAINT backfill_plan_status_check CHECK (
        status IN ('pending','running','completed','failed','cancelled')
    )
);

CREATE INDEX IF NOT EXISTS backfill_plan_tenant_idx
    ON xero.backfill_plan (tenant_id, created_at DESC);

CREATE TABLE IF NOT EXISTS xero.backfill_chunk (
    chunk_id       uuid        PRIMARY KEY,
    plan_id        uuid        NOT NULL REFERENCES xero.backfill_plan(plan_id) ON DELETE CASCADE,
    tenant_id      text        NOT NULL,
    entity_type    text        NOT NULL,
    window_start   date        NOT NULL,
    window_end     date        NOT NULL,
    status         text        NOT NULL DEFAULT 'pending',
    attempt_count  integer     NOT NULL DEFAULT 0,
    max_attempts   integer     NOT NULL DEFAULT 3,
    run_id         uuid        REFERENCES xero.sync_run(run_id),
    error_message  text,
    created_at     timestamptz NOT NULL DEFAULT now(),
    started_at     timestamptz,
    finished_at    timestamptz,
    CONSTRAINT backfill_chunk_window_check CHECK (window_end > window_start),
    CONSTRAINT backfill_chunk_status_check CHECK (
        status IN ('pending','running','succeeded','failed','skipped')
    )
);

CREATE INDEX IF NOT EXISTS backfill_chunk_plan_status_idx
    ON xero.backfill_chunk (plan_id, status);

-- Worker poll hot path: cheap to find next pending chunk across all plans.
CREATE INDEX IF NOT EXISTS backfill_chunk_pending_idx
    ON xero.backfill_chunk (status, created_at)
    WHERE status = 'pending';
