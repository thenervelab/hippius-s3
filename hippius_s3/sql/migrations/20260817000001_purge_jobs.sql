-- migrate:up

-- Async account-purge jobs (issue #421). One row per DELETE /admin/accounts/{id}/data.
-- The purger worker claims queued rows (FOR UPDATE SKIP LOCKED) and drives the existing
-- soft-delete -> unpinner -> janitor pipeline; deleted_bytes is LOGICAL bytes purged
-- (sum of object_versions.size_bytes), not reclaimed disk, which trails asynchronously.
CREATE TABLE purge_jobs (
    job_id          UUID PRIMARY KEY,
    account_id      VARCHAR(255) NOT NULL,
    state           VARCHAR(16)  NOT NULL DEFAULT 'queued',
    deleted_objects BIGINT       NOT NULL DEFAULT 0,
    deleted_bytes   BIGINT       NOT NULL DEFAULT 0,
    error           TEXT,
    progress        JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_at      TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    started_at      TIMESTAMP WITH TIME ZONE,
    finished_at     TIMESTAMP WITH TIME ZONE,
    heartbeat_at    TIMESTAMP WITH TIME ZONE,
    CONSTRAINT ck_purge_jobs_state CHECK (state IN ('queued', 'running', 'done', 'failed'))
);

-- At most one live purge per account: repeat DELETE .../data returns the active job_id.
CREATE UNIQUE INDEX uq_purge_jobs_account_active
    ON purge_jobs (account_id)
    WHERE state IN ('queued', 'running');

-- migrate:down

DROP TABLE IF EXISTS purge_jobs;
