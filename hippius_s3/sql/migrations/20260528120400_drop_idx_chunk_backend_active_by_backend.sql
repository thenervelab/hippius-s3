-- migrate:up

-- Drop unused index on chunk_backend (~3 prod scans, ~806 MB).
-- See indexes.md Tier 1. Run CONCURRENTLY out-of-band via
-- k8s/cleanup-indexes-staging-apply.yaml.
DROP INDEX IF EXISTS idx_chunk_backend_active_by_backend;

-- migrate:down

-- Rollback (indexdef captured from the dryrun job, 2026-05-28 staging).
CREATE INDEX IF NOT EXISTS idx_chunk_backend_active_by_backend
    ON public.chunk_backend USING btree (backend)
    WHERE (NOT deleted);
