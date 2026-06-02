-- migrate:up

-- Drop unused index on part_chunks (0 prod scans, ~864 MB).
-- See indexes.md Tier 1. Run CONCURRENTLY out-of-band via
-- k8s/cleanup-indexes-staging-apply.yaml.
DROP INDEX IF EXISTS idx_part_chunks_created_at;

-- migrate:down

-- Rollback: confirm exact indexdef from dryrun output before relying on this.
CREATE INDEX IF NOT EXISTS idx_part_chunks_created_at
    ON public.part_chunks (created_at);
