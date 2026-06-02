-- migrate:up

-- Drop unused index on object_versions (0 prod scans, ~350 MB).
-- See indexes.md Tier 1. Run CONCURRENTLY out-of-band via
-- k8s/cleanup-indexes-staging-apply.yaml.
DROP INDEX IF EXISTS idx_object_versions_kek_id;

-- migrate:down

-- Rollback: confirm exact indexdef from dryrun output before relying on this.
CREATE INDEX IF NOT EXISTS idx_object_versions_kek_id
    ON public.object_versions (kek_id);
