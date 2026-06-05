-- migrate:up

-- Drop unused index on object_versions (~1 prod scan, ~351 MB).
-- See indexes.md Tier 1. Run CONCURRENTLY out-of-band via
-- k8s/cleanup-indexes-staging-apply.yaml.
DROP INDEX IF EXISTS idx_object_versions_status;

-- migrate:down

-- Rollback: confirm exact indexdef from dryrun output before relying on this.
CREATE INDEX IF NOT EXISTS idx_object_versions_status
    ON public.object_versions (status);
