-- migrate:up

-- Drop the recent-uploads index on object_versions (~2 prod scans, ~1302 MB).
-- The /user/recent_uploads feature is effectively unused (~2 scans ever in
-- prod). Companion test in tests/integration/test_user_recent_uploads_sql.py
-- was updated in the same change to drop the assertion that this index exists.
-- See indexes.md Tier 1 ("scrutinize first" item).
-- Run CONCURRENTLY out-of-band via k8s/cleanup-indexes-staging-apply.yaml.
DROP INDEX IF EXISTS idx_object_versions_last_modified_desc;

-- migrate:down

-- Rollback: recreate the original index from migration 20260427000000.
CREATE INDEX IF NOT EXISTS idx_object_versions_last_modified_desc
    ON public.object_versions (last_modified DESC);
