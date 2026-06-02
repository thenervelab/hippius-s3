-- migrate:up

-- Reclaim bloat on the bucket-prefix list/scan index on objects
-- (~5.4 GB, ~114M scans in prod). The index is heavily used; rebuilding it
-- CONCURRENTLY to reclaim accumulated bloat. See indexes.md Tier 3.
--
-- Run CONCURRENTLY out-of-band via k8s/cleanup-indexes-staging-apply.yaml;
-- that job inserts the schema_migrations row before this file runs via dbmate,
-- so dbmate sees it applied and skips it. On a fresh DB (dbmate's path) this
-- is a no-op (no bloat to reclaim on a brand-new index).
REINDEX INDEX idx_objects_bucket_prefix;

-- migrate:down

-- REINDEX is in-place; no semantic rollback exists. No-op by design.
SELECT 1;
