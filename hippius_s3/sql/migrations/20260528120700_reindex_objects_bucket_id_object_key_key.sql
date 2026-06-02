-- migrate:up

-- Reclaim bloat on the active uniqueness index for the PUT path on objects
-- (~5.4 GB, ~17M scans in prod). The index is heavily used; we are NOT
-- dropping it, just rebuilding it CONCURRENTLY to reclaim accumulated bloat.
-- See indexes.md Tier 3.
--
-- Run CONCURRENTLY out-of-band via k8s/cleanup-indexes-staging-apply.yaml;
-- that job inserts the schema_migrations row before this file runs via dbmate,
-- so dbmate sees it applied and skips it. On a fresh DB (dbmate's path) this
-- is a no-op (no bloat to reclaim on a brand-new index).
REINDEX INDEX objects_bucket_id_object_key_key;

-- migrate:down

-- REINDEX is in-place; no semantic rollback exists. No-op by design.
SELECT 1;
