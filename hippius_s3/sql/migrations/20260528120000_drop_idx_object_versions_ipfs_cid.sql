-- migrate:up

-- Drop unused index on object_versions (0 prod scans, ~353 MB).
-- Adds write amplification on every PUT/overwrite/append; nothing reads it.
-- See indexes.md Tier 1.
-- Run CONCURRENTLY out-of-band via k8s/cleanup-indexes-staging-apply.yaml;
-- that job inserts the schema_migrations row before this file runs via dbmate,
-- so dbmate sees it applied and skips it (no write lock during deploy).
DROP INDEX IF EXISTS idx_object_versions_ipfs_cid;

-- migrate:down

-- Rollback: recreate the index. Exact indexdef should be confirmed from the
-- dryrun job output (pg_indexes.indexdef); the line below is the best-guess
-- shape based on the index name.
CREATE INDEX IF NOT EXISTS idx_object_versions_ipfs_cid
    ON public.object_versions (ipfs_cid);
