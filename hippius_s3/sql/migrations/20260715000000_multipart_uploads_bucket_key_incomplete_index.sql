-- migrate:up

-- Slow object DELETE fix. Every object DELETE runs the "cleanup provisional
-- multipart uploads" query (delete_object_endpoint.py):
--   DELETE FROM multipart_uploads WHERE bucket_id=$1 AND object_key=$2 AND is_completed=FALSE
-- The only usable index is idx_multipart_uploads_bucket (bucket_id), so the plan
-- is an Index Scan on bucket_id + Filter on object_key: it scans EVERY row for the
-- bucket. On prod multipart_uploads is ~38 GB / ~124M rows (only ~953k incomplete;
-- completed rows are never pruned), so on the largest buckets (hippius-juicefs-data,
-- ~39M rows) each delete is a disk-bound scan measured live at ~3-14.6 s, with ~10
-- concurrent copies stuck on IO/AioIoCompletion on the primary.
--
-- Regression origin: the table originally carried UNIQUE(bucket_id, object_key,
-- upload_id) whose btree led with (bucket_id, object_key) and served this predicate
-- as a seek. Migration 20260528120600 dropped it as "redundant" (true as a uniqueness
-- constraint - upload_id is already PK) but it was the only prefix cover, so dropping
-- it introduced the full-bucket scan.
--
-- This restores prefix coverage in a far smaller, targeted form: a PARTIAL index
-- limited to is_completed = FALSE. Only ~953k of ~124M rows are incomplete (<1%), so
-- the index is a few tens of MB (vs the ~7 GB full unique that was dropped), and its
-- predicate matches the DELETE's `is_completed = FALSE` exactly, so the planner can
-- use it and turn the per-delete scan into a rows=1 seek.
--
-- Built CONCURRENTLY out-of-band on prod via a k8s apply job (mirror
-- k8s/cleanup-indexes-staging-apply.yaml / migrate-recent-uploads-index-apply.yaml);
-- that job inserts the schema_migrations row before this file runs via dbmate, so
-- dbmate sees it applied and skips it (no SHARE lock stalling PUT/DELETE on the 38 GB
-- table during deploy). On a fresh DB (dbmate's path) the plain CREATE INDEX below is
-- a no-op-cost build on an empty table. Keep the file body a plain CREATE INDEX (no
-- CONCURRENTLY): dbmate wraps migrations in a transaction and CONCURRENTLY cannot run
-- inside one — the concurrency happens in the out-of-band job, not here.
CREATE INDEX IF NOT EXISTS idx_mpu_bucket_key_incomplete
    ON multipart_uploads (bucket_id, object_key)
    WHERE is_completed = FALSE;

-- migrate:down

DROP INDEX IF EXISTS idx_mpu_bucket_key_incomplete;
