-- migrate:up transaction:false

-- Slow object DELETE fix. Every object DELETE runs the "cleanup provisional
-- multipart uploads" query (delete_object_endpoint.py:79-83):
--   DELETE FROM multipart_uploads WHERE bucket_id=$1 AND object_key=$2 AND is_completed=FALSE
-- The only usable index is idx_multipart_uploads_bucket (bucket_id), so the plan
-- is an Index Scan on bucket_id + Filter on object_key: it scans EVERY row for the
-- bucket and filters object_key in the executor. On the largest buckets
-- (hippius-juicefs-data = 2aa2df10-..., ~39M rows; another bucket ~59M) this is a
-- disk-bound scan measured live at ~3-14.6 s per delete, with ~10 concurrent copies
-- stuck on IO/AioIoCompletion on the prod primary.
--
-- Regression origin: the original table carried UNIQUE(bucket_id, object_key,
-- upload_id) whose btree led with (bucket_id, object_key) and served this predicate
-- as a cheap seek. Migration 20260528120600 DROPPED it as "redundant" — correct
-- viewed purely as a uniqueness constraint (upload_id is already PK) but it was also
-- the only prefix cover for (bucket_id, object_key), so dropping it introduced the
-- full-bucket scan.
--
-- This restores prefix coverage in a far smaller, targeted form: a PARTIAL index
-- limited to is_completed = FALSE. Only ~953k of ~124M rows are incomplete (<1%), so
-- the index is a few tens of MB (vs the ~7 GB full unique that was dropped), and its
-- predicate matches the DELETE's `is_completed = FALSE` exactly, so the planner can
-- use it and turn the per-delete scan into a rows=1 seek.
--
-- CONCURRENTLY (+ `transaction:false` above) is mandatory: a plain CREATE INDEX takes
-- a SHARE lock that blocks every INSERT/UPDATE/DELETE on multipart_uploads for the
-- whole build, and migrations run before the API serves traffic on deploy — a plain
-- build at ~124M rows would stall every PUT/DELETE for minutes. CONCURRENTLY builds
-- without the write lock, but Postgres forbids it inside a transaction block — hence
-- the `transaction:false` directive. NOTE: even partial, the build must scan the full
-- 28 GB heap (twice) to find matching rows, so it adds I/O to an already-hot primary;
-- prefer a lower-traffic window and watch fs/IO while it runs.
--
-- Recovery caveat (same as idx_parts_upload_uploaded_at): a CONCURRENTLY build that
-- fails midway leaves an INVALID index behind, which `IF NOT EXISTS` then SKIPS on the
-- next run. If this migration errors, drop the leftover
-- (`DROP INDEX CONCURRENTLY idx_mpu_bucket_key_incomplete;`) or REINDEX it before
-- re-running. Keep this as a SINGLE statement: a `transaction:false` body runs as one
-- simple query and any multi-statement string runs in an implicit transaction, which
-- makes CREATE INDEX CONCURRENTLY fail with "cannot run inside a transaction block".
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_mpu_bucket_key_incomplete
    ON multipart_uploads (bucket_id, object_key)
    WHERE is_completed = FALSE;

-- migrate:down

DROP INDEX IF EXISTS idx_mpu_bucket_key_incomplete;
