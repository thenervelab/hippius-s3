-- Soft-delete a bucket. Returns the row on first call, zero rows on a repeat
-- call against an already-soft-deleted bucket. Caller treats zero rows as
-- 404 NoSuchBucket (NOT 403 — auth/ACL is enforced upstream).
-- The bucket_reaper worker drains child rows and hard-deletes the bucket row.
-- Parameters: $1: bucket_id
UPDATE buckets
SET deleted_at = now()
WHERE bucket_id = $1
  AND deleted_at IS NULL
RETURNING bucket_id, bucket_name
