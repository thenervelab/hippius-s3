-- Live public prefixes for one bucket, oldest-name order.
-- Parameters: $1: bucket_id
-- Soft-deleted buckets keep their rows (ON DELETE CASCADE fires only on a hard
-- delete) and the name can be reused, so a read that skipped this join would
-- publish the previous tenant's prefixes under the new bucket.
SELECT p.prefix
FROM bucket_public_prefixes p
JOIN buckets b ON b.bucket_id = p.bucket_id
WHERE p.bucket_id = $1
  AND b.deleted_at IS NULL
ORDER BY p.prefix
