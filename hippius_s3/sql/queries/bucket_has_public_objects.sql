-- Whether this live bucket publishes anonymous GET/HEAD of every current key.
-- Parameters: $1: bucket_id
-- A soft-deleted bucket keeps the row until a hard delete cascades. The name
-- can be reused, so a read that skipped this join would publish the next tenant.
-- Parameters: $1: bucket_id
-- Soft-deleted buckets keep the row (ON DELETE CASCADE fires only on a hard
-- delete) and the name can be reused, so a read that skipped this join would
-- publish the previous tenant's objects under the new bucket.
-- `published` is the column the reader checks. A row from another query on
-- this connection must not count as the grant.
SELECT 1 AS published
FROM bucket_public_objects o
JOIN buckets b ON b.bucket_id = o.bucket_id
WHERE o.bucket_id = $1
  AND b.deleted_at IS NULL
