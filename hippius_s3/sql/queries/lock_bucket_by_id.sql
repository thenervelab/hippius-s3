-- Serialize PutBucketPolicy against itself. Two policy writes that both observed a
-- private ACL would otherwise publish a prefix set and a whole-bucket grant together
-- without either writer noticing.
-- Parameters: $1: bucket_id
SELECT bucket_id
FROM buckets
WHERE bucket_id = $1
  AND deleted_at IS NULL
FOR UPDATE
