-- Delete a bucket (gateway handles permission checks)
-- Parameters: $1: bucket_id
DELETE FROM buckets
WHERE bucket_id = $1
RETURNING bucket_id, bucket_name
