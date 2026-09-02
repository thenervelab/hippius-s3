-- Update bucket object_lock configuration
-- Parameters: $1: bucket_id, $2: object_lock (JSONB, or NULL to clear)
UPDATE buckets
SET object_lock = $2
WHERE bucket_id = $1
RETURNING bucket_id, bucket_name, object_lock;
