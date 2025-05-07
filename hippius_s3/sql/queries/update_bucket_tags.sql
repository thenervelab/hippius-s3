-- Update bucket tags
-- Parameters: $1: bucket_id, $2: tags (JSONB)
UPDATE buckets
SET tags = $2
WHERE bucket_id = $1
RETURNING bucket_id, bucket_name, tags;
