-- Get bucket by name
-- Parameters: $1: bucket_name
SELECT bucket_id, bucket_name, created_at, is_public, tags, main_account_id
FROM buckets
WHERE bucket_name = $1
LIMIT 1
