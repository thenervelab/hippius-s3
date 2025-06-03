-- Get bucket by name and owner
-- Parameters: $1: bucket_name, $2: main_account_id
SELECT bucket_id, bucket_name, created_at, is_public, tags, main_account_id
FROM buckets
WHERE bucket_name = $1 AND main_account_id = $2
