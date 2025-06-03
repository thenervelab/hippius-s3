-- Get bucket by name and owner
-- Parameters: $1: bucket_name, $2: owner_user_id
SELECT bucket_id, bucket_name, created_at, is_public, tags, owner_user_id
FROM buckets
WHERE bucket_name = $1 AND owner_user_id = $2
