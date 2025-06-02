-- List buckets owned by a specific user
-- Parameters: $1: owner_user_id
SELECT bucket_id, bucket_name, created_at, is_public, tags
FROM buckets
WHERE owner_user_id = $1
ORDER BY created_at DESC
