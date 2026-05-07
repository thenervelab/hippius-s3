-- List all buckets
SELECT bucket_id, bucket_name, created_at, is_public, tags
FROM buckets
WHERE deleted_at IS NULL
ORDER BY created_at DESC
