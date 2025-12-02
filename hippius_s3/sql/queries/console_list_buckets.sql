-- List buckets owned by a specific user with object count and total size (console/user endpoint)
-- Parameters: $1: main_account_id
SELECT
    b.bucket_id,
    b.bucket_name,
    b.created_at,
    ba.acl_json,
    b.tags,
    COALESCE(COUNT(o.object_id), 0)::bigint AS total_objects,
    COALESCE(SUM(ov.size_bytes), 0)::bigint AS total_size_bytes
FROM buckets b
LEFT JOIN bucket_acls ba ON ba.bucket_id = b.bucket_id
LEFT JOIN objects o ON o.bucket_id = b.bucket_id
LEFT JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
WHERE b.main_account_id = $1
GROUP BY b.bucket_id, b.bucket_name, b.created_at, ba.acl_json, b.tags
ORDER BY b.created_at DESC
