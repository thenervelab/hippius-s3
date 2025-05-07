-- List objects in a bucket with optional prefix
-- Parameters: $1: bucket_id, $2: prefix (optional)
SELECT o.object_id, o.bucket_id, o.object_key, o.ipfs_cid,
       o.size_bytes, o.content_type, o.created_at,
       b.bucket_name
FROM objects o
JOIN buckets b ON o.bucket_id = b.bucket_id
WHERE o.bucket_id = $1
  AND ($2 IS NULL OR o.object_key LIKE $2 || '%')
ORDER BY o.object_key
