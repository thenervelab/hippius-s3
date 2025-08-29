-- List objects in a bucket with optional prefix and pagination
-- Parameters: $1: bucket_id, $2: prefix (optional), $3: limit (default 1000), $4: offset (default 0)
SELECT o.object_id, o.bucket_id, o.object_key, o.ipfs_cid,
       o.size_bytes, o.content_type, o.created_at, o.md5_hash,
       b.bucket_name
FROM objects o
JOIN buckets b ON o.bucket_id = b.bucket_id
WHERE o.bucket_id = $1
  AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
ORDER BY o.object_key
LIMIT $3 OFFSET $4
