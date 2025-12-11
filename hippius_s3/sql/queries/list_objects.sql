-- List objects in a bucket with optional prefix
-- Parameters: $1: bucket_id, $2: prefix (optional)
SELECT o.object_id, o.bucket_id, o.object_key, o.current_object_version,
       COALESCE(c.cid, ov.ipfs_cid) as ipfs_cid,
       ov.size_bytes, ov.content_type, o.created_at, ov.md5_hash,
       ov.status, b.bucket_name, ov.multipart
FROM objects o
JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
JOIN buckets b ON o.bucket_id = b.bucket_id
LEFT JOIN cids c ON ov.cid_id = c.id
WHERE o.bucket_id = $1
  AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
  AND ov.status != 'failed'
ORDER BY o.object_key COLLATE "C"
