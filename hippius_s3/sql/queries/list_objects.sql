-- List objects in a bucket with optional prefix
-- Parameters: $1: bucket_id, $2: prefix (optional)
SELECT o.object_id, o.bucket_id, o.object_key,
       COALESCE(c.cid, o.ipfs_cid) as ipfs_cid,
       o.size_bytes, o.content_type, o.created_at, o.md5_hash,
       o.status, b.bucket_name, o.multipart
FROM objects o
JOIN buckets b ON o.bucket_id = b.bucket_id
LEFT JOIN cids c ON o.cid_id = c.id
WHERE o.bucket_id = $1
  AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
ORDER BY o.object_key COLLATE "C"
