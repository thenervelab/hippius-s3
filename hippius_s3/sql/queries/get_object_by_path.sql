-- Get object by bucket and key path
-- Parameters: $1: bucket_id, $2: object_key
SELECT o.object_id, o.bucket_id, o.object_key, o.ipfs_cid,
       o.size_bytes, o.content_type, o.created_at, o.metadata,
       b.bucket_name
FROM objects o
JOIN buckets b ON o.bucket_id = b.bucket_id
WHERE o.bucket_id = $1 AND o.object_key = $2
ORDER BY o.created_at DESC LIMIT 1
