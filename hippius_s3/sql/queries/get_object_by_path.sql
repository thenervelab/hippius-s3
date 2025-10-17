-- Get object by bucket and key path
-- Parameters: $1: bucket_id, $2: object_key
SELECT o.object_id, o.bucket_id, o.object_key,
       COALESCE(c.cid, ov.ipfs_cid, '') as ipfs_cid,
       ov.size_bytes, ov.content_type, o.created_at, ov.metadata, ov.md5_hash,
       ov.append_version,
       ov.storage_version,
       ov.object_version,
       b.bucket_name
FROM objects o
JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
JOIN buckets b ON o.bucket_id = b.bucket_id
LEFT JOIN cids c ON ov.cid_id = c.id
WHERE o.bucket_id = $1 AND o.object_key = $2
ORDER BY o.created_at DESC LIMIT 1
