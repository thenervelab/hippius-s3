-- Find object by IPFS CID
-- Parameters: $1: ipfs_cid
SELECT o.object_id, o.bucket_id, o.object_key,
       COALESCE(c.cid, ov.ipfs_cid) as ipfs_cid,
       ov.size_bytes, ov.content_type, o.created_at, ov.metadata,
       b.bucket_name
FROM objects o
JOIN object_versions ov ON ov.object_id = o.object_id AND ov.version_seq = o.current_version_seq
JOIN buckets b ON o.bucket_id = b.bucket_id
LEFT JOIN cids c ON ov.cid_id = c.id
WHERE COALESCE(c.cid, ov.ipfs_cid) = $1
