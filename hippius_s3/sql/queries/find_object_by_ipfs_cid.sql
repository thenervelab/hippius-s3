-- Find object by IPFS CID
-- Parameters: $1: ipfs_cid
SELECT o.object_id, o.bucket_id, o.object_key, o.ipfs_cid,
       o.size_bytes, o.content_type, o.created_at, o.metadata,
       b.bucket_name
FROM objects o
JOIN buckets b ON o.bucket_id = b.bucket_id
WHERE o.ipfs_cid = $1
