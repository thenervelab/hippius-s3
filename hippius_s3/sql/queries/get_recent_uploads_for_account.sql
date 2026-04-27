-- Most recent 10 uploads (deduplicated by object) for a main_account_id.
-- $1: main_account_id
SELECT
    o.object_id,
    o.object_key,
    o.bucket_id,
    b.bucket_name,
    ov.size_bytes,
    ov.content_type,
    ov.md5_hash,
    COALESCE(c.cid, ov.ipfs_cid) AS ipfs_cid,
    o.last_modified AS uploaded_at
FROM objects o
JOIN buckets b ON b.bucket_id = o.bucket_id
JOIN object_versions ov
    ON ov.object_id = o.object_id
   AND ov.object_version = o.current_object_version
LEFT JOIN cids c ON c.id = ov.cid_id
WHERE b.main_account_id = $1
  AND o.deleted_at IS NULL
ORDER BY o.last_modified DESC
LIMIT 10
