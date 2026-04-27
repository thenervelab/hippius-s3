-- Most recent 10 uploads (one row per object) for a main_account_id.
-- Joins on objects.current_object_version so each object appears at most once.
-- Excludes soft-deleted objects and version rows whose backend processing failed.
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
    o.created_at AS uploaded_at
FROM buckets b
JOIN objects o ON o.bucket_id = b.bucket_id
JOIN object_versions ov
    ON ov.object_id = o.object_id
   AND ov.object_version = o.current_object_version
LEFT JOIN cids c ON c.id = ov.cid_id
WHERE b.main_account_id = $1
  AND o.deleted_at IS NULL
  AND ov.status <> 'failed'
ORDER BY o.created_at DESC
LIMIT 10
