-- Most recent 10 uploads (one row per object) for a main_account_id.
-- Uses a per-bucket LATERAL join so the planner can push LIMIT into the
-- per-bucket index scan on idx_objects_bucket_created_desc_active. Without
-- LATERAL, the planner materializes every matching object before sorting and
-- LIMIT-ing, which makes accounts with millions of objects in a single bucket
-- (one prod account has 11.8M) take 100+ seconds. With LATERAL the work is
-- bounded by buckets * 10 candidate rows.
--
-- Joins on objects.current_object_version so each object appears at most once.
-- Excludes soft-deleted objects and version rows whose backend processing failed.
-- $1: main_account_id
SELECT
    recent.object_id,
    recent.object_key,
    recent.bucket_id,
    b.bucket_name,
    ov.size_bytes,
    ov.content_type,
    ov.md5_hash,
    COALESCE(c.cid, ov.ipfs_cid) AS ipfs_cid,
    recent.created_at AS uploaded_at
FROM buckets b
CROSS JOIN LATERAL (
    SELECT o.object_id, o.object_key, o.bucket_id, o.current_object_version, o.created_at
    FROM objects o
    WHERE o.bucket_id = b.bucket_id
      AND o.deleted_at IS NULL
    ORDER BY o.created_at DESC
    LIMIT 10
) recent
JOIN object_versions ov
    ON ov.object_id = recent.object_id
   AND ov.object_version = recent.current_object_version
LEFT JOIN cids c ON c.id = ov.cid_id
WHERE b.main_account_id = $1
  AND ov.status <> 'failed'
ORDER BY recent.created_at DESC
LIMIT 10
