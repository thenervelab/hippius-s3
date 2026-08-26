-- List objects in a bucket with optional prefix and pagination (console/user endpoint)
-- Parameters: $1: bucket_id, $2: prefix (optional), $3: limit, $4: offset
-- ipfs_cid is the legacy manifest CID: NULL on everything written since the Arion cutover, kept
-- only so pre-2026 rows still resolve. body_blake3 is the live digest and the one the console
-- should read — it is the same value ListObjects surfaces in Owner.ID.
SELECT o.object_id, o.bucket_id, o.object_key, o.current_object_version,
       COALESCE(c.cid, ov.ipfs_cid) as ipfs_cid,
       ov.body_blake3,
       ov.size_bytes, ov.content_type, o.created_at, ov.md5_hash,
       ov.status, b.bucket_name, ov.multipart
FROM (
    SELECT o.object_id, o.bucket_id, o.object_key, o.current_object_version, o.created_at
    FROM objects o
    WHERE o.bucket_id = $1
      AND o.deleted_at IS NULL
      AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
    UNION ALL
    SELECT o.object_id, n.bucket_id, n.object_key, o.current_object_version, n.created_at
    FROM object_names n
    JOIN objects o ON o.object_id = n.object_id AND o.deleted_at IS NULL
    WHERE n.bucket_id = $1
      AND ($2::text IS NULL OR n.object_key LIKE $2::text || '%')
) o
JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
 AND NOT ov.is_delete_marker
JOIN buckets b ON o.bucket_id = b.bucket_id
LEFT JOIN cids c ON ov.cid_id = c.id
WHERE o.bucket_id = $1
  AND b.deleted_at IS NULL
  AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
ORDER BY o.object_key COLLATE "C"
LIMIT $3 OFFSET $4
