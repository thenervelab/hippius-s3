-- List objects in a bucket with optional prefix and keyset pagination.
-- Parameters: $1: bucket_id, $2: prefix (optional), $3: cursor / start-after key (optional), $4: limit
SELECT o.object_id,
       o.object_key,
       ov.size_bytes,
       ov.content_type,
       o.created_at,
       ov.md5_hash,
       ov.status,
       ov.multipart
FROM objects o
JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = (
    -- Skip incomplete multipart placeholders (InitiateMultipartUpload without Complete)
    SELECT v.object_version
    FROM object_versions v
    WHERE v.object_id = o.object_id
      AND v.object_version <= o.current_object_version
      AND (v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
    ORDER BY v.object_version DESC
    LIMIT 1
)
WHERE o.bucket_id = $1
  AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
  AND ($3::text IS NULL OR o.object_key > $3::text)
  AND o.deleted_at IS NULL
-- DB is C-collation; an explicit COLLATE here would defeat the (bucket_id, object_key) index ordered scan.
ORDER BY o.object_key
LIMIT $4::int
