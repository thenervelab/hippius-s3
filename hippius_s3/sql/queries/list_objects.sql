-- List objects in a bucket with optional prefix
-- Parameters: $1: bucket_id, $2: prefix (optional)
SELECT o.object_id, o.bucket_id, o.object_key, o.current_object_version,
       COALESCE(c.cid, ov.ipfs_cid) as ipfs_cid,
       cb_arion.backend_identifier as arion_file_hash,
       ov.size_bytes, ov.content_type, o.created_at, ov.md5_hash,
       ov.status, b.bucket_name, ov.multipart
FROM objects o
JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
JOIN buckets b ON o.bucket_id = b.bucket_id
LEFT JOIN cids c ON ov.cid_id = c.id
LEFT JOIN parts p1 ON p1.object_id = o.object_id
  AND p1.object_version = o.current_object_version
  AND p1.part_number = 1
LEFT JOIN part_chunks pc0 ON pc0.part_id = p1.part_id
  AND pc0.chunk_index = 0
LEFT JOIN chunk_backend cb_arion ON cb_arion.chunk_id = pc0.id
  AND cb_arion.backend = 'arion'
  AND NOT cb_arion.deleted
  AND cb_arion.backend_identifier IS NOT NULL
WHERE o.bucket_id = $1
  AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
  AND o.deleted_at IS NULL
ORDER BY o.object_key COLLATE "C"
