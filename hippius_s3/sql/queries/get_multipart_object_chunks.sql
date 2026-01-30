-- Get multipart object chunks with CIDs for download and decryption
-- Parameters: $1: bucket_name, $2: object_key, $3: main_account_id
SELECT p.part_number, c.cid, p.size_bytes
FROM objects o
JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
JOIN buckets b ON o.bucket_id = b.bucket_id
JOIN parts p ON p.object_id = o.object_id AND p.object_version = ov.object_version
JOIN cids c ON p.cid_id = c.id
WHERE b.bucket_name = $1
  AND o.object_key = $2
  AND b.main_account_id = $3
  AND ov.multipart = TRUE
  AND o.deleted_at IS NULL
ORDER BY p.part_number ASC
