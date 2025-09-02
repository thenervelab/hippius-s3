-- Get multipart object chunks with CIDs for download and decryption
-- Parameters: $1: bucket_name, $2: object_key, $3: main_account_id
SELECT p.part_number, c.cid, p.size_bytes
FROM objects o
JOIN buckets b ON o.bucket_id = b.bucket_id
JOIN users u ON b.user_id = u.user_id
JOIN parts p ON p.object_id = o.object_id
JOIN cids c ON p.cid_id = c.id
WHERE b.bucket_name = $1
  AND o.object_key = $2
  AND u.main_account_id = $3
  AND o.multipart = TRUE
ORDER BY p.part_number ASC
