-- Get multipart object chunks with CIDs via multipart_uploads relationship
-- Parameters: $1: object_id
SELECT p.part_number, c.cid, p.size_bytes
FROM multipart_uploads mu
JOIN parts p ON p.upload_id = mu.upload_id
JOIN cids c ON p.cid_id = c.id
WHERE mu.object_id = $1
  AND mu.is_completed = TRUE
ORDER BY p.part_number ASC
