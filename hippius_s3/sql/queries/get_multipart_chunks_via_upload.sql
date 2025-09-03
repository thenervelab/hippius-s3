-- Get multipart object chunks with CIDs via direct object_id reference
-- Parameters: $1: object_id
SELECT p.part_number, c.cid, p.size_bytes
FROM parts p
JOIN cids c ON p.cid_id = c.id
WHERE p.object_id = $1
ORDER BY p.part_number ASC
