-- Get parts etags for multipart upload hashing
-- Parameters: $1: upload_id
SELECT etag, part_number
FROM parts
WHERE upload_id = $1
ORDER BY part_number ASC
