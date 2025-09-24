-- Get parts etags for multipart upload hashing
-- Parameters: $1: object_id
SELECT etag, part_number
FROM parts
WHERE object_id = $1
ORDER BY part_number ASC
