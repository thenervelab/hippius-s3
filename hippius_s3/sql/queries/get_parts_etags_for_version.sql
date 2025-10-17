-- Get parts etags for multipart upload hashing (specific version)
-- Parameters: $1: object_id, $2: object_version
SELECT etag, part_number
FROM parts
WHERE object_id = $1 AND object_version = $2
ORDER BY part_number ASC
