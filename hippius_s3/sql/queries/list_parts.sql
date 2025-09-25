-- List parts for a multipart upload
-- Parameters: $1: object_id
SELECT *
FROM parts
WHERE object_id = $1
ORDER BY part_number ASC
