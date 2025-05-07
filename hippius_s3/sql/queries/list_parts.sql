-- List parts for a multipart upload
-- Parameters: $1: upload_id
SELECT *
FROM parts
WHERE upload_id = $1
ORDER BY part_number ASC
