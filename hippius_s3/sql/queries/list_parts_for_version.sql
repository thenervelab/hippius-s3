-- List parts for a specific object version
-- Parameters: $1: object_id, $2: object_version_seq
SELECT *
FROM parts
WHERE object_id = $1 AND object_version_seq = $2
ORDER BY part_number ASC
