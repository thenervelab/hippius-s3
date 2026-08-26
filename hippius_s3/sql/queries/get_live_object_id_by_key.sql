-- Live primary name only (not aliases). $1 bucket_id, $2 object_key
SELECT object_id
FROM objects
WHERE bucket_id = $1
  AND object_key = $2
  AND deleted_at IS NULL
LIMIT 1
