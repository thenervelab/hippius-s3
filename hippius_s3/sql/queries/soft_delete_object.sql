-- $1 bucket_id, $2 object_key
UPDATE objects
SET deleted_at = now()
WHERE bucket_id = $1 AND object_key = $2 AND deleted_at IS NULL
RETURNING object_id, current_object_version;
