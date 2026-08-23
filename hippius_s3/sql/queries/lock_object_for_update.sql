-- Take the objects row lock before a multi-statement version mutation, so concurrent versioned
-- DELETEs on the same key serialise instead of interleaving between the soft-delete and the
-- current-pointer repoint.
-- Parameters: $1: bucket_id (uuid), $2: object_key (text)
SELECT object_id, current_object_version
FROM objects
WHERE bucket_id = $1
  AND object_key = $2
  AND deleted_at IS NULL
FOR UPDATE
