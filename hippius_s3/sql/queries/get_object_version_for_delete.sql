-- Resolve one specific, still-live version of a key for a versioned DELETE.
-- Returns nothing when the bucket/key/version does not exist or the version is already deleted,
-- which the caller turns into an idempotent 204.
-- Parameters: $1: bucket_id (uuid), $2: object_key (text), $3: object_version (bigint)
SELECT o.object_id,
       ov.object_version,
       ov.is_delete_marker,
       o.current_object_version
FROM objects o
JOIN object_versions ov
  ON ov.object_id = o.object_id
 AND ov.object_version = $3
WHERE o.bucket_id = $1
  AND o.object_key = $2
  AND o.deleted_at IS NULL
  AND ov.deleted_at IS NULL
