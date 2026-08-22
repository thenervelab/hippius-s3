-- Newest still-live version of an object, excluding one. Used to roll current_object_version back
-- when the current version is deleted.
-- Parameters: $1: object_id (uuid), $2: excluded object_version (bigint)
SELECT object_version
FROM object_versions
WHERE object_id = $1
  AND object_version <> $2
  AND deleted_at IS NULL
ORDER BY object_version DESC
LIMIT 1
