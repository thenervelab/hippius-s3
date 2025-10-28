-- Delete a specific object version and its parts
-- Parameters:
--   $1: object_id (uuid)
--   $2: object_version (bigint)
WITH del_parts AS (
  DELETE FROM parts
   WHERE object_id = $1
     AND object_version = $2
  RETURNING 1
), del_ver AS (
  DELETE FROM object_versions
   WHERE object_id = $1
     AND object_version = $2
  RETURNING 1
)
SELECT (SELECT COUNT(*) FROM del_parts) AS parts_deleted,
       (SELECT COUNT(*) FROM del_ver) AS versions_deleted
;
