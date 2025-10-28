-- Parameters:
--   $1: object_id (uuid)
--   $2: expected_old_version (bigint)
--   $3: new_version (bigint)
-- Returns: updated row (object_id, current_object_version) if success
UPDATE objects
   SET current_object_version = $3
 WHERE object_id = $1
   AND current_object_version = $2
RETURNING object_id, current_object_version
;
