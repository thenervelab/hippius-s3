-- Update object status
-- Parameters: $1: object_id, $2: status
UPDATE objects
SET status = $2
WHERE object_id = $1
RETURNING object_id, status
