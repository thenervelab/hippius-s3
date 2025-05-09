-- Update an object's metadata
-- Parameters: $1: metadata (JSON), $2: object_id
UPDATE objects
SET metadata = $1
WHERE object_id = $2
