-- Parameters: $1 backend (TEXT), $2 backend_identifier (TEXT)
UPDATE chunk_backend
SET deleted = true, deleted_at = now()
WHERE backend = $1 AND backend_identifier = $2 AND NOT deleted
RETURNING chunk_id;
