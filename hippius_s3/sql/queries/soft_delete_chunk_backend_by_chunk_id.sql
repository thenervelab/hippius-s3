-- $1 backend (TEXT), $2 chunk_id (BIGINT)
UPDATE chunk_backend
SET deleted = true, deleted_at = now()
WHERE backend = $1 AND chunk_id = $2 AND NOT deleted
RETURNING chunk_id;
