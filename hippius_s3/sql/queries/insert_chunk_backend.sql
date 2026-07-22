-- Parameters: $1 part_id (UUID), $2 chunk_index (INT), $3 backend (TEXT), $4 backend_identifier (TEXT)
-- B2: on a re-upload/re-pin of a chunk, REVIVE the row (clear the soft-delete) and take the fresh
-- identifier. Previously the conflict update only touched backend_identifier and left
-- deleted=true, so a chunk re-uploaded after an unpin stayed soft-deleted → invisible to reads
-- (get_chunk_backend_identifier filters `NOT deleted`) and un-unpinnable (leak). The primary key
-- is (chunk_id, backend), so the conflict fires regardless of the deleted flag. Content-addressed
-- backends return the same identifier for the same bytes, so taking EXCLUDED is the freshly-pinned
-- location; a still-live row re-pinned with the SAME identifier is unchanged.
INSERT INTO chunk_backend (chunk_id, backend, backend_identifier, deleted, deleted_at)
SELECT pc.id, $3, $4, false, NULL
FROM part_chunks pc
WHERE pc.part_id = $1 AND pc.chunk_index = $2
ON CONFLICT (chunk_id, backend) DO UPDATE
SET backend_identifier = COALESCE(EXCLUDED.backend_identifier, chunk_backend.backend_identifier),
    deleted = false,
    deleted_at = NULL
RETURNING chunk_id;
