-- Atomically increment storage backend counter for a chunk
-- Parameters: $1 part_id (UUID), $2 chunk_index (INT)
-- Returns: new storage_backends_uploaded value
UPDATE part_chunks
SET storage_backends_uploaded = storage_backends_uploaded + 1
WHERE part_id = $1 AND chunk_index = $2
RETURNING storage_backends_uploaded;
