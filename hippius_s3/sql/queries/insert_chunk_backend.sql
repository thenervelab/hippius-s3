-- Parameters: $1 part_id (UUID), $2 chunk_index (INT), $3 backend (TEXT), $4 backend_identifier (TEXT)
INSERT INTO chunk_backend (chunk_id, backend, backend_identifier)
SELECT pc.id, $3, $4
FROM part_chunks pc
WHERE pc.part_id = $1 AND pc.chunk_index = $2
ON CONFLICT (chunk_id, backend) DO UPDATE
SET backend_identifier = COALESCE(EXCLUDED.backend_identifier, chunk_backend.backend_identifier)
RETURNING chunk_id;
