-- $1 backend (TEXT), $2 object_id (UUID), $3 object_version (BIGINT), $4 part_number (INT), $5 chunk_index (INT)
SELECT cb.backend_identifier
FROM chunk_backend cb
JOIN part_chunks pc ON pc.id = cb.chunk_id
JOIN parts p ON pc.part_id = p.part_id
WHERE cb.backend = $1
  AND p.object_id = $2
  AND p.object_version = $3
  AND p.part_number = $4
  AND pc.chunk_index = $5
  AND NOT cb.deleted
  AND cb.backend_identifier IS NOT NULL
LIMIT 1;
