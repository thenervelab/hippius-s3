SELECT DISTINCT cb.backend
FROM chunk_backend cb
JOIN part_chunks pc ON pc.id = cb.chunk_id
JOIN parts p ON pc.part_id = p.part_id
WHERE p.object_id = $1
  AND ($2::bigint IS NULL OR p.object_version = $2)
  AND NOT cb.deleted
  AND cb.backend_identifier IS NOT NULL;
