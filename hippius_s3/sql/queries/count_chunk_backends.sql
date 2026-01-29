-- Parameters: $1 object_id, $2 object_version, $3 part_number, $4 expected_backends TEXT[]
WITH relevant_chunks AS (
    SELECT pc.id
    FROM parts p
    JOIN part_chunks pc ON pc.part_id = p.part_id
    WHERE p.object_id = $1 AND p.object_version = $2 AND p.part_number = $3
),
backend_sets AS (
    SELECT cb.chunk_id, array_agg(cb.backend) AS backends
    FROM chunk_backend cb
    JOIN relevant_chunks rc ON rc.id = cb.chunk_id
    WHERE NOT cb.deleted
    GROUP BY cb.chunk_id
)
SELECT
    COUNT(pc.id) AS total_chunks,
    COUNT(pc.id) FILTER (WHERE COALESCE(bs.backends, ARRAY[]::text[]) @> $4) AS replicated_chunks,
    CEIL(p.size_bytes::float / 4194304)::int AS expected_chunks
FROM parts p
LEFT JOIN part_chunks pc ON pc.part_id = p.part_id
LEFT JOIN backend_sets bs ON bs.chunk_id = pc.id
WHERE p.object_id = $1
  AND p.object_version = $2
  AND p.part_number = $3
GROUP BY p.part_id, p.size_bytes
