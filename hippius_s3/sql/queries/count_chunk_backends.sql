-- Parameters: $1 object_id, $2 object_version, $3 part_number, $4 required_backends TEXT[]
--
-- Returns:
--   total_chunks      — number of part_chunks rows for the part
--   replicated_chunks — how many of those have a live chunk_backend row for
--                       EVERY backend in $4 (i.e. fully replicated)
--   expected_chunks   — how many chunks the part SHOULD have, computed from
--                       parts.size_bytes / parts.chunk_size_bytes. Falls back
--                       to 4 MiB when chunk_size_bytes is NULL (legacy rows).
--
-- The janitor compares these three. If total < expected, the part is still
-- being materialised (do not delete). If total == replicated == expected,
-- the part is safe to evict. Anything else: keep.
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
    CEIL(p.size_bytes::float / GREATEST(COALESCE(p.chunk_size_bytes, 4194304), 1))::int AS expected_chunks
FROM parts p
LEFT JOIN part_chunks pc ON pc.part_id = p.part_id
LEFT JOIN backend_sets bs ON bs.chunk_id = pc.id
WHERE p.object_id = $1
  AND p.object_version = $2
  AND p.part_number = $3
GROUP BY p.part_id, p.size_bytes, p.chunk_size_bytes
