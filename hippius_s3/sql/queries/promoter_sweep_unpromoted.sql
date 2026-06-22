-- Backstop for a missed `cephor_replicated` NOTIFY (s3-2.1 PR-7): replicated parts
-- with at least one chunk that has NO chunk_backend row on any backend — i.e. the
-- backend upload never ran. Bounded and self-clearing: once a part is promoted and
-- uploaded, every chunk gains a chunk_backend row and the part drops out of this set.
-- A part already (partially) on a backend is left to that backend worker's own retry.
SELECT DISTINCT crs.object_id, crs.version, crs.part_number
FROM cephor_replication_status crs
JOIN parts p
  ON p.object_id = crs.object_id::uuid
 AND p.object_version = crs.version
 AND p.part_number = crs.part_number
JOIN part_chunks pc ON pc.part_id = p.part_id
LEFT JOIN chunk_backend cb ON cb.chunk_id = pc.id
WHERE crs.status = 'replicated' AND cb.chunk_id IS NULL
LIMIT $1;
