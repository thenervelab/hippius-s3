-- Roll a version's Arion hash up from chunk_backend onto object_versions.arion_hash.
-- Only a version stored as exactly one chunk has a single Arion hash; anything else is left alone.
-- Runs after every successful upload request, so it counts chunks across ALL parts of the version:
-- for a multipart upload an early part finishing must not stamp its chunk as the object's hash.
-- $1 object_id (UUID), $2 object_version (BIGINT), $3 backend (TEXT)
UPDATE object_versions ov
SET arion_hash = s.arion_hash
FROM (
    SELECT count(*) AS n_chunks, max(cb.arion_hash) AS arion_hash
    FROM parts p
    JOIN part_chunks pc ON pc.part_id = p.part_id
    LEFT JOIN chunk_backend cb ON cb.chunk_id = pc.id AND cb.backend = $3 AND NOT cb.deleted
    WHERE p.object_id = $1
      AND p.object_version = $2
) s
WHERE ov.object_id = $1
  AND ov.object_version = $2
  AND s.n_chunks = 1
  AND s.arion_hash IS NOT NULL
  AND ov.arion_hash IS DISTINCT FROM s.arion_hash
