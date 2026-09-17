-- Roll a version's Arion hash up from chunk_backend onto object_versions.arion_hash.
-- Only a version stored as exactly one chunk has a single Arion hash; anything else is NULL.
-- Runs after every successful upload request, so it counts chunks across ALL parts of the version.
-- It must also CLEAR: parts land in any order, and a rollup that ran when only one part row
-- existed (a small last part of a multipart upload replicating first, or an object before its
-- first S4 append) has stamped a hash that stops being the object's once the next part arrives.
-- $1 object_id (UUID), $2 object_version (BIGINT), $3 backend (TEXT)
UPDATE object_versions ov
SET arion_hash = s.rolled_up
FROM (
    SELECT CASE WHEN count(*) = 1 THEN max(cb.arion_hash) END AS rolled_up
    FROM parts p
    JOIN part_chunks pc ON pc.part_id = p.part_id
    LEFT JOIN chunk_backend cb ON cb.chunk_id = pc.id AND cb.backend = $3 AND NOT cb.deleted
    WHERE p.object_id = $1
      AND p.object_version = $2
) s
WHERE ov.object_id = $1
  AND ov.object_version = $2
  AND ov.arion_hash IS DISTINCT FROM s.rolled_up
