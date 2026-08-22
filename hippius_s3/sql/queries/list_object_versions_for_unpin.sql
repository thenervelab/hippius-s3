-- Every version of an object that still holds a live backend copy.
--
-- The delete path enqueues one UnpinChainRequest per returned version. Deliberately NOT a single
-- request with a NULL object_version ("all versions, resolved later"): a re-PUT between the soft
-- delete and the unpin revives the object and changes what NULL would resolve to. Resolving the
-- list at delete time is race-free, and get_chunk_backend_identifiers' own guard still refuses to
-- hand back the current version of a live object.
--
-- Index-driven per object (idx_parts_object_version), so it is bounded by the object's own rows.
-- Parameters: $1: object_id (uuid)
SELECT DISTINCT p.object_version
FROM parts p
JOIN part_chunks pc ON pc.part_id = p.part_id
JOIN chunk_backend cb ON cb.chunk_id = pc.id
WHERE p.object_id = $1
  AND NOT cb.deleted
  AND cb.backend_identifier IS NOT NULL
ORDER BY 1
