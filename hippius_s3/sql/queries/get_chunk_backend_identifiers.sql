-- $1 backend (TEXT), $2 object_id (UUID), $3 object_version (BIGINT, nullable)
--
-- DEFENSIVE GUARD (last line): never hand the unpinner the CURRENT version of a LIVE object.
--
-- The unpinner deletes whatever this returns from the backend; until now nothing here
-- re-checked that the object was still deleted, so safety rested ENTIRELY on the caller
-- passing the right object_version. That holds today only because a re-PUT of a deleted
-- object revives the SAME row and BUMPS the version (upsert_object_basic:
-- `deleted_at = NULL, current_object_version = GREATEST(...) + 1`), so a racing revive
-- lands on version N+1 while an in-flight unpin still targets the dead version N. That is
-- a single point of failure: any future change that reuses a version number, or any caller
-- that passes a stale/NULL version, would delete live data from the backend.
--
-- The guard is deliberately NOT a blunt `o.deleted_at IS NOT NULL`: superseded versions of
-- LIVE objects are legitimately unpinned (overwrite retention, cleanup_migration_versions.py,
-- the unpin DLQ requeue path), and that check would silently block them. The precise
-- invariant is "the version being unpinned must not be the live current one".
--
-- Fail direction is safe-by-construction: if the guard excludes rows the unpinner simply
-- deletes nothing (the chunk_backend rows stay live and the object stays un-hard-deletable),
-- which is a visible stall rather than data loss.
SELECT cb.backend_identifier, cb.chunk_id
FROM chunk_backend cb
JOIN part_chunks pc ON pc.id = cb.chunk_id
JOIN parts p ON pc.part_id = p.part_id
JOIN objects o ON o.object_id = p.object_id
WHERE cb.backend = $1
  AND p.object_id = $2
  AND ($3::bigint IS NULL OR p.object_version = $3)
  AND NOT cb.deleted
  AND cb.backend_identifier IS NOT NULL
  AND (o.deleted_at IS NOT NULL OR p.object_version <> o.current_object_version)
ORDER BY cb.chunk_id;
