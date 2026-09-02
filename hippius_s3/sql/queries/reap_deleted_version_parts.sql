-- Reclaim the storage rows of ONE soft-deleted object version, re-verifying readiness ATOMICALLY
-- under the row lock taken by DELETE.
--
-- Why the re-check: find_versions_ready_for_reap returns a slice, and the janitor then reaps each
-- ready row one at a time. Between the find and this delete, a lagging upload can land and insert
-- live chunk_backend rows. A blind DELETE would then cascade those away
-- (parts -> part_chunks -> chunk_backend are all ON DELETE CASCADE), orphaning bytes on Arion with
-- no DB record. The WHERE clause MIRRORS the finder's `ready` boolean exactly.
-- Keep in sync with find_versions_ready_for_reap.sql.
--
-- Why only `parts` and not the object_versions row: version numbers must stay monotonic. A DELETE
-- of the current version repoints current_object_version DOWN, and upsert_object_basic allocates
-- the next version as GREATEST(current, MAX(object_version)) + 1 — so removing the row would let
-- MAX drop and RE-MINT a version number that already existed. That is precisely the hazard
-- get_chunk_backend_identifiers warns about ("any future change that reuses a version number ...
-- would delete live data from the backend"), and it would also collide with stale FS cache under
-- the old `v<version>/` path. Leaving the row as a tombstone (deleted_at set, invisible to every
-- read) keeps MAX correct at negligible cost; `parts`/`part_chunks`/`chunk_backend` are the bulky
-- tables and they are what this reclaims. The tombstone is finally removed when the whole object
-- is hard-deleted, which cascades object_versions.
--
-- Returns "DELETE n" with n > 0 when rows were reclaimed, "DELETE 0" when skipped.
--
-- Parameters: $1: object_id (uuid), $2: object_version (bigint)
DELETE FROM parts p
WHERE p.object_id = $1
  AND p.object_version = $2
  AND EXISTS (
      SELECT 1
      FROM object_versions ov
      WHERE ov.object_id = $1
        AND ov.object_version = $2
        AND ov.deleted_at IS NOT NULL
        AND ov.deleted_at < now() - INTERVAL '1 hour'
  )
  -- OBJECT LOCK (Tier 2): re-checked here, not just in the finder, for the same reason the
  -- readiness conditions are — the reap must be safe against anything that changed between the
  -- find and this DELETE (a legal hold placed on the version mid-batch, or a caller that never
  -- consulted the finder at all). Mirrors object_lock_enforcement.LOCKED_VERSION_SQL_PREDICATE.
  AND NOT EXISTS (
      SELECT 1
      FROM object_versions ov
      WHERE ov.object_id = $1
        AND ov.object_version = $2
        AND (ov.object_lock_legal_hold
             OR (ov.object_lock_retain_until IS NOT NULL AND ov.object_lock_retain_until > now()))
  )
  AND NOT EXISTS (
      SELECT 1
      FROM parts p2
      JOIN part_chunks pc ON pc.part_id = p2.part_id
      JOIN chunk_backend cb ON cb.chunk_id = pc.id
      WHERE p2.object_id = $1
        AND p2.object_version = $2
        AND NOT cb.deleted
  )
  AND (
      EXISTS (
          SELECT 1
          FROM parts p2
          JOIN part_chunks pc ON pc.part_id = p2.part_id
          JOIN chunk_backend cb ON cb.chunk_id = pc.id
          WHERE p2.object_id = $1
            AND p2.object_version = $2
      )
      OR EXISTS (
          SELECT 1
          FROM object_versions ov
          WHERE ov.object_id = $1
            AND ov.object_version = $2
            AND ov.deleted_at < now() - INTERVAL '24 hours'
      )
  )
