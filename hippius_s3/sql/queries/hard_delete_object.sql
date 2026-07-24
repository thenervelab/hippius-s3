-- Hard-delete a single soft-deleted object, re-verifying readiness ATOMICALLY
-- under the row lock taken by DELETE.
--
-- Why the re-check: find_objects_ready_for_hard_delete returns a slice, and the
-- janitor then deletes each ready row one at a time. Between the find and this
-- delete an object can be REVIVED — re-PUT does `ON CONFLICT DO UPDATE SET
-- deleted_at = NULL` on the SAME object_id and writes new, live chunk_backend
-- rows (deleted = false). A blind `DELETE FROM objects WHERE object_id = $1`
-- would then destroy the freshly-uploaded live object and cascade-delete its
-- live chunk metadata, orphaning chunks on the backend.
--
-- The WHERE clause MIRRORS the finder's `ready` boolean exactly (per-row
-- readiness + the aged zero-row relaxation) so the guarded delete agrees with
-- the find: every row the finder marked ready still deletes here, and a revived
-- (or otherwise no-longer-ready) row matches nothing and is left untouched.
-- Returns "DELETE 1" when removed, "DELETE 0" when skipped.
--
-- ready = past-1h-grace
--         AND NOT (any live chunk_backend row)                  -- revival / partial unpin
--         AND ( EXISTS(any chunk_backend row)                   -- real, audited "all deleted"
--               OR deleted_at < now() - INTERVAL '24 hours' )   -- aged never-replicated relaxation
-- Keep in sync with find_objects_ready_for_hard_delete.sql.
DELETE FROM objects o
WHERE o.object_id = $1
  AND o.deleted_at IS NOT NULL
  AND o.deleted_at < now() - INTERVAL '1 hour'
  AND NOT EXISTS (
      SELECT 1
      FROM parts p
      JOIN part_chunks pc ON pc.part_id = p.part_id
      JOIN chunk_backend cb ON cb.chunk_id = pc.id
      WHERE p.object_id = o.object_id
        AND NOT cb.deleted
  )
  AND (
      EXISTS (
          SELECT 1
          FROM parts p
          JOIN part_chunks pc ON pc.part_id = p.part_id
          JOIN chunk_backend cb ON cb.chunk_id = pc.id
          WHERE p.object_id = o.object_id
      )
      OR o.deleted_at < now() - INTERVAL '24 hours'
  );
