-- Hard-delete a single soft-deleted object, re-verifying readiness ATOMICALLY
-- under the row lock taken by DELETE.
--
-- Why the re-check: find_objects_ready_for_hard_delete returns a batch, and the
-- janitor then deletes each row one at a time. Between the find and this delete
-- an object can be REVIVED — re-PUT does `ON CONFLICT DO UPDATE SET
-- deleted_at = NULL` on the SAME object_id and writes new, live chunk_backend
-- rows (deleted = false). A blind `DELETE FROM objects WHERE object_id = $1`
-- would then destroy the freshly-uploaded live object and cascade-delete its
-- live chunk metadata, orphaning chunks on the backend.
--
-- Re-asserting deleted_at + grace + "no live chunk_backend rows" here means the
-- delete only fires if the object STILL qualifies at delete time; a revived (or
-- otherwise no-longer-ready) object matches nothing and is left untouched.
-- Returns "DELETE 1" when removed, "DELETE 0" when skipped.
--
-- The deleted_at/grace/NOT-EXISTS conditions mirror find_objects_ready_for_hard_delete.sql
-- (the finder also has an EXISTS(>=1 chunk_backend) guard, not re-checked here because
-- chunk_backend rows never disappear between find and delete). Keep the two in sync.
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
  );
