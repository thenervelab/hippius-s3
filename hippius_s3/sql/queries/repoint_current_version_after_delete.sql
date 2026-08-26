-- Move objects.current_object_version off a just-deleted version onto the newest live version
-- BELOW it, atomically. Returns the new current, or nothing when no successor exists (the caller
-- then soft-deletes the whole object).
--
-- One statement rather than SELECT-then-CAS: the UPDATE takes the row lock and its target list is
-- recomputed under EvalPlanQual, so the successor is chosen against committed state rather than a
-- snapshot read moments earlier. The read-then-write shape could pick a version that a concurrent
-- versioned DELETE soft-deleted in between, leaving current_object_version pointing at a dead row
-- — invisible to reads (they fall back), but permanently un-unpinnable, because
-- get_chunk_backend_identifiers refuses to hand back the current version of a live object.
--
-- `object_version < $2` is load-bearing, not just AWS's "next newest below" semantic:
-- create_migration_version inserts rows ABOVE current_object_version without bumping it, so an
-- unbounded MAX() could promote an incomplete migration placeholder to current.
--
-- Parameters: $1: object_id (uuid), $2: deleted object_version (bigint)
UPDATE objects o
   SET current_object_version = (
       SELECT max(ov.object_version)
       FROM object_versions ov
       WHERE ov.object_id = o.object_id
         AND ov.deleted_at IS NULL
         AND ov.object_version < $2
   )
 WHERE o.object_id = $1
   AND o.current_object_version = $2
   AND EXISTS (
       SELECT 1
       FROM object_versions ov
       WHERE ov.object_id = o.object_id
         AND ov.deleted_at IS NULL
         AND ov.object_version < $2
   )
RETURNING current_object_version
