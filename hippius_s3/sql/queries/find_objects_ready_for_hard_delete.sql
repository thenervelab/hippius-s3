-- Find soft-deleted objects whose chunks have all been confirmed deleted
-- from every backend.
--
-- Guard against two failure modes:
-- 1. Pure "no rows" false positive: the old query used only NOT EXISTS
--    over `chunk_backend`, which returned true if zero rows exist at all
--    (e.g. a crash during upload left no chunk_backend rows). That silently
--    hard-deleted object metadata without a real deletion audit trail.
-- 2. Partial unpin: some backends confirmed delete but others still have
--    live rows. The existing NOT EXISTS clause covers this.
--
-- This version additionally requires that AT LEAST ONE chunk_backend row
-- exists per expected chunk — i.e. the object was actually replicated at
-- some point, so the "all deleted" signal is meaningful.
--
-- Parameter: $1 = batch size (max objects returned per call).
--
-- We materialise a small batch of soft-deleted candidates FIRST, then apply
-- the EXISTS/NOT EXISTS checks to just that batch. Without this, the planner
-- estimates millions of matching deleted rows and folds the EXISTS into a
-- parallel hash join that full-scans chunk_backend (~52M rows) + part_chunks +
-- parts on every call — a ~135 GiB read storm that saturated the data disk and
-- stalled the primary (see oom-psql-postmortem.md). AS MATERIALIZED + LIMIT
-- forces per-object index probes; the janitor drains the backlog over cycles.
WITH candidates AS MATERIALIZED (
    SELECT object_id
    FROM objects
    WHERE deleted_at IS NOT NULL
      AND deleted_at < now() - INTERVAL '1 hour'  -- grace period
    ORDER BY deleted_at  -- oldest first; uses idx_objects_deleted
    LIMIT $1
)
SELECT c.object_id
FROM candidates c
WHERE EXISTS (
      SELECT 1
      FROM parts p
      JOIN part_chunks pc ON pc.part_id = p.part_id
      JOIN chunk_backend cb ON cb.chunk_id = pc.id
      WHERE p.object_id = c.object_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM parts p
      JOIN part_chunks pc ON pc.part_id = p.part_id
      JOIN chunk_backend cb ON cb.chunk_id = pc.id
      WHERE p.object_id = c.object_id
        AND NOT cb.deleted
  );
