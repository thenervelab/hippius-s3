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
SELECT o.object_id
FROM objects o
WHERE o.deleted_at IS NOT NULL
  AND o.deleted_at < now() - INTERVAL '1 hour'  -- grace period
  AND EXISTS (
      SELECT 1
      FROM parts p
      JOIN part_chunks pc ON pc.part_id = p.part_id
      JOIN chunk_backend cb ON cb.chunk_id = pc.id
      WHERE p.object_id = o.object_id
  )
  AND NOT EXISTS (
      SELECT 1
      FROM parts p
      JOIN part_chunks pc ON pc.part_id = p.part_id
      JOIN chunk_backend cb ON cb.chunk_id = pc.id
      WHERE p.object_id = o.object_id
        AND NOT cb.deleted
  );
