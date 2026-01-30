-- Find soft-deleted objects where all chunk_backend rows are also soft-deleted
SELECT o.object_id
FROM objects o
WHERE o.deleted_at IS NOT NULL
  AND o.deleted_at < now() - INTERVAL '1 hour'  -- grace period
  AND NOT EXISTS (
      SELECT 1
      FROM parts p
      JOIN part_chunks pc ON pc.part_id = p.part_id
      JOIN chunk_backend cb ON cb.chunk_id = pc.id
      WHERE p.object_id = o.object_id
        AND NOT cb.deleted
  );
