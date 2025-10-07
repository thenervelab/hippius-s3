-- migrate:up

-- Add storage_version to distinguish legacy (v1) vs modern (v2) storage layout
ALTER TABLE objects
    ADD COLUMN IF NOT EXISTS storage_version SMALLINT NOT NULL DEFAULT 2;

-- Backfill storage_version based on chunk layout:
-- v2 if any part has >1 chunk; else v1
WITH part_chunk_counts AS (
  SELECT p.object_id,
         MAX(COALESCE(pc.cnt, 0)) AS max_chunks_per_part
  FROM parts p
  LEFT JOIN (
    SELECT part_id, COUNT(*) AS cnt
    FROM part_chunks
    GROUP BY part_id
  ) pc ON pc.part_id = p.part_id
  GROUP BY p.object_id
)
UPDATE objects o
SET storage_version = CASE
  WHEN pcc.max_chunks_per_part > 1 THEN 2
  ELSE 1
END
FROM part_chunk_counts pcc
WHERE o.object_id = pcc.object_id;

CREATE INDEX IF NOT EXISTS idx_objects_storage_version ON objects(storage_version);

-- migrate:down

DROP INDEX IF EXISTS idx_objects_storage_version;
ALTER TABLE objects DROP COLUMN IF EXISTS storage_version;
