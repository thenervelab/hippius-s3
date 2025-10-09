-- migrate:up

-- Add storage_version to distinguish legacy (v1) vs modern (v2) storage layout
ALTER TABLE objects
    ADD COLUMN IF NOT EXISTS storage_version SMALLINT NOT NULL DEFAULT 2;

-- Backfill storage_version based on chunk layout:
-- v2 if any part has >1 chunk; else v1
WITH max_chunks_per_object AS (
  SELECT o.object_id,
         COALESCE(MAX(pc.cnt), 0) AS max_chunks_per_part
  FROM objects o
  LEFT JOIN parts p ON p.object_id = o.object_id
  LEFT JOIN (
    SELECT part_id, COUNT(*) AS cnt
    FROM part_chunks
    GROUP BY part_id
  ) pc ON pc.part_id = p.part_id
  GROUP BY o.object_id
)
UPDATE objects o
SET storage_version = CASE
  WHEN mco.max_chunks_per_part > 1 THEN 2
  ELSE 1
END
FROM max_chunks_per_object mco
WHERE o.object_id = mco.object_id;

CREATE INDEX IF NOT EXISTS idx_objects_storage_version ON objects(storage_version);

-- migrate:down

DROP INDEX IF EXISTS idx_objects_storage_version;
ALTER TABLE objects DROP COLUMN IF EXISTS storage_version;
