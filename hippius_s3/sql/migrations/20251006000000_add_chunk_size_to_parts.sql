-- migrate:up

ALTER TABLE parts ADD COLUMN IF NOT EXISTS chunk_size_bytes INTEGER;

-- Backfill chunk_size_bytes for existing parts
-- For parts with exactly 1 chunk, set chunk_size_bytes = size_bytes (migrated single-chunk parts)
UPDATE parts p
SET chunk_size_bytes = p.size_bytes
WHERE chunk_size_bytes IS NULL
  AND EXISTS (
    SELECT 1 FROM part_chunks pc
    WHERE pc.part_id = p.part_id
    GROUP BY pc.part_id
    HAVING COUNT(*) = 1
  );

-- For parts with multiple chunks, calculate chunk_size from plaintext size and chunk count
-- We use GREATEST to ensure we get at least 1 to avoid division issues
UPDATE parts p
SET chunk_size_bytes = CEIL(p.size_bytes::numeric / GREATEST(
  (SELECT COUNT(*) FROM part_chunks WHERE part_id = p.part_id),
  1
))::integer
WHERE chunk_size_bytes IS NULL
  AND EXISTS (
    SELECT 1 FROM part_chunks pc
    WHERE pc.part_id = p.part_id
    GROUP BY pc.part_id
    HAVING COUNT(*) > 1
  );

-- For any remaining parts without part_chunks (edge case), leave NULL
-- The reader will fall back to config value

-- migrate:down

ALTER TABLE parts DROP COLUMN chunk_size_bytes;
