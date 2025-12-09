-- Normalize part_chunks to use cid_id FK instead of TEXT cid
-- This migration consolidates CID storage to the cids table for deduplication
--
-- migrate:up

-- Step 1: Add cid_id column (nullable initially for backfill)
ALTER TABLE part_chunks ADD COLUMN cid_id UUID;

-- Step 2: Add foreign key constraint to cids table
ALTER TABLE part_chunks ADD CONSTRAINT fk_part_chunks_cid
    FOREIGN KEY (cid_id) REFERENCES cids(id) ON DELETE SET NULL;

-- Step 3: Backfill cid_id from TEXT cid
-- For each unique CID in part_chunks, upsert to cids table and get ID
WITH cid_upserts AS (
    INSERT INTO cids (cid)
    SELECT DISTINCT cid FROM part_chunks WHERE cid IS NOT NULL AND cid != ''
    ON CONFLICT (cid) DO UPDATE SET cid = EXCLUDED.cid
    RETURNING id, cid
)
UPDATE part_chunks pc
SET cid_id = cu.id
FROM cid_upserts cu
WHERE pc.cid = cu.cid AND pc.cid IS NOT NULL;

-- Step 4: Create index on cid_id for JOIN performance
CREATE INDEX idx_part_chunks_cid_id ON part_chunks(cid_id);

-- Step 5: Drop TEXT cid column (commented out for safety - run after validation)
-- Uncomment after verifying all data is migrated and queries are updated
-- ALTER TABLE part_chunks DROP COLUMN cid;

-- Step 6: Make cid_id NOT NULL (commented out - run after cid column dropped)
-- Uncomment after TEXT cid column is dropped
-- ALTER TABLE part_chunks ALTER COLUMN cid_id SET NOT NULL;

-- migrate:down

-- Rollback steps (reverse order)
-- Note: Data loss will occur if TEXT cid column was dropped

-- Restore TEXT cid column if it was dropped
-- ALTER TABLE part_chunks ADD COLUMN cid TEXT;

-- Backfill TEXT cid from cids table (if column was dropped)
-- UPDATE part_chunks pc
-- SET cid = c.cid
-- FROM cids c
-- WHERE pc.cid_id = c.id;

-- Drop foreign key constraint
ALTER TABLE part_chunks DROP CONSTRAINT IF EXISTS fk_part_chunks_cid;

-- Drop index
DROP INDEX IF EXISTS idx_part_chunks_cid_id;

-- Drop cid_id column
ALTER TABLE part_chunks DROP COLUMN IF EXISTS cid_id;
