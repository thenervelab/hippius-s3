-- migrate:up

-- Add status column to objects table to track object processing state (if it doesn't exist)
ALTER TABLE objects ADD COLUMN IF NOT EXISTS status VARCHAR(50) DEFAULT 'publishing';

-- Create index for efficient status-based queries (if it doesn't exist)
CREATE INDEX IF NOT EXISTS idx_objects_status ON objects(status);

-- migrate:down

-- Remove the index and column
DROP INDEX IF EXISTS idx_objects_status;
ALTER TABLE objects DROP COLUMN IF EXISTS status;
