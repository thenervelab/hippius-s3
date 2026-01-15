-- Add storage backend upload tracking to part_chunks
-- This enables graceful cleanup based on replication status

-- migrate:up

ALTER TABLE part_chunks
ADD COLUMN storage_backends_uploaded SMALLINT NOT NULL DEFAULT 0
CHECK (storage_backends_uploaded >= 0);

CREATE INDEX idx_part_chunks_backend_count ON part_chunks(storage_backends_uploaded);
CREATE INDEX idx_part_chunks_created_at ON part_chunks(created_at);

COMMENT ON COLUMN part_chunks.storage_backends_uploaded IS
'Counter incremented after successful upload to each storage backend. 0=pending, 1=primary backend, 2+=replicated.';

-- migrate:down

DROP INDEX IF EXISTS idx_part_chunks_created_at;
DROP INDEX IF EXISTS idx_part_chunks_backend_count;
ALTER TABLE part_chunks DROP COLUMN storage_backends_uploaded;
