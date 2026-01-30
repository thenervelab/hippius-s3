-- migrate:up

CREATE TABLE public.chunk_backend (
    chunk_id   BIGINT NOT NULL REFERENCES part_chunks(id) ON DELETE CASCADE,
    backend    TEXT   NOT NULL,
    backend_identifier TEXT,
    deleted    BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at TIMESTAMPTZ,
    PRIMARY KEY (chunk_id, backend)
);

-- Supports "count backends for chunk_id" queries (janitor replication check)
CREATE INDEX idx_chunk_backend_active_by_chunk
  ON chunk_backend(chunk_id)
  WHERE NOT deleted;

-- Supports "fetch rows by backend" queries (cleanup/reporting)
CREATE INDEX idx_chunk_backend_active_by_backend
  ON chunk_backend(backend)
  WHERE NOT deleted;

-- Backfill IPFS rows (chunks that have a CID)
INSERT INTO chunk_backend (chunk_id, backend, backend_identifier, deleted, created_at)
SELECT id, 'ipfs', cid, false, created_at
FROM part_chunks
WHERE cid IS NOT NULL;

-- Drop the now-redundant counter column
DROP INDEX IF EXISTS idx_part_chunks_backend_count;
ALTER TABLE part_chunks DROP COLUMN storage_backends_uploaded;

-- migrate:down

ALTER TABLE part_chunks
ADD COLUMN storage_backends_uploaded SMALLINT NOT NULL DEFAULT 0
CHECK (storage_backends_uploaded >= 0);

UPDATE part_chunks pc
SET storage_backends_uploaded = (
    SELECT COUNT(*) FROM chunk_backend cb
    WHERE cb.chunk_id = pc.id AND NOT cb.deleted
);

CREATE INDEX idx_part_chunks_backend_count ON part_chunks(storage_backends_uploaded);

DROP INDEX IF EXISTS idx_chunk_backend_active_by_chunk;
DROP INDEX IF EXISTS idx_chunk_backend_active_by_backend;
DROP TABLE IF EXISTS chunk_backend;
