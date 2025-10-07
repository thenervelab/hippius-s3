-- Create part_chunks table to store one CID per ciphertext chunk
-- Forward-compatible with EC where data shards align to ciphertext chunks

-- migrate:up

CREATE TABLE IF NOT EXISTS part_chunks (
  id BIGSERIAL PRIMARY KEY,
  part_id UUID NOT NULL REFERENCES parts(part_id) ON DELETE CASCADE,
  chunk_index INT NOT NULL CHECK (chunk_index >= 0),
  cid TEXT,
  cipher_size_bytes BIGINT NOT NULL CHECK (cipher_size_bytes >= 0),
  plain_size_bytes BIGINT CHECK (plain_size_bytes >= 0),
  checksum BYTEA,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(part_id, chunk_index)
);

CREATE INDEX IF NOT EXISTS part_chunks_part_idx ON part_chunks(part_id);

-- Backfill existing part-level CIDs into chunk index 0
-- We cannot know exact ciphertext boundaries historically; store as a single chunk per part
-- Note: cipher_size_bytes here is parts.size_bytes which may be plaintext (public) or ciphertext (private) depending on bucket
INSERT INTO part_chunks (part_id, chunk_index, cid, cipher_size_bytes)
SELECT p.part_id, 0 AS chunk_index, COALESCE(c.cid, p.ipfs_cid) AS cid, p.size_bytes
FROM parts p
LEFT JOIN cids c ON p.cid_id = c.id
WHERE (p.ipfs_cid IS NOT NULL AND p.ipfs_cid <> '') OR p.cid_id IS NOT NULL
ON CONFLICT (part_id, chunk_index) DO NOTHING;

-- migrate:down

DROP INDEX IF EXISTS part_chunks_part_idx;
DROP TABLE IF EXISTS part_chunks;
