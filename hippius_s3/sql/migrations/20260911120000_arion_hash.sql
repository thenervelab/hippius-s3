-- migrate:up

-- The hash an object is registered under on Arion, which is what the validator, the indexer and
-- the explorer look files up by.
--
-- Neither existing column is that hash:
--   * chunk_backend.backend_identifier is the file_id HCFS /upload returns: the HCFS path hash,
--     used for our own /download and /delete calls. Arion has never heard of it.
--   * object_versions.body_blake3 is the BLAKE3 of the PLAINTEXT. Arion only ever sees ciphertext.
-- HCFS returns the real one on /upload (arion_hash, and upload_id on older servers), and it is the
-- BLAKE3 of the ciphertext bytes we POSTed.
--
-- chunk_backend.arion_hash is per uploaded chunk. object_versions.arion_hash is the rollup for
-- versions stored as exactly one chunk; a multi-chunk version has no single Arion hash and stays
-- NULL. It lives on object_versions, like body_blake3, so listings project it without joining the
-- chunk tables per row.
--
-- No index, for the same reason as body_blake3: nothing looks objects up by it. ADD COLUMN with no
-- default is a catalog-only change.
ALTER TABLE chunk_backend ADD COLUMN IF NOT EXISTS arion_hash text;
ALTER TABLE object_versions ADD COLUMN IF NOT EXISTS arion_hash text;

-- migrate:down

ALTER TABLE object_versions DROP COLUMN IF EXISTS arion_hash;
ALTER TABLE chunk_backend DROP COLUMN IF EXISTS arion_hash;
