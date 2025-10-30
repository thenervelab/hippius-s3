-- migrate:up

-- Ensure pgcrypto is available for gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS pgcrypto;

-- blobs: unified CID work ledger
CREATE TABLE IF NOT EXISTS blobs (
  id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
  kind             TEXT        NOT NULL CHECK (kind IN ('data','replica','parity','manifest')),
  object_id        UUID        NOT NULL,
  object_version   INT         NOT NULL CHECK (object_version > 0),
  part_id          UUID        NULL REFERENCES parts(part_id) ON DELETE CASCADE,
  policy_version   INT         NULL CHECK (policy_version IS NULL OR policy_version >= 0),
  chunk_index      INT         NULL CHECK (chunk_index IS NULL OR chunk_index >= 0),
  stripe_index     INT         NULL CHECK (stripe_index IS NULL OR stripe_index >= 0),
  parity_index     INT         NULL CHECK (parity_index IS NULL OR parity_index >= 0),
  fs_path          TEXT        NOT NULL,
  size_bytes       BIGINT      NOT NULL CHECK (size_bytes >= 0),
  cid              TEXT        NULL,
  status           TEXT        NOT NULL CHECK (status IN ('staged','uploading','uploaded','pinning','pinned','failed')) DEFAULT 'staged',
  last_error       TEXT        NULL,
  last_error_at    TIMESTAMPTZ NULL,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Uniqueness by kind
CREATE UNIQUE INDEX IF NOT EXISTS blobs_parity_uq
ON blobs (part_id, policy_version, kind, stripe_index, parity_index)
WHERE kind = 'parity';

CREATE UNIQUE INDEX IF NOT EXISTS blobs_replica_uq
ON blobs (part_id, kind, chunk_index, parity_index)
WHERE kind = 'replica';

CREATE UNIQUE INDEX IF NOT EXISTS blobs_data_uq
ON blobs (part_id, kind, chunk_index)
WHERE kind = 'data';

CREATE UNIQUE INDEX IF NOT EXISTS blobs_manifest_uq
ON blobs (object_id, object_version, kind)
WHERE kind = 'manifest';

-- Lookups
CREATE INDEX IF NOT EXISTS blobs_status_idx ON blobs (status);
CREATE INDEX IF NOT EXISTS blobs_part_kind_idx ON blobs (part_id, policy_version, kind);
CREATE INDEX IF NOT EXISTS blobs_object_idx ON blobs (object_id, object_version);
CREATE INDEX IF NOT EXISTS blobs_cid_idx ON blobs (cid);

-- Touch updated_at
CREATE OR REPLACE FUNCTION touch_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN NEW.updated_at = now(); RETURN NEW; END $$;

DROP TRIGGER IF EXISTS trg_touch_blobs ON blobs;
CREATE TRIGGER trg_touch_blobs
BEFORE UPDATE ON blobs
FOR EACH ROW EXECUTE FUNCTION touch_updated_at();

-- migrate:down
DROP TRIGGER IF EXISTS trg_touch_blobs ON blobs;
DROP FUNCTION IF EXISTS touch_updated_at();
DROP INDEX IF EXISTS blobs_cid_idx;
DROP INDEX IF EXISTS blobs_object_idx;
DROP INDEX IF EXISTS blobs_part_kind_idx;
DROP INDEX IF EXISTS blobs_status_idx;
DROP INDEX IF EXISTS blobs_manifest_uq;
DROP INDEX IF EXISTS blobs_data_uq;
DROP INDEX IF EXISTS blobs_replica_uq;
DROP INDEX IF EXISTS blobs_parity_uq;
DROP TABLE IF EXISTS blobs;
