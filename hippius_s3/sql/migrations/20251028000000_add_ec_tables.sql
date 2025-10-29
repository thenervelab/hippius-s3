-- migrate:up

-- Create part_ec table for EC metadata per part and policy_version
CREATE TABLE IF NOT EXISTS part_ec (
    part_id UUID NOT NULL REFERENCES parts(part_id) ON DELETE CASCADE,
    policy_version INT NOT NULL,
    scheme TEXT NOT NULL,
    k INT NOT NULL CHECK (k > 0),
    m INT NOT NULL CHECK (m >= 0),
    shard_size_bytes BIGINT NOT NULL CHECK (shard_size_bytes > 0),
    stripes INT NOT NULL CHECK (stripes > 0),
    state TEXT NOT NULL DEFAULT 'pending_encode',
    manifest_cid TEXT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (part_id, policy_version),
    CONSTRAINT part_ec_state_check CHECK (state IN ('pending_encode', 'pending_upload', 'complete', 'failed'))
);

CREATE INDEX IF NOT EXISTS idx_part_ec_state ON part_ec(state);
CREATE INDEX IF NOT EXISTS idx_part_ec_created_at ON part_ec(created_at);

-- Create part_parity_chunks table for parity CIDs per stripe/index
CREATE TABLE IF NOT EXISTS part_parity_chunks (
    part_id UUID NOT NULL,
    policy_version INT NOT NULL,
    stripe_index INT NOT NULL CHECK (stripe_index >= 0),
    parity_index INT NOT NULL CHECK (parity_index >= 0),
    cid TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    FOREIGN KEY (part_id, policy_version) REFERENCES part_ec(part_id, policy_version) ON DELETE CASCADE,
    UNIQUE (part_id, policy_version, stripe_index, parity_index)
);

CREATE INDEX IF NOT EXISTS idx_part_parity_chunks_part_pv ON part_parity_chunks(part_id, policy_version);
CREATE INDEX IF NOT EXISTS idx_part_parity_chunks_cid ON part_parity_chunks(cid);

-- migrate:down

DROP INDEX IF EXISTS idx_part_parity_chunks_cid;
DROP INDEX IF EXISTS idx_part_parity_chunks_part_pv;
DROP TABLE IF EXISTS part_parity_chunks;
DROP INDEX IF EXISTS idx_part_ec_created_at;
DROP INDEX IF EXISTS idx_part_ec_state;
DROP TABLE IF EXISTS part_ec;
