-- Per-node heartbeat state, written by each agent and read by the allocator.
-- One row per ingest node; the agent upserts on its own node_id.
CREATE TABLE cephor_node_state (
    node_id         TEXT        PRIMARY KEY,
    -- Disk pressure in basis points (0..=10000); mirrors core::DiskPressure.
    pressure_bps    INTEGER     NOT NULL CHECK (pressure_bps BETWEEN 0 AND 10000),
    backlog_bytes   BIGINT      NOT NULL CHECK (backlog_bytes >= 0),
    max_drain_rate  BIGINT      NOT NULL CHECK (max_drain_rate >= 0),
    observed_p99_ns BIGINT      NOT NULL CHECK (observed_p99_ns >= 0),
    error_bps       INTEGER     NOT NULL CHECK (error_bps BETWEEN 0 AND 10000),
    -- Server-side timestamp so freshness is judged against one clock, not the
    -- agents' (which may skew); the allocator filters stale rows on read.
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
