-- Per-node write-budget allotment, written by the leader allocator each tick
-- and pulled by each agent. fencing_epoch carries the leader's lease epoch so a
-- stale (deposed) leader's writes can be detected and ignored (enforced in M7).
CREATE TABLE cephor_allocation (
    node_id       TEXT        PRIMARY KEY,
    budget_bytes  BIGINT      NOT NULL CHECK (budget_bytes >= 0),
    fencing_epoch BIGINT      NOT NULL CHECK (fencing_epoch >= 0),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
