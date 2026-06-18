-- Singleton leader lease for the allocator (election logic lands in M7).
-- The BOOLEAN primary key fixed to TRUE makes this a single-row table by
-- construction: there can be at most one lease. epoch increments on each new
-- leadership era so it can fence a deposed leader's writes.
CREATE TABLE cephor_leader_lease (
    only_row    BOOLEAN     PRIMARY KEY DEFAULT TRUE CHECK (only_row),
    instance_id TEXT        NOT NULL,
    epoch       BIGINT      NOT NULL DEFAULT 0 CHECK (epoch >= 0),
    expires_at  TIMESTAMPTZ NOT NULL
);
