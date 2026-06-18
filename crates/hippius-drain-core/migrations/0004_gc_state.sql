-- Durable GC claim/completion marker for in-flight-terminal cleanup (M6).
-- One row per file being reclaimed; claimed_at makes the CephFS-side GC a
-- once-claimed task (only the agent that inserts the row does the rm).
CREATE TABLE cephor_gc_state (
    file_id      TEXT        PRIMARY KEY,
    claimed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    completed_at TIMESTAMPTZ
);
