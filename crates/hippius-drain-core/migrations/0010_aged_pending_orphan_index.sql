-- Covering partial index for the janitor's aged-pending-orphan gauge
-- (count_aged_pending_orphans.sql) and its sibling sweep
-- (list_orphan_replication_versions.sql), both of which scan the ACTIVE
-- (non-terminal) rows across ALL nodes, group by (object_id, version), and take
-- MAX(landed_at) — a shape neither existing partial index serves:
--   * cephor_replication_status_pending is (node_id, landed_at) WHERE status='pending'
--     — node-scoped and pending-only, so it misses 'draining' and the cross-node gauge.
--   * cephor_replication_status_stale_draining is (claimed_at) WHERE status='draining'.
-- Without a matching index the gauge/sweep fall to a full-table seq scan every janitor
-- cycle, which at prod cardinality (R2) is the CNPG-pressure risk we are hardening against.
--
-- Partial on the two non-terminal states (the orphan population is always a small,
-- bounded subset of a mostly-terminal table), ordered (object_id, version, landed_at) so
-- the grouped MAX(landed_at) is served directly from the index.
CREATE INDEX IF NOT EXISTS cephor_replication_status_active_orphans
    ON cephor_replication_status (object_id, version, landed_at)
    WHERE status IN ('pending', 'draining');
