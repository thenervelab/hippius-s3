-- Per-PART replication status: the authoritative state cephor claims and commits
-- as it drains a part from SSD to the CephFS pool. The hippius-s3 api lays an object
-- out as <object_id>/v<version>/part_<n>/ with a meta.json marker, so the drain's
-- unit is the PART, identified by (object_id, version, part_number) — not a
-- content-addressed chunk. The reconciler backfills a 'pending' row for any complete
-- SSD part it finds without one; there is no api-emitted trigger (reconciler-only).
--
-- claimed_at records when the current claim was taken (server clock) so a claim
-- older than the lease TTL is recognisably abandoned and re-claimable; NULL while
-- not 'draining', stamped by claim_part, cleared by release_part. Because every
-- drain step is idempotent (path-preserving copy, byte-verify before commit,
-- mark_replicated guarded by the 'draining' status), re-claiming a stale row is safe
-- against a late-committing zombie agent.
--
-- version/part_number are BIGINT because the domain numbers are u32: u32::MAX
-- (4294967295) exceeds INTEGER's i32 range, so INTEGER would reject high values.
CREATE TABLE cephor_replication_status (
    object_id   TEXT        NOT NULL,
    version     BIGINT      NOT NULL,
    part_number BIGINT      NOT NULL,
    status      TEXT        NOT NULL DEFAULT 'pending'
                            CHECK (status IN ('pending', 'draining', 'replicated', 'failed')),
    landed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    claimed_at  TIMESTAMPTZ,
    PRIMARY KEY (object_id, version, part_number)
);

-- The SKIP-LOCKED claim and the reconciler backlog both filter status='pending'
-- ordered by arrival, so a partial index keeps that hot path off the full table.
CREATE INDEX cephor_replication_status_pending
    ON cephor_replication_status (landed_at)
    WHERE status = 'pending';

-- The re-claim branch of the claim selector filters
-- status='draining' AND claimed_at < now() - lease; a partial index keeps that
-- recovery scan off the full table, mirroring the pending hot-path index.
CREATE INDEX cephor_replication_status_stale_draining
    ON cephor_replication_status (claimed_at)
    WHERE status = 'draining';
