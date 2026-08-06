-- Track which replicated parts are still RESIDENT on their node's ingest SSD.
--
-- Until now "replicated" and "gone from the SSD" were the same event: drain_part unlinked
-- the local copy as the last step of its happy path, so the SSD held exactly the undrained
-- backlog. Retaining replicated parts to serve reads from local NVMe (~705 MB/s, ~6 ms per
-- chunk) instead of the CephFS pool (~94 MB/s, ~40 ms, measured on node1 2026-08-06) breaks
-- that equivalence, so residency needs its own state.
--
-- resident_at:
--   NULL      -> this part is NOT on the SSD. Two populations share this: every row that
--                predates retention (the drain already unlinked it), and any row that never
--                reached 'replicated'.
--   timestamp -> the drain committed 'replicated' and DELIBERATELY kept the SSD copy.
--
-- evicted_at:
--   NULL      -> not yet evicted.
--   timestamp -> the evictor reclaimed the SSD copy; the pool copy remains authoritative.
--
-- Resident predicate: `resident_at IS NOT NULL AND evicted_at IS NULL`.
--
-- Keying residency on a POSITIVE marker (resident_at) rather than on `evicted_at IS NULL`
-- alone is the load-bearing choice here, and it is what makes this migration backfill-free.
-- Prod holds ~11.08M 'replicated' rows (2026-08-06) whose SSD copies were unlinked long ago.
-- Under an `evicted_at IS NULL` predicate every one of them would read as resident: the
-- evictor would chase ~5.4 TB of parts that do not exist, and each node's heartbeat would
-- report a phantom multi-terabyte cache — which, via the residency-aware pressure signal,
-- would understate that node's true drain urgency. With resident_at they are simply NULL and
-- therefore not resident, with no 11M-row UPDATE and no rewrite. Both ADD COLUMNs are
-- metadata-only (no DEFAULT), so neither rewrites the table.
ALTER TABLE cephor_replication_status ADD COLUMN resident_at TIMESTAMPTZ;
ALTER TABLE cephor_replication_status ADD COLUMN evicted_at TIMESTAMPTZ;

-- lock_timeout, as on 0013: CREATE INDEX takes SHARE, which conflicts with the ROW EXCLUSIVE
-- held by every drain writer (claim_part / release_part / mark_replicated / record_landed_part).
-- With no timeout it would wait out any open writer AND queue every new one behind it, and
-- migrate() runs in the allocator's startup path before its liveness file is first touched —
-- a long wait is CrashLoopBackOff, with each restart re-queuing the lock. Fail fast, retry on
-- the next start.
SET LOCAL lock_timeout = '5s';

-- The evictor's worklist and the heartbeat's cache_bytes sum: this node's resident parts,
-- oldest-resident first. node_id leads so the equality is an index condition; resident_at
-- follows so the eviction cursor reads straight off the index in order.
--
-- The partial predicate matches ZERO rows on arrival (nothing has been retained yet), so
-- despite the 11M-row table this builds fast and near-empty — the scan is sequential but
-- almost nothing is written. It then grows only with genuinely resident parts, bounded by
-- what fits on a 3.84 TB NVMe rather than by the table.
--
-- Plain build, NOT CONCURRENTLY, for the reasons spelled out in 0013: CONCURRENTLY needs
-- `-- no-transaction`, and a failed concurrent build leaves an INVALID index that
-- IF NOT EXISTS then silently refuses to rebuild while the migration records as applied.
--
-- Rollback: `DROP INDEX CONCURRENTLY cephor_replication_resident_idx;` by hand. sqlx will
-- not recreate it (the _sqlx_migrations row stays), so no revert migration is needed. The
-- columns can stay — they are inert unless the drain writes them.
CREATE INDEX IF NOT EXISTS cephor_replication_resident_idx
    ON cephor_replication_status (node_id, resident_at)
    WHERE resident_at IS NOT NULL AND evicted_at IS NULL;
