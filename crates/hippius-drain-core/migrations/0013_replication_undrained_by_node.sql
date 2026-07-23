-- Index the readiness/heartbeat query `node_undrained_count`:
--   WHERE node_id = $1 AND status IN ('pending', 'draining')
--
-- No existing index serves that pair, so it fell to the only one whose predicate covers
-- both states — cephor_replication_status_active_orphans (object_id, version, landed_at)
-- WHERE status IN ('pending','draining') — which is keyed on object_id, not node_id. The
-- planner therefore scanned every active entry and discarded the other nodes' rows with a
-- post-filter ("Rows Removed by Filter: 53" for 28 kept, on prod 2026-07-23). The
-- docstring on Store::node_undrained_count claimed it "uses the node-scoped pending
-- index"; that index is WHERE status = 'pending' only, so it never applied to a query
-- that also matches 'draining'.
--
-- node_id leads so the equality is an index condition rather than a filter, and the whole
-- query is answerable from the index. Partial on the two non-terminal states, matching the
-- sibling hot-path indexes: the active population is a small bounded subset (99 rows out
-- of 1.79M on prod) of a table that is ~99.98% terminal.
--
-- Scale note: this makes a correct plan cheap, but it is NOT what made that query slow.
-- On 2026-07-23 it read 239,014 buffers to return 28 rows because a 96-minute statement
-- elsewhere (the abandoned-MPU reaper, fixed alongside this) pinned the xmin horizon, so
-- VACUUM could not reclaim and this index carried ~116k dead entries per scan. An index
-- cannot outrun bloat — keeping the horizon moving is what does.
-- Build is plain, NOT CONCURRENTLY, and that is deliberate at this cardinality: 1.8M rows
-- builds in seconds and rolls back cleanly if aborted. CONCURRENTLY was rejected because
-- (a) it needs `-- no-transaction`, and a failed concurrent build leaves an INVALID index
-- that `IF NOT EXISTS` then silently refuses to rebuild while the migration records as
-- applied — a dead index the planner ignores, with no operator-visible apply job to catch
-- it (the python half hit exactly this: see 20260706000000_parts_upload_uploaded_at_index),
-- and (b) it waits out all older snapshots twice, which on a horizon-pinned database is
-- unbounded — the precise failure this PR exists to fix. Do NOT copy this file onto a
-- large table: the sibling `parts` is 140M rows and would need the CONCURRENTLY route.
--
-- lock_timeout is the load-bearing line. CREATE INDEX takes SHARE, which conflicts with
-- ROW EXCLUSIVE, so with no timeout it waits forever for any open writer AND blocks every
-- new writer on this table behind it — claim_part, release_part, mark_replicated,
-- record_landed_part, i.e. the entire drain fleet. Worse, migrate() runs in the allocator's
-- startup path BEFORE its liveness file is first touched, and that probe SIGKILLs at
-- ~50-65s: a long lock wait means CrashLoopBackOff, each restart re-queuing the lock and
-- re-stalling the fleet, plus a failed 5m deploy gate. Failing fast and retrying on the
-- next start is strictly better. In the healthy case it never fires — drain writers are
-- all single autocommit statements.
--
-- Rollback, if this ever proves a net loss: `DROP INDEX CONCURRENTLY
-- cephor_replication_status_undrained_by_node;` by hand. sqlx will not recreate it (the
-- _sqlx_migrations row stays), so no revert migration is needed.
SET LOCAL lock_timeout = '5s';

CREATE INDEX IF NOT EXISTS cephor_replication_status_undrained_by_node
    ON cephor_replication_status (node_id)
    WHERE status IN ('pending', 'draining');

-- Vacuum this table on its own schedule. At the default scale_factor of 0.2 autovacuum
-- waits for ~360k dead tuples on 1.8M live before it even tries, which is why 499k dead
-- looked unremarkable to it through 429 runs on 2026-07-23. Every part churns through
-- pending -> draining -> replicated -> upload_enqueued_at, so dead tuples arrive far faster
-- here than the table-size heuristic assumes, and each one strands entries in SEVEN partial
-- indexes (this migration adds the seventh, on the same hot predicate as active_orphans —
-- so it doubles the bloat surface of the one predicate that already blew up, which makes
-- this setting more necessary, not less).
--
-- Deliberately NOT setting autovacuum_vacuum_cost_delay = 0: unthrottled vacuum on a
-- primary with this cluster's history of flapping is its own outage risk. Lowering the
-- trigger threshold is the safe half of that pair; if bloat persists after this, raise
-- cost_limit under supervision rather than removing the throttle outright.
ALTER TABLE cephor_replication_status SET (
    autovacuum_vacuum_scale_factor = 0.02,
    autovacuum_analyze_scale_factor = 0.02
);
