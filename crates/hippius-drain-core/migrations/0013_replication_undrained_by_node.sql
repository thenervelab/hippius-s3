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
CREATE INDEX IF NOT EXISTS cephor_replication_status_undrained_by_node
    ON cephor_replication_status (node_id)
    WHERE status IN ('pending', 'draining');
