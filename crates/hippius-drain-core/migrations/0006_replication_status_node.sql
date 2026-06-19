-- Node-scope the part replication queue.
--
-- A part's chunks live on the node-local SSD ingest dir, so a part may only be
-- drained by the node that holds it. cephor_replication_status was global (keyed by
-- (object_id, version, part_number), no node), and claim_part selected the oldest
-- pending row regardless of node. With >1 ingest node a drain-agent therefore
-- routinely claimed a part whose data sits on a *peer* node, found the local part dir
-- missing, and failed at the meta copy (ENOENT) -- churning forever, draining nothing.
--
-- Add node_id so the per-node reconciler stamps the owning node (record_landed_part
-- UPSERT) and claim_part scopes to it. Legacy rows are NULL until the node that still
-- holds the part locally re-records and adopts them.
ALTER TABLE cephor_replication_status ADD COLUMN node_id TEXT;

-- The claim selector is now (node_id = $node AND status = 'pending') ordered by
-- landed_at; replace the global pending index with a node-scoped one that matches.
DROP INDEX IF EXISTS cephor_replication_status_pending;
CREATE INDEX cephor_replication_status_pending
    ON cephor_replication_status (node_id, landed_at)
    WHERE status = 'pending';
