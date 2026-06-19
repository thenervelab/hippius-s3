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
-- UPSERT) and claim_part scopes to it.
ALTER TABLE cephor_replication_status ADD COLUMN node_id TEXT;

-- Existing non-terminal rows predate node ownership (node_id NULL) and are unclaimable
-- under the node-scoped claim. They are NOT self-healing: the reconciler only records a
-- part it sees as absent (status None), never one already 'pending'/'draining', so a
-- NULL-node row would orphan forever. The node-local SSD is the source of truth, so
-- clear the in-flight queue here and let each node's reconciler rebuild it node-aware
-- (scan SSD -> status None -> record_landed stamps node_id). Terminal rows
-- ('replicated'/'failed') are kept. This runs once; in steady state record_landed_part
-- always stamps a node, so no node_id IS NULL rows are created.
DELETE FROM cephor_replication_status WHERE node_id IS NULL AND status IN ('pending', 'draining');

-- The claim selector is now (node_id = $node AND status = 'pending') ordered by
-- landed_at; replace the global pending index with a node-scoped one that matches.
DROP INDEX IF EXISTS cephor_replication_status_pending;
CREATE INDEX cephor_replication_status_pending
    ON cephor_replication_status (node_id, landed_at)
    WHERE status = 'pending';
