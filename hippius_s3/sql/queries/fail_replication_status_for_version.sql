-- Mark every still-active replication row for one object version terminal ('failed'),
-- the central churn-stopper for an aborted or abandoned upload whose object_versions
-- address will never be written. The drain owns cephor_replication_status but shares the
-- hippius DB. A 'failed' row is skipped by the reconciler (not re-recorded) and by
-- claim_part (not re-claimed) on EVERY node, so the per-node drain stops re-copying and
-- re-deferring the parts — even though the node-local SSD copies (unreachable from a
-- central caller) remain for the orphan GC to reclaim. Deleting the rows instead would
-- be undone: each node's reconciler re-records a part it still sees on local SSD. Only
-- 'pending'/'draining' rows are touched; a 'replicated' row is legitimately done.
-- Parameters: $1: object_id (text), $2: object_version (bigint)
UPDATE cephor_replication_status
SET status = 'failed', updated_at = now(), claimed_at = NULL
WHERE object_id = $1 AND version = $2 AND status IN ('pending', 'draining')
