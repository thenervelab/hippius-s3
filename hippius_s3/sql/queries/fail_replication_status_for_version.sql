-- Mark every still-active replication row for one object version terminal ('failed'),
-- the central churn-stopper for an aborted or abandoned upload whose object_versions
-- address will never be written. The drain owns cephor_replication_status but shares the
-- hippius DB. A 'failed' row is skipped by the reconciler (not re-recorded) and by
-- claim_part (not re-claimed) on EVERY node, so the per-node drain stops re-copying and
-- re-deferring the parts — even though the node-local SSD copies (unreachable from a
-- central caller) remain for the orphan GC to reclaim. Deleting the rows instead would
-- be undone: each node's reconciler re-records a part it still sees on local SSD.
--
-- Two classes are failed:
--   * 'pending'/'draining' — an active, never-committed part (unchanged behavior);
--   * 'replicated' with upload_enqueued_at IS NULL AND the version UNSERVABLE — the Tier-2
--     decoupled-commit orphan: a part committed to the pool before its address was written,
--     whose backend upload the enqueue sweep can never publish because the abandoned object
--     never got an address. Without failing it, the janitor's replication gate pins its pool
--     copy forever (no chunk_backend rows). The UNSERVABLE guard (address IS NULL AND
--     size_bytes<=0 AND md5_hash='') is MANDATORY on this arm: it keeps a NULL-$2 abort of one
--     abandoned upload from flipping a *different, servable* version's part that is only
--     momentarily replicated+unenqueued (an inline enqueue that hit a transient Redis blip) —
--     that part is still on the enqueue-sweep worklist and must be published, not failed. The
--     pending/draining arm keeps its original un-servability-gated behavior (the caller — the
--     reaper's abandoned/orphan selection — already scoped it).
-- A NULL $2 fails every still-active row for the object regardless of version: an
-- abandoned upload (address IS NULL) is junk at every version, and legacy parts can
-- carry a NULL object_version, so keying on version alone would strand them.
-- Parameters: $1: object_id (text), $2: object_version (bigint, NULL = all versions)
UPDATE cephor_replication_status crs
SET status = 'failed', updated_at = now(), claimed_at = NULL
WHERE crs.object_id = $1 AND ($2::bigint IS NULL OR crs.version = $2)
  AND (
        crs.status IN ('pending', 'draining')
        OR (
             crs.status = 'replicated' AND crs.upload_enqueued_at IS NULL
             AND EXISTS (
               SELECT 1 FROM object_versions ov
               WHERE ov.object_id = crs.object_id::uuid
                 AND ov.object_version = crs.version
                 AND ov.address IS NULL
                 AND ov.size_bytes <= 0
                 AND COALESCE(ov.md5_hash, '') = ''
             )
           )
      )
