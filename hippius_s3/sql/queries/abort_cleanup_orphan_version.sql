-- B5: repoint objects.current_object_version off the reserved version an aborted MPU leaves
-- behind. The reserved object_versions row itself is RETAINED.
--
-- Why the row is not deleted (the abort -> version-reuse poison):
--   Every version allocator — upsert_object_basic.sql, upsert_object_multipart.sql and
--   upsert_object_with_cid.sql — computes
--       GREATEST(objects.current_object_version, MAX(ov.object_version)) + 1.
--   Deleting version N and repointing current to N-1 made BOTH inputs N-1, so the next upload on
--   this key was handed N again. cephor_replication_status has no FK to object_versions, so the
--   aborted attempt's rows — which the abort marks terminal 'failed' a few statements earlier
--   (fail_replication_status_for_version.sql) — survived the delete, and the reused version's
--   parts landed on them. Nothing re-drives a 'failed' row: the reconciler skips it, claim_part
--   never claims it, and the R4 re-drive worker reads only 'corrupt'. The result was a completed,
--   servable version with no pool copy and no backend upload, readable only from the one ingest
--   node still holding the SSD copy. Retention also keeps the aborted version permanently
--   unservable, which is what lets the drain's reclaim free those SSD copies rather than pin them.
--   This is an abort-path invariant, not a schema-wide one: cleanup_migration_versions.py,
--   delete_legacy_object_versions.py and the uncalled delete_version_and_parts.sql still delete
--   the highest row and can reopen the same poison.
--
-- Known residual: the fallback reads other versions without locking them, so a
-- CompleteMultipartUpload of a LOWER in-flight version committing after this statement's snapshot
-- is invisible and the pointer can land below it. The root cause is that Complete writes only
-- object_versions and never advances current_object_version — it assumes initiate did. Fixing
-- that is the real repair; this query cannot do better than its snapshot for rows it does not own.
-- Params: $1 object_id (uuid), $2 object_version (bigint)
WITH locked AS (
    -- EvalPlanQual refreshes only the UPDATE's own target row in `objects`, so a bare EXISTS on
    -- object_versions would keep the statement-start snapshot and miss a CompleteMultipartUpload
    -- of this version landing mid-statement (an SDK retrying a Complete it thinks timed out issues
    -- exactly that pair). FOR UPDATE makes the reserved-check a real compare-and-swap.
    SELECT ov.object_version
    FROM object_versions ov
    WHERE ov.object_id = $1
      AND ov.object_version = $2
      AND ov.size_bytes <= 0
      AND (ov.md5_hash IS NULL OR ov.md5_hash = '')
    FOR UPDATE
),
fallback AS (
    -- Prefer a completed version: queries that join current_object_version directly cannot skip a
    -- placeholder the way unversioned GET does. But never decline for lack of one — two MPUs can
    -- be open on a new key at once, and stranding the pointer on the aborted version is permanent
    -- (Complete never advances it), which makes DELETE resolve to a version with no chunk_backend
    -- rows and leave the real version's chunks pinned forever.
    -- Bounded below $2: the migrator finalizes a version above current before its CAS promotes it,
    -- so an unbounded reach could promote it early and break that CAS.
    SELECT object_version AS v
    FROM object_versions
    WHERE object_id = $1 AND object_version < $2
    ORDER BY (size_bytes > 0 OR (md5_hash IS NOT NULL AND md5_hash != '')) DESC, object_version DESC
    LIMIT 1
)
UPDATE objects o
SET current_object_version = f.v
FROM fallback f
WHERE o.object_id = $1
  AND o.current_object_version = $2
  AND EXISTS (SELECT 1 FROM locked)
RETURNING o.object_id, o.current_object_version;
