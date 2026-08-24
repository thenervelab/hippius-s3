-- B5: repoint the object's current_object_version off the orphan version an aborted MPU leaves
-- behind. The reserved object_versions row itself is DELIBERATELY RETAINED — see "Why the row is
-- not deleted" below. The read path already falls back to the latest completed version
-- (get_object_for_download_with_permissions skips size_bytes=0/empty-md5 placeholders), so the
-- retained row is invisible to reads; without the repoint, a stale current_object_version pointer
-- would linger forever.
--
-- Why the row is not deleted (the abort -> version-reuse poison):
--   Every version allocator — upsert_object_basic.sql, upsert_object_multipart.sql and
--   upsert_object_with_cid.sql — computes
--       GREATEST(objects.current_object_version, MAX(ov.object_version)) + 1.
--   Deleting version N and repointing current to N-1 made BOTH inputs N-1, so the very next
--   upload on this key was handed N again. cephor_replication_status has no FK to object_versions,
--   so the aborted attempt's rows — which the abort marks terminal 'failed' a few statements
--   earlier (fail_replication_status_for_version.sql) — survived the delete, and the reused
--   version's parts landed straight onto them. Nothing re-drives a 'failed' row: the reconciler
--   skips it, claim_part never claims it, and the R4 re-drive worker reads only 'corrupt'. The
--   result was a completed, servable version with no pool copy, no backend upload and no
--   chunk_backend rows — readable only from the single ingest node still holding the SSD copy.
--   Retaining the row keeps MAX(object_version) = N, so the next allocation is N+1 and the abort
--   path can no longer reissue the number. It also keeps the aborted version permanently
--   UNSERVABLE (size 0, empty md5, NULL address), which is what lets the drain's reclaim free its
--   SSD copies instead of pinning them as the corrupt-live (skipped_corrupt) case.
--   This is NOT a schema-wide invariant, only an abort-path one: scripts that DELETE the highest
--   object_versions row (cleanup_migration_versions.py, delete_legacy_object_versions.py, and the
--   currently-uncalled delete_version_and_parts.sql) can still drop MAX and reopen the same poison
--   through their own door.
--   Cost is one empty row per abort. The ALLOCATOR's MAX() is unfiltered, so it stays an
--   index-only backward scan on object_versions_pkey (object_id, object_version) — O(1) however
--   many tombstones accrue. The filtered MAX() in this query's fallback does walk back through
--   them, but it runs once per abort, not on any read path.
--
-- Known residual (pre-existing class, not introduced here): the fallback reads OTHER versions of
-- the object without locking them, so a CompleteMultipartUpload of a LOWER in-flight version that
-- commits after this statement's snapshot is invisible, and the pointer can land below it. The
-- root cause is that CompleteMultipartUpload writes only object_versions and never advances
-- objects.current_object_version — it assumes initiate already did. Fixing that is the real
-- repair; until then this query cannot do better than its snapshot for versions it does not own.
--
-- Safety:
--   * Only repoints off a STILL-RESERVED version (size_bytes<=0 AND md5 empty/NULL) — never off a
--     completed one, which would hide live data behind an older pointer. Enforced by the `locked`
--     CTE above, which makes that check a locked compare-and-swap rather than a snapshot read.
--   * Never repoints when $2 is the object's SOLE version: current_object_version is NOT NULL and
--     FK-references object_versions (objects_current_version_fk), so a repoint needs a target.
--     When $2 is the only version, `fallback.v` is NULL and nothing fires.
--   * Only repoints when current == $2; a newer version that already owns the pointer is untouched.
-- Params: $1 object_id (uuid), $2 object_version (bigint)
WITH locked AS (
    -- The reserved-check must be a real CAS against CompleteMultipartUpload, which can land on
    -- THIS version while the abort is in flight (an SDK retrying a Complete it thinks timed out
    -- issues exactly this pair). A plain EXISTS here would not do it: under READ COMMITTED this
    -- statement's snapshot is fixed at statement start, and EvalPlanQual refreshes only the UPDATE's
    -- own target row in `objects` — reads of `object_versions`, a different relation, are never
    -- re-evaluated. The old code was accidentally safe here because `size_bytes = 0` was a qual on
    -- the DELETE's own target row, so EPQ rechecked it; moving that predicate off the target row is
    -- what would have lost the protection.
    --
    -- FOR UPDATE restores it: it takes the row lock, always reads the latest committed row version,
    -- and re-applies the qual to it. A completion that committed first makes this CTE empty (no
    -- repoint); one that arrives later blocks until this statement commits.
    SELECT ov.object_version
    FROM object_versions ov
    WHERE ov.object_id = $1
      AND ov.object_version = $2
      AND ov.size_bytes <= 0
      AND (ov.md5_hash IS NULL OR ov.md5_hash = '')
    FOR UPDATE
),
fallback AS (
    -- PREFER the highest completed version, but never decline for lack of one.
    --
    -- Preference: current_object_version should land on something a read can serve. Retained
    -- tombstones and the reserved row of a concurrently in-flight MPU are empty; unversioned
    -- GET/HEAD survive a pointer on one (they scan DOWN from current for the first serveable row)
    -- but every query that joins current_object_version directly does not.
    --
    -- Why the second arm exists: two MPUs can be open on a new key at once, so at abort time BOTH
    -- versions can still be reserved and there is no completed row to point at. Declining then
    -- would strand current_object_version on the aborted version permanently — nothing repoints it
    -- later, because CompleteMultipartUpload only writes object_versions and assumes initiate
    -- already set the pointer. A DELETE would then resolve to the stranded version, find no
    -- chunk_backend rows, and leave the real version's chunks pinned forever. Falling back to the
    -- highest remaining version is exactly what this query did before, so this arm is never worse.
    --
    -- Bounded BELOW $2 in both arms: the migrator finalizes a version above current before its CAS
    -- promotes it (migrate_objects.py), so an unbounded MAX could promote it early and break that
    -- CAS. The pre-change query had that reach; this closes it.
    SELECT COALESCE(
        (SELECT MAX(object_version) FROM object_versions
          WHERE object_id = $1 AND object_version < $2
            AND (size_bytes > 0 OR (md5_hash IS NOT NULL AND md5_hash != ''))),
        (SELECT MAX(object_version) FROM object_versions
          WHERE object_id = $1 AND object_version < $2)
    ) AS v
)
UPDATE objects o
SET current_object_version = f.v
FROM fallback f, locked l
WHERE o.object_id = $1
  AND o.current_object_version = $2
  AND f.v IS NOT NULL
RETURNING o.object_id, o.current_object_version;
