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
--   Retaining the row keeps MAX(object_version) = N, so the next allocation is N+1 and the number
--   can never be reused. It also keeps the aborted version permanently UNSERVABLE (size_bytes=0,
--   empty md5, NULL address), which is what lets the drain's reclaim free its SSD copies instead
--   of pinning them as the corrupt-live (skipped_corrupt) case.
--   Cost is one empty row per abort. MAX() above is an index-only backward scan on
--   object_versions_pkey (object_id, object_version), so retained rows cost O(1) to skip.
--
-- Safety:
--   * Only repoints off a STILL-RESERVED version (size_bytes=0 AND md5 empty/NULL) — never off a
--     completed one, which would hide live data behind an older pointer. This predicate used to
--     guard the DELETE; it guards the repoint now that the delete is gone.
--   * Never repoints when $2 is the object's SOLE version: current_object_version is NOT NULL and
--     FK-references object_versions (objects_current_version_fk), so a repoint needs a target.
--     When $2 is the only version, `fallback.v` is NULL and nothing fires.
--   * Only repoints when current == $2; a newer version that already owns the pointer is untouched.
-- Params: $1 object_id (uuid), $2 object_version (bigint)
WITH fallback AS (
    -- Highest COMPLETED version, not merely the highest version: current_object_version must land
    -- on something a read can serve. Retained abort tombstones (and the reserved row of a
    -- concurrently in-flight MPU) are higher-numbered but empty, and a bare MAX() would repoint
    -- onto one — leaving `current` on a 0-byte placeholder. Unversioned GET/HEAD survive that
    -- (they scan DOWN from current for the first serveable row) but every query that joins
    -- current_object_version directly does not. When no completed version exists, `v` is NULL and
    -- nothing fires, which is the same fail-safe as the sole-version case.
    SELECT MAX(object_version) AS v
    FROM object_versions
    WHERE object_id = $1
      AND object_version <> $2
      AND (size_bytes > 0 OR (md5_hash IS NOT NULL AND md5_hash != ''))
)
UPDATE objects o
SET current_object_version = f.v
FROM fallback f
WHERE o.object_id = $1
  AND o.current_object_version = $2
  AND f.v IS NOT NULL
  AND EXISTS (
        SELECT 1
        FROM object_versions ov
        WHERE ov.object_id = $1
          AND ov.object_version = $2
          AND ov.size_bytes = 0
          AND (ov.md5_hash IS NULL OR ov.md5_hash = '')
      )
RETURNING o.object_id, o.current_object_version;
