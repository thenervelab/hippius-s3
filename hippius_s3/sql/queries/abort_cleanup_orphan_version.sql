-- B5: clean up the orphan object_versions row an aborted MPU leaves behind, and repoint the
-- object's current_object_version off it. The read path already falls back to the latest
-- completed version (get_object_for_download_with_permissions skips size_bytes=0/empty-md5
-- placeholders), so this is DB hygiene, not a correctness fix: without it, an aborted upload's
-- empty reserved version and a stale current_object_version pointer linger forever.
--
-- Safety:
--   * Only deletes a still-reserved version (size_bytes=0 AND md5 empty/NULL) — never a completed one.
--   * Never deletes the object's SOLE version: current_object_version is NOT NULL and FK-references
--     object_versions (objects_current_version_fk), so repointing has to have a target. When $2 is
--     the only version, `fallback.v` is NULL and neither branch fires (orphan is left as-is).
--   * The FK is DEFERRABLE INITIALLY DEFERRED, so the repoint + delete are checked together at
--     statement end: current points at the surviving version by the time the constraint is verified.
--   * Only repoints when current == $2; a newer version that already owns the pointer is untouched.
-- Params: $1 object_id (uuid), $2 object_version (bigint)
WITH fallback AS (
    SELECT MAX(object_version) AS v
    FROM object_versions
    WHERE object_id = $1 AND object_version <> $2
),
repoint AS (
    UPDATE objects o
    SET current_object_version = f.v
    FROM fallback f
    WHERE o.object_id = $1
      AND o.current_object_version = $2
      AND f.v IS NOT NULL
    RETURNING o.object_id
)
DELETE FROM object_versions ov
USING fallback f
WHERE ov.object_id = $1
  AND ov.object_version = $2
  AND f.v IS NOT NULL
  AND ov.size_bytes = 0
  AND (ov.md5_hash IS NULL OR ov.md5_hash = '')
RETURNING ov.object_version;
