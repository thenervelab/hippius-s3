-- Tail re-check for PutObject If-None-Match: *, run right after lock_object_row_for_update.
--
-- True when a version other than ours became serveable, and not as a delete marker, after our
-- reserve: another writer created the key while this request was streaming, so ours must not land.
-- Versions at or below the reserve-time baseline were already judged by conditional_write_state.
--
-- $1 object_id (uuid), $2 our object_version (bigint), $3 baseline (bigint)
SELECT EXISTS (
    SELECT 1
    FROM object_versions v
    WHERE v.object_id = $1
      AND v.object_version > $3
      AND v.object_version <> $2
      AND v.deleted_at IS NULL
      AND NOT v.is_delete_marker
      AND (v.is_delete_marker OR v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
)
