-- State of a key for a conditional write (PutObject If-None-Match: *), read inside the reserve
-- transaction right after lock_object_by_key_for_update.
--
--   exists_live  The key currently serves an object, resolved the way reads resolve it: the live
--                primary row or an object_names alias (resolve_object_id), then its newest serveable
--                version at or below current_object_version, which must not be a delete marker.
--   baseline     Newest serveable version of the key's primary row, soft-deleted or not (0 if none).
--                The tail re-check only counts versions ABOVE it, so what the key held before this
--                request — including content hidden by a soft delete — is never mistaken for a
--                concurrent writer that won the race.
--
-- "Serveable" is the predicate the listing and download queries use: a version is invisible until
-- its finalizing UPDATE writes size/md5, so an in-flight upload does not count.
--
-- $1 bucket_id (uuid), $2 object_key (text)
WITH hit AS (
    SELECT o.object_id, o.current_object_version
    FROM objects o
    WHERE o.object_id = resolve_object_id($1::uuid, $2)
), newest AS (
    SELECT v.is_delete_marker
    FROM hit h
    CROSS JOIN LATERAL (
        SELECT v.is_delete_marker
        FROM object_versions v
        WHERE v.object_id = h.object_id
          AND v.object_version <= h.current_object_version
          AND v.deleted_at IS NULL
          AND (v.is_delete_marker OR v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
        ORDER BY v.object_version DESC
        LIMIT 1
    ) v
)
SELECT
    COALESCE((SELECT NOT is_delete_marker FROM newest), FALSE) AS exists_live,
    COALESCE((
        SELECT max(v.object_version)
        FROM objects o
        JOIN object_versions v ON v.object_id = o.object_id
        WHERE o.bucket_id = $1::uuid
          AND o.object_key = $2
          AND v.deleted_at IS NULL
          AND (v.is_delete_marker OR v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
    ), 0)::bigint AS baseline
