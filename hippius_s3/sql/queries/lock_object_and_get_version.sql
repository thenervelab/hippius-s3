-- Lock the objects row AND resolve the target version in one statement.
--
-- The lock is what makes a versioned DELETE atomic against a concurrent DELETE of a DIFFERENT
-- version of the same key: without it, one request can pick a successor that the other soft-deletes
-- before the pointer moves, stranding current_object_version on a dead row (invisible to reads, but
-- permanently un-unpinnable — get_chunk_backend_identifiers refuses the current version of a live
-- object). Taking the lock and reading the version separately cost an extra round trip on every
-- versioned DELETE, which DeleteObjects multiplies by up to 1000.
--
-- LEFT JOIN so an absent or already-deleted version still returns the (locked) objects row with
-- NULL version columns; the caller turns that into an idempotent 204. `FOR UPDATE OF o` is required
-- rather than a bare FOR UPDATE — Postgres refuses to lock the nullable side of an outer join.
--
-- Parameters: $1: bucket_id (uuid), $2: object_key (text), $3: object_version (bigint)
--
-- Resolved through resolve_object_id so an ALIAS key (a second name attached to one object_id by
-- a same-bucket CopyObject) reaches the same object rather than silently matching nothing. The
-- caller refuses the delete outright when the object carries more than one name, because the
-- version is shared: destroying it would remove content still published under the other key.
--
-- `alias_count` is projected here rather than read separately so that decision happens inside
-- this same row lock — an alias cannot be attached between the check and the delete.
SELECT o.object_id,
       o.current_object_version,
       ov.object_version,
       ov.is_delete_marker,
       -- OBJECT LOCK: read inside the same FOR UPDATE row lock as the rest, so a retention or
       -- legal hold committed concurrently cannot land between the check and the delete.
       ov.object_lock_mode,
       ov.object_lock_retain_until,
       ov.object_lock_legal_hold,
       (SELECT count(*) FROM object_names n WHERE n.object_id = o.object_id)::int AS alias_count
FROM objects o
LEFT JOIN object_versions ov
       ON ov.object_id = o.object_id
      AND ov.object_version = $3
      AND ov.deleted_at IS NULL
WHERE o.object_id = resolve_object_id($1::uuid, $2)
FOR UPDATE OF o
