-- Sum ONE keyset page of a bucket, for verifying a bucket too large to aggregate in one statement.
--
-- This is the whole reason sliced verification is possible. The unsliced aggregate over prod's
-- largest bucket (80.7% of the objects table) plans as a parallel seq scan of objects hash-joined to
-- object_versions, cost 15,032,678. Bounded to a key range it plans as an index scan on
-- idx_objects_bucket_prefix_active feeding a nested loop against object_versions_pkey, cost 1,212 --
-- about 12,000x cheaper, and flat in the size of the bucket rather than linear.
--
-- LEFT JOIN, with the version predicate as a FILTER rather than a join condition. An inner join would
-- drop objects whose current version is deleted or is a delete marker, and those rows still have to
-- advance the cursor -- otherwise a page made entirely of them returns no next_cursor and the sweep
-- stalls on it forever. The FILTER reproduces get_account_storage_bytes.sql's SUM exactly: such rows
-- contribute nothing.
--
-- `object_key > $2` is a strict keyset cursor. (bucket_id, object_key) is UNIQUE, so it is a total
-- order: no row can be skipped and none can be visited twice. '' starts a sweep because object_key
-- is NOT NULL and non-empty in practice; an empty key would simply be included on the first page.
WITH page AS (
    SELECT o.object_id, o.current_object_version, o.object_key
    FROM objects o
    WHERE o.bucket_id = $1
      AND o.deleted_at IS NULL
      AND o.object_key > $2
    ORDER BY o.object_key
    LIMIT $3
)
SELECT
    COALESCE(
        SUM(ov.size_bytes) FILTER (WHERE ov.deleted_at IS NULL AND NOT ov.is_delete_marker),
        0
    )::bigint AS bytes,
    count(*)::bigint AS objects_scanned,
    MAX(p.object_key) AS next_cursor
FROM page p
LEFT JOIN object_versions ov
       ON ov.object_id = p.object_id
      AND ov.object_version = p.current_object_version
