-- $1 object_id, $2 object_version (NULL = object-level check only)
-- Always returns exactly one row: TRUE (deleted/missing) or FALSE (exists and alive).
SELECT COALESCE((
    SELECT
        o.deleted_at IS NOT NULL
        OR ($2 IS NOT NULL AND ov.object_id IS NULL)
    FROM objects o
    LEFT JOIN object_versions ov
        ON o.object_id = ov.object_id
        AND ov.object_version = $2
    WHERE o.object_id = $1
), TRUE) AS is_deleted
