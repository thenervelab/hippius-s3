-- $1 bucket_id, $2 primary object_key being removed
-- If the object has an alias, rename the primary key to that alias and drop the alias.
-- Returns object_id when a name was promoted; no row when the caller should soft-delete.
WITH src AS (
    SELECT object_id
    FROM objects
    WHERE bucket_id = $1
      AND object_key = $2
      AND deleted_at IS NULL
),
picked AS (
    SELECT n.object_key, n.object_id
    FROM object_names n
    JOIN src ON src.object_id = n.object_id
    WHERE n.bucket_id = $1
    ORDER BY n.object_key
    LIMIT 1
),
upd AS (
    UPDATE objects o
    SET object_key = picked.object_key
    FROM picked
    WHERE o.object_id = picked.object_id
    RETURNING o.object_id
),
del AS (
    DELETE FROM object_names n
    USING picked
    WHERE n.bucket_id = $1
      AND n.object_key = picked.object_key
    RETURNING n.object_id
)
SELECT object_id FROM upd
