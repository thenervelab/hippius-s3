-- Whether a version holds finished data (the same serveable predicate list_objects uses). An
-- upload in progress never does; UploadPart refuses to write parts into one that does.
-- Parameters: $1: object_id (uuid), $2: object_version (bigint)
SELECT EXISTS (
    SELECT 1
    FROM object_versions ov
    WHERE ov.object_id = $1
      AND ov.object_version = $2
      AND (ov.size_bytes > 0 OR (ov.md5_hash IS NOT NULL AND ov.md5_hash != ''))
)
