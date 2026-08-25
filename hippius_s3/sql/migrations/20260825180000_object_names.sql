-- migrate:up
--
-- Second S3 names for one object_id. Ciphertext AAD binds bucket_id+object_id, so a
-- same-bucket CopyObject cannot allocate a new object_id (the disabled v5 CID-reuse
-- path). Harbor's blob commit is CopyObject + DeleteObject of _uploads/... ->
-- blobs/sha256/...; an alias makes that metadata-only.
--
-- objects.object_key remains the primary name. object_names holds extra keys.
-- resolve_object_id prefers the primary name when both would match (they should not).

CREATE TABLE IF NOT EXISTS object_names (
    bucket_id uuid NOT NULL REFERENCES buckets (bucket_id) ON DELETE CASCADE,
    object_key text NOT NULL,
    object_id uuid NOT NULL REFERENCES objects (object_id) ON DELETE CASCADE,
    created_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (bucket_id, object_key)
);

CREATE INDEX IF NOT EXISTS object_names_object_id_idx
    ON object_names (object_id);

CREATE OR REPLACE FUNCTION resolve_object_id(p_bucket_id uuid, p_object_key text)
RETURNS uuid
LANGUAGE sql
STABLE
AS $$
    SELECT object_id
    FROM (
        SELECT o.object_id, 0 AS pri
        FROM objects o
        WHERE o.bucket_id = p_bucket_id
          AND o.object_key = p_object_key
          AND o.deleted_at IS NULL
        UNION ALL
        SELECT n.object_id, 1 AS pri
        FROM object_names n
        INNER JOIN objects o
            ON o.object_id = n.object_id
           AND o.deleted_at IS NULL
        WHERE n.bucket_id = p_bucket_id
          AND n.object_key = p_object_key
    ) hits
    ORDER BY pri
    LIMIT 1
$$;

-- migrate:down

DROP FUNCTION IF EXISTS resolve_object_id(uuid, text);
DROP TABLE IF EXISTS object_names;
