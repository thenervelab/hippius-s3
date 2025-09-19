-- migrate:up

-- Backfill part 0 for simple objects so that all objects are represented by parts
-- Criteria:
--  - Object has no parts rows yet
--  - Object has a concrete CID (cid_id or ipfs_cid not pending/empty)

WITH simple_objects AS (
    SELECT o.object_id,
           o.bucket_id,
           o.object_key,
           o.size_bytes,
           o.md5_hash,
           COALESCE(c.cid, o.ipfs_cid) AS base_cid
    FROM objects o
    LEFT JOIN cids c ON o.cid_id = c.id
    WHERE NOT EXISTS (
        SELECT 1 FROM parts p WHERE p.object_id = o.object_id
    )
      AND COALESCE(NULLIF(TRIM(COALESCE(c.cid, o.ipfs_cid)), ''), 'pending') <> 'pending'
)
INSERT INTO parts (part_id, upload_id, part_number, ipfs_cid, size_bytes, etag, uploaded_at, object_id, cid_id)
SELECT gen_random_uuid() AS part_id,
       mu.upload_id,
       0 AS part_number,
       so.base_cid AS ipfs_cid,
        so.size_bytes::bigint AS size_bytes,
       so.md5_hash AS etag,
       NOW() AT TIME ZONE 'UTC' AS uploaded_at,
       so.object_id,
       c.id AS cid_id
FROM simple_objects so
JOIN LATERAL (
    -- Ensure a multipart_uploads row exists per object (idempotent)
    INSERT INTO multipart_uploads (upload_id, bucket_id, object_key, initiated_at, is_completed, content_type, metadata)
    VALUES (gen_random_uuid(), so.bucket_id, so.object_key, NOW() AT TIME ZONE 'UTC', TRUE, NULL, '{}'::jsonb)
    ON CONFLICT DO NOTHING
    RETURNING upload_id
) mu ON TRUE
JOIN cids c ON c.cid = so.base_cid;

-- migrate:down
-- This down migration deletes only part 0 rows for objects that still have no other parts
DELETE FROM parts p
USING (
    SELECT object_id
    FROM parts
    GROUP BY object_id
    HAVING COUNT(*) = 1
) solo
WHERE p.object_id = solo.object_id
  AND p.part_number = 0;
