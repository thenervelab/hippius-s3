-- migrate:up

-- Note: avoid extensions; use md5(random()||clock_timestamp())::uuid for UUIDs

-- Backfill part 0 for simple objects so that all objects are represented by parts
-- Criteria:
--  - Object has no parts rows yet
--  - Object has a concrete CID (cid_id or ipfs_cid not pending/empty)
-- Seed missing base CIDs into cids table (idempotent)
WITH simple_objects AS (
    SELECT o.object_id,
           o.bucket_id,
           o.object_key,
           o.size_bytes,
           o.md5_hash,
           COALESCE(c.cid, o.ipfs_cid) AS base_cid
    FROM objects o
    LEFT JOIN cids c ON o.cid_id = c.id
    WHERE NOT EXISTS (SELECT 1 FROM parts p WHERE p.object_id = o.object_id)
      AND COALESCE(NULLIF(TRIM(COALESCE(c.cid, o.ipfs_cid)), ''), 'pending') <> 'pending'
)
INSERT INTO cids (id, cid)
SELECT md5(random()::text || clock_timestamp()::text)::uuid, so.base_cid
FROM (
    SELECT DISTINCT base_cid
    FROM simple_objects
) so
ON CONFLICT (cid) DO NOTHING;

-- Ensure a multipart_uploads row exists for each simple object (idempotent by object_id)
WITH simple_objects AS (
    SELECT o.object_id,
           o.bucket_id,
           o.object_key,
           o.size_bytes,
           o.md5_hash,
           COALESCE(c.cid, o.ipfs_cid) AS base_cid
    FROM objects o
    LEFT JOIN cids c ON o.cid_id = c.id
    WHERE NOT EXISTS (SELECT 1 FROM parts p WHERE p.object_id = o.object_id)
      AND COALESCE(NULLIF(TRIM(COALESCE(c.cid, o.ipfs_cid)), ''), 'pending') <> 'pending'
)
INSERT INTO multipart_uploads (upload_id, bucket_id, object_key, initiated_at, is_completed, content_type, metadata, object_id)
SELECT md5(random()::text || clock_timestamp()::text)::uuid, so.bucket_id, so.object_key, NOW() AT TIME ZONE 'UTC', TRUE, NULL, '{}'::jsonb, so.object_id
FROM simple_objects so
WHERE NOT EXISTS (SELECT 1 FROM multipart_uploads mu WHERE mu.object_id = so.object_id);

-- Now insert part 0 for each simple object using the upload row
WITH simple_objects AS (
    SELECT o.object_id,
           o.bucket_id,
           o.object_key,
           o.size_bytes,
           o.md5_hash,
           COALESCE(c.cid, o.ipfs_cid) AS base_cid
    FROM objects o
    LEFT JOIN cids c ON o.cid_id = c.id
    WHERE NOT EXISTS (SELECT 1 FROM parts p WHERE p.object_id = o.object_id)
      AND COALESCE(NULLIF(TRIM(COALESCE(c.cid, o.ipfs_cid)), ''), 'pending') <> 'pending'
)
INSERT INTO parts (part_id, upload_id, part_number, ipfs_cid, size_bytes, etag, uploaded_at, object_id, cid_id)
SELECT md5(random()::text || clock_timestamp()::text)::uuid, mu.upload_id, 0, so.base_cid, so.size_bytes::bigint, so.md5_hash, NOW() AT TIME ZONE 'UTC', so.object_id, c.id
FROM simple_objects so
JOIN multipart_uploads mu ON mu.object_id = so.object_id
JOIN cids c ON c.cid = so.base_cid
ON CONFLICT (upload_id, part_number) DO NOTHING;

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
