-- Get complete download information for an object by object_id (handles both simple and multipart)
-- Parameters: $1: object_id
WITH object_info AS (
    SELECT
        o.object_id,
        ov.multipart,
        ov.cid_id as simple_cid_id,
        b.is_public,
        c.cid as simple_cid,
        ov.storage_version,
        ov.object_version as object_version
    FROM objects o
    JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
    JOIN buckets b ON o.bucket_id = b.bucket_id
    LEFT JOIN cids c ON ov.cid_id = c.id
    WHERE o.object_id = $1
),
multipart_chunks AS (
    -- CIDs are optional: allow cid_id NULL by falling back to parts.ipfs_cid; may still be NULL
    -- for CID-less (v4+) objects.
    SELECT
        p.part_number,
        COALESCE(c.cid, p.ipfs_cid) AS cid,
        p.size_bytes,
        oi.object_id
    FROM object_info oi
    JOIN parts p ON p.object_id = oi.object_id AND p.object_version = oi.object_version
    LEFT JOIN cids c ON p.cid_id = c.id
    WHERE oi.multipart = TRUE
)
SELECT
    oi.object_id,
    oi.multipart,
    oi.object_version,
    oi.storage_version,
    NOT oi.is_public as needs_decryption,
    CASE
        WHEN oi.multipart = FALSE THEN
            CASE
                WHEN oi.storage_version >= 4 THEN
                    JSON_BUILD_ARRAY(
                        JSON_BUILD_OBJECT(
                            'part_number', 1,
                            'cid', NULL,
                            'size_bytes', NULL
                        )
                    )
                WHEN oi.simple_cid IS NULL THEN
                    '[]'::json
                ELSE
                    JSON_BUILD_ARRAY(
                        JSON_BUILD_OBJECT(
                            'part_number', 1,
                            'cid', oi.simple_cid,
                            'size_bytes', NULL
                        )
                    )
            END
        ELSE
            COALESCE(
                (SELECT JSON_AGG(
                    JSON_BUILD_OBJECT(
                        'part_number', mc.part_number,
                        'cid', mc.cid,
                        'size_bytes', mc.size_bytes
                    ) ORDER BY mc.part_number
                ) FROM multipart_chunks mc WHERE mc.object_id = oi.object_id),
                '[]'::json
            )
    END as download_chunks
FROM object_info oi;
