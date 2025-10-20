-- Get object info for download with chunks data
-- Parameters: $1: bucket_id, $2: object_key, $3: main_account_id
WITH object_info AS (
    SELECT
        o.object_id,
        o.object_key,
        ov.size_bytes,
        ov.multipart,
        ov.status,
        ov.content_type,
        b.bucket_name,
        b.is_public,
        c.cid as simple_cid,
        ov.object_version as object_version
    FROM objects o
    JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
    JOIN buckets b ON o.bucket_id = b.bucket_id
    LEFT JOIN cids c ON ov.cid_id = c.id
    WHERE o.bucket_id = $1
      AND o.object_key = $2
      AND b.main_account_id = $3
),
multipart_chunks AS (
    SELECT
        p.part_number,
        c.cid,
        p.size_bytes,
        oi.object_id
    FROM object_info oi
    JOIN parts p ON p.object_id = oi.object_id AND p.object_version = oi.object_version
    JOIN cids c ON p.cid_id = c.id
    WHERE oi.multipart = TRUE
    ORDER BY p.part_number ASC
)
SELECT
    oi.object_id,
    oi.object_key,
    oi.size_bytes,
    oi.multipart,
    oi.status,
    oi.content_type,
    oi.bucket_name,
    NOT oi.is_public as should_decrypt,
    (
        SELECT mu.upload_id
        FROM multipart_uploads mu
        WHERE mu.object_id = oi.object_id
        ORDER BY mu.initiated_at DESC
        LIMIT 1
    ) AS upload_id,
    CASE
        WHEN oi.multipart = FALSE THEN
            CASE
                WHEN oi.simple_cid IS NOT NULL THEN
                    JSON_BUILD_ARRAY(
                        JSON_BUILD_OBJECT(
                            'part_number', 1,
                            'cid', oi.simple_cid,
                            'size_bytes', oi.size_bytes
                        )
                    )
                ELSE
                    '[]'::json
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
