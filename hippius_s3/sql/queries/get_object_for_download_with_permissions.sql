-- Get object info for download with chunks data, checking permissions
-- Parameters: $1: bucket_name, $2: object_key, $3: main_account_id
WITH object_info AS (
    SELECT
        o.object_id,
        o.object_key,
        o.size_bytes,
        o.multipart,
        o.status,
        o.content_type,
        o.metadata,
        o.created_at,
        o.md5_hash,
        o.append_version,
        b.bucket_name,
        b.is_public,
        b.main_account_id as bucket_owner_id,
        c.cid as simple_cid,
        o.storage_version
    FROM objects o
    JOIN buckets b ON o.bucket_id = b.bucket_id
    LEFT JOIN cids c ON o.cid_id = c.id
    WHERE b.bucket_name = $1
      AND o.object_key = $2
      AND (b.is_public = TRUE OR b.main_account_id = $3)
),
multipart_chunks AS (
    SELECT
        p.part_number,
        c.cid,
        p.size_bytes,
        oi.object_id
    FROM object_info oi
    JOIN parts p ON p.object_id = oi.object_id
    LEFT JOIN cids c ON p.cid_id = c.id
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
    oi.metadata,
    oi.created_at,
    oi.md5_hash,
    oi.bucket_name,
    oi.simple_cid,
    oi.storage_version,
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
