-- Get object info for download with chunks data, checking permissions
-- Parameters: $1: bucket_name, $2: object_key, $3: main_account_id
WITH object_info AS (
    SELECT
        o.object_id,
        o.object_key,
        ov.size_bytes,
        ov.multipart,
        ov.status,
        ov.content_type,
        ov.metadata,
        o.created_at,
        ov.md5_hash,
        ov.append_version,
        b.bucket_name,
        b.is_public,
        b.main_account_id as bucket_owner_id,
        c.cid as simple_cid,
        ov.storage_version,
        ov.object_version as object_version
    FROM objects o
    JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = o.current_object_version
    JOIN buckets b ON o.bucket_id = b.bucket_id
    LEFT JOIN cids c ON ov.cid_id = c.id
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
    JOIN parts p ON p.object_id = oi.object_id AND p.object_version = oi.object_version
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
    oi.is_public,
    oi.bucket_owner_id,
    oi.object_version,
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
