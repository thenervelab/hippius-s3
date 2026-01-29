-- Get object info for download with chunks data for a specific version
-- Gateway now handles all permission checks, backend just retrieves data
-- Parameters: $1: bucket_name, $2: object_key, $3: object_version
WITH object_info AS (
    SELECT
        o.object_id,
        o.bucket_id,
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
        ov.object_version as object_version,
        ov.encryption_version,
        ov.enc_suite_id,
        ov.enc_chunk_size_bytes,
        ov.kek_id,
        ov.wrapped_dek
    FROM objects o
    JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = $3
    JOIN buckets b ON o.bucket_id = b.bucket_id
    LEFT JOIN cids c ON ov.cid_id = c.id
    WHERE b.bucket_name = $1
      AND o.object_key = $2
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
    oi.bucket_id,
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
    oi.encryption_version,
    oi.enc_suite_id,
    oi.enc_chunk_size_bytes,
    oi.kek_id,
    oi.wrapped_dek,
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
                WHEN oi.storage_version >= 4 THEN
                    JSON_BUILD_ARRAY(
                        JSON_BUILD_OBJECT(
                            'part_number', 1,
                            'cid', NULL,
                            'size_bytes', oi.size_bytes
                        )
                    )
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
