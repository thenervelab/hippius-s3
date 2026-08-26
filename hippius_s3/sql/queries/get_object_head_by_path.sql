-- HD-4/HD-5: lightweight HEAD metadata by (bucket_name, object_key).
-- Returns exactly the fields the HEAD endpoint needs plus the Arion first-chunk hash via LATERAL.
-- Drops the download_chunks JSON_AGG and the mpu upload_id subquery of the download query, and
-- projects append_version so the endpoint's per-request append-version fallback never fires.
-- Uses the SAME incomplete-multipart-placeholder skip predicate as the download query so HEAD
-- resolves the identical version.
-- Parameters: $1: bucket_name, $2: object_key
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
        ov.body_blake3,
        ov.append_version,
        b.bucket_name,
        ov.object_version AS object_version
    FROM objects o
    JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = (
        SELECT v.object_version
        FROM object_versions v
        WHERE v.object_id = o.object_id
          AND v.object_version <= o.current_object_version
          AND (v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
        ORDER BY v.object_version DESC
        LIMIT 1
    )
    JOIN buckets b ON o.bucket_id = b.bucket_id
    WHERE b.bucket_name = $1
      AND b.deleted_at IS NULL
      AND o.deleted_at IS NULL
      AND o.object_id = resolve_object_id(b.bucket_id, $2)
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
    oi.body_blake3,
    oi.append_version,
    oi.bucket_name,
    oi.object_version,
    arion.backend_identifier AS arion_file_hash
FROM object_info oi
LEFT JOIN LATERAL (
    SELECT cb.backend_identifier
    FROM chunk_backend cb
    JOIN part_chunks pc ON pc.id = cb.chunk_id
    JOIN parts p ON pc.part_id = p.part_id
    WHERE cb.backend = 'arion'
      AND p.object_id = oi.object_id
      AND p.object_version = oi.object_version
      AND p.part_number = 1
      AND pc.chunk_index = 0
      AND NOT cb.deleted
      AND cb.backend_identifier IS NOT NULL
    LIMIT 1
) arion ON TRUE;
