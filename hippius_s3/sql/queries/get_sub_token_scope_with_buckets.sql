SELECT
    s.access_key_id,
    s.account_id,
    s.permission,
    s.bucket_scope,
    s.bucket_ids,
    COALESCE(
        array_agg(b.bucket_name ORDER BY b.bucket_name) FILTER (WHERE b.bucket_name IS NOT NULL),
        ARRAY[]::varchar[]
    ) AS live_bucket_names,
    COALESCE(
        array_agg(missing_id::text ORDER BY missing_id::text) FILTER (
            WHERE missing_id IS NOT NULL AND NOT EXISTS (SELECT 1 FROM buckets WHERE bucket_id = missing_id)
        ),
        ARRAY[]::text[]
    ) AS stale_bucket_ids
FROM sub_token_scopes s
LEFT JOIN LATERAL unnest(s.bucket_ids) AS missing_id ON TRUE
LEFT JOIN buckets b ON b.bucket_id = missing_id
WHERE s.access_key_id = $1
GROUP BY s.access_key_id, s.account_id, s.permission, s.bucket_scope, s.bucket_ids
