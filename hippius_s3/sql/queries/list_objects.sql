-- List objects in a bucket with optional prefix and keyset pagination.
-- Parameters: $1: bucket_id, $2: prefix (optional), $3: inclusive cursor / boundary key (optional),
--             $4: limit, $5: exclusive prefix upper bound (successor of prefix, optional)
-- $3 is an INCLUSIVE lower bound: the endpoint computes the boundary (content key + '\x01', or the
-- lexicographic successor of a delimiter common-prefix) so a single >= predicate expresses both
-- "resume after a content key" and "skip the whole collapsed directory group".
-- LATERAL is required so the planner uses idx_objects_bucket_prefix as an ordered range scan
-- and stops after LIMIT rows. The previous correlated-subquery JOIN forced a full hash join over
-- objects + object_versions on large buckets (~5+ min on hyperliquid → asyncpg 30s timeout).
--
-- LS-3: each arm carries its own ORDER BY / LIMIT $4. Nothing can stop an ordinary
-- `UNION ALL ... ORDER BY ... LIMIT` early — the outer limit sits above an Append, so the planner
-- has to produce the whole of both arms first. Measured on a bucket with ~1.4M keys left after the
-- cursor: 13,581 ms and 801k buffer reads for one 1001-row page, against 9.3 ms for the same page
-- when only the `objects` arm was present and the index scan could stop at the limit.
-- Limiting each arm is exact rather than an approximation: a row in the merged top-N has at most
-- N-1 rows before it in the union, hence at most N-1 within its own arm, so it is in that arm's
-- top-N too.
-- That holds only if the limit sits below every row-eliminating predicate, which is why the LATERAL
-- and the delete-marker filter are repeated per arm instead of being applied once above the union:
-- a row dropped after an arm's LIMIT would shorten the batch, and the endpoint reads a batch
-- shorter than the limit as "the bucket has no more keys".
SELECT o.object_id,
       o.object_key,
       o.size_bytes,
       o.content_type,
       o.created_at,
       o.md5_hash,
       o.status,
       o.multipart,
       o.body_blake3
FROM (
    (
        SELECT o.object_id,
               o.object_key,
               o.created_at,
               ov.size_bytes,
               ov.content_type,
               ov.md5_hash,
               ov.status,
               ov.multipart,
               ov.body_blake3
        FROM objects o
        CROSS JOIN LATERAL (
            -- Skip incomplete multipart placeholders (InitiateMultipartUpload without Complete)
            SELECT v.size_bytes,
                   v.content_type,
                   v.md5_hash,
                   v.status,
                   v.multipart,
                   v.is_delete_marker,
                   v.body_blake3
            FROM object_versions v
            WHERE v.object_id = o.object_id
              AND v.object_version <= o.current_object_version
              AND v.deleted_at IS NULL
              -- A delete marker is zero-size with no md5, so it fails the serveable half of this
              -- predicate. Admit it explicitly, or resolution silently falls back to the previous
              -- content version and serves deleted data.
              AND (v.is_delete_marker OR v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
            ORDER BY v.object_version DESC
            LIMIT 1
        ) ov
        WHERE o.bucket_id = $1
          AND o.deleted_at IS NULL
          AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
          AND ($3::text IS NULL OR o.object_key >= $3::text)
          -- LS-2: explicit exclusive upper bound so the (bucket_id, object_key) index range is
          -- bounded on both ends even under a generic prepared plan (a sparse prefix no longer
          -- scans to partition end).
          AND ($5::text IS NULL OR o.object_key < $5::text COLLATE "C")
          -- The key vanishes from the listing when its newest version is a delete marker. This must
          -- sit OUTSIDE the LATERAL: filtering markers inside it would make the subquery fall
          -- through to the previous content version and list a deleted key as though it were still
          -- there.
          AND NOT ov.is_delete_marker
        -- DB is C-collation; an explicit COLLATE here would defeat the (bucket_id, object_key)
        -- index ordered scan, which is the whole reason this arm can stop at the limit.
        ORDER BY o.object_key
        LIMIT $4::int
    )
    UNION ALL
    (
        -- Same-bucket CopyObject cannot mint a new object_id (the v5 AAD binds bucket_id+object_id),
        -- so it attaches a second name here against the same id. Without this arm every copied key
        -- is invisible to ListObjects.
        SELECT o.object_id,
               n.object_key,
               n.created_at,
               ov.size_bytes,
               ov.content_type,
               ov.md5_hash,
               ov.status,
               ov.multipart,
               ov.body_blake3
        FROM object_names n
        JOIN objects o ON o.object_id = n.object_id AND o.deleted_at IS NULL
        CROSS JOIN LATERAL (
            SELECT v.size_bytes,
                   v.content_type,
                   v.md5_hash,
                   v.status,
                   v.multipart,
                   v.is_delete_marker,
                   v.body_blake3
            FROM object_versions v
            WHERE v.object_id = o.object_id
              AND v.object_version <= o.current_object_version
              AND v.deleted_at IS NULL
              AND (v.is_delete_marker OR v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
            ORDER BY v.object_version DESC
            LIMIT 1
        ) ov
        WHERE n.bucket_id = $1
          AND ($2::text IS NULL OR n.object_key LIKE $2::text || '%')
          AND ($3::text IS NULL OR n.object_key >= $3::text)
          AND ($5::text IS NULL OR n.object_key < $5::text COLLATE "C")
          -- A delete marker hides EVERY name of the object, primary and alias alike, because the
          -- marker lives on the shared object_id. That is intended: a marker is the object being
          -- deleted, whereas deleting one alias only drops that name (see drop_s3_name).
          AND NOT ov.is_delete_marker
        ORDER BY n.object_key
        LIMIT $4::int
    )
) o
-- At most 2 x $4 rows reach here, so this merge is a small in-memory sort, not a scan.
ORDER BY o.object_key
LIMIT $4::int
