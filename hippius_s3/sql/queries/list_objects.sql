-- List objects in a bucket with optional prefix and keyset pagination.
-- Parameters: $1: bucket_id, $2: prefix (optional), $3: inclusive cursor / boundary key (optional),
--             $4: limit, $5: exclusive prefix upper bound (successor of prefix, optional)
-- $3 is an INCLUSIVE lower bound: the endpoint computes the boundary (content key + '\x01', or the
-- lexicographic successor of a delimiter common-prefix) so a single >= predicate expresses both
-- "resume after a content key" and "skip the whole collapsed directory group".
-- LATERAL is required so the planner uses idx_objects_bucket_prefix as an ordered range scan
-- and stops after LIMIT rows. The previous correlated-subquery JOIN forced a full hash join over
-- objects + object_versions on large buckets (~5+ min on hyperliquid → asyncpg 30s timeout).
SELECT o.object_id,
       o.object_key,
       ov.size_bytes,
       ov.content_type,
       o.created_at,
       ov.md5_hash,
       ov.status,
       ov.multipart,
       ov.body_blake3
FROM objects o,
     LATERAL (
         -- Skip incomplete multipart placeholders (InitiateMultipartUpload without Complete)
         SELECT v.object_version,
                v.size_bytes,
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
  AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
  AND ($3::text IS NULL OR o.object_key >= $3::text)
  -- LS-2: explicit exclusive upper bound so the (bucket_id, object_key) index range is bounded on
  -- both ends even under a generic prepared plan (a sparse prefix no longer scans to partition end).
  AND ($5::text IS NULL OR o.object_key < $5::text COLLATE "C")
  AND o.deleted_at IS NULL
  -- The key vanishes from the listing when its newest version is a delete marker. This must sit
  -- OUTSIDE the LATERAL: filtering markers inside it would make the subquery fall through to the
  -- previous content version and list a deleted key as though it were still there.
  AND NOT ov.is_delete_marker
-- DB is C-collation; an explicit COLLATE here would defeat the (bucket_id, object_key) index ordered scan.
ORDER BY o.object_key
LIMIT $4::int
