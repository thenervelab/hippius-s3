-- ListObjectVersions: every live version and delete marker of every key in a bucket.
--
-- Ordering is load-bearing: AWS returns key ascending, then version descending (newest first)
-- within a key, and the caller derives NextKeyMarker/NextVersionIdMarker from the first row it
-- did NOT return, so the result order must be stable.
--
-- Keyset resume ($3 key marker, $4 version marker) is a two-part cursor because a single key can
-- span a page boundary: rows resume at a strictly later key, or at the same key with a version at
-- or below the version marker (versions descend, so "further into the page" means smaller).
--
-- $7 current_only serves buckets that never enabled versioning: they return one entry per key at
-- current_object_version, matching AWS's shape for an unversioned bucket, rather than exposing
-- the overwrite history we happen to retain.
--
-- The (is_delete_marker OR serveable) predicate mirrors the repo-wide "this version is complete,
-- not a reserved multipart placeholder" rule. A delete marker is zero-size with no md5, so it
-- would fail the serveable half — it has to be admitted explicitly or markers would be invisible.
--
-- Parameters:
--   $1: bucket_id (uuid)
--   $2: prefix (text, optional)
--   $3: key marker (text, optional) — exclusive on the key, inclusive with $4
--   $4: version marker (bigint, optional)
--   $5: limit (int) — caller passes max_keys + 1 to probe truncation
--   $6: exclusive prefix upper bound (text, optional)
--   $7: current_only (boolean)
SELECT o.object_key,
       ov.object_version,
       ov.is_delete_marker,
       ov.size_bytes,
       ov.md5_hash,
       COALESCE(ov.last_modified, ov.created_at) AS last_modified,
       o.current_object_version
FROM objects o
JOIN object_versions ov ON ov.object_id = o.object_id
WHERE o.bucket_id = $1
  AND o.deleted_at IS NULL
  AND ov.deleted_at IS NULL
  AND ov.object_version <= o.current_object_version
  AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
  -- Bounded on both ends so the (bucket_id, object_key) index range cannot scan to partition end.
  AND ($6::text IS NULL OR o.object_key < $6::text COLLATE "C")
  -- Redundant but load-bearing: the precise marker clause below spans BOTH tables in its third
  -- branch, so the planner demotes the whole disjunction to a Filter and every page re-scans the
  -- bucket's key range from the start (measured: 439 pages x ~220k rows on a 438k-key bucket).
  -- This single-table lower bound is implied by that clause, so it changes no results, but it does
  -- give the (bucket_id, object_key) index an Index Cond — measured cost 84206 -> 5769.
  AND ($3::text IS NULL OR o.object_key >= $3::text)
  AND (
        $3::text IS NULL
        OR o.object_key > $3::text
        -- key-marker WITHOUT version-id-marker is EXCLUSIVE per AWS: resume strictly after the
        -- key. Making this branch unconditional would re-emit every version of the marker key,
        -- which loops forever for a client paginating on key-marker alone.
        OR (o.object_key = $3::text AND $4::bigint IS NOT NULL AND ov.object_version <= $4::bigint)
      )
  AND (ov.is_delete_marker OR ov.size_bytes > 0 OR (ov.md5_hash IS NOT NULL AND ov.md5_hash != ''))
  AND (NOT $7::boolean OR ov.object_version = o.current_object_version)
ORDER BY o.object_key, ov.object_version DESC
LIMIT $5::int
