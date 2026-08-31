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
-- KEYS COME FROM TWO PLACES, assembled exactly as list_objects.sql assembles them. Same-bucket
-- CopyObject cannot mint a new object_id (the v5 AAD binds bucket_id+object_id), so it attaches a
-- second name in `object_names` against the same id. Sourcing keys from `objects` alone made every
-- copied key invisible here while ListObjects still returned it: the two listings disagreed about
-- which keys exist in one bucket, and a client enumerating versions to prune never saw the copies.
--
-- LS-3: each arm is a complete listing of its own — every predicate, then its own ORDER BY and
-- LIMIT $5 — and only the already-limited arms are merged. Nothing can stop an ordinary
-- `UNION ALL ... ORDER BY ... LIMIT` early, so the outer limit above an Append forces both arms to
-- be produced in full; on a large bucket that is the whole remaining key range per page (measured
-- on the ListObjects twin: 13,581 ms and 801k buffer reads for one 1001-row page, versus 9.3 ms
-- for a single arm that could stop at the limit). Per-arm limiting is exact, not an approximation:
-- a row in the merged top-N has at most N-1 rows before it in the union, hence at most N-1 within
-- its own arm. It is only sound below every row-eliminating predicate — a row dropped after an
-- arm's LIMIT would shorten the batch, and the caller reads a batch shorter than the limit as
-- "there is nothing after this page".
-- Repeating the prefix and marker bounds inside each arm is also what keeps each arm's
-- (bucket_id, object_key) index range bounded — see the marker note below for what that costs.
--
-- A delete marker lives on the shared object_id, so it lists under every name of the object,
-- primary and alias alike. That matches what the read and list paths already do: the marker is the
-- OBJECT being deleted, so it hides — and here, describes — all of its names.
--
-- Parameters:
--   $1: bucket_id (uuid)
--   $2: prefix (text, optional)
--   $3: key marker (text, optional) — exclusive on the key, inclusive with $4
--   $4: version marker (bigint, optional)
--   $5: limit (int) — caller passes max_keys + 1 to probe truncation
--   $6: exclusive prefix upper bound (text, optional)
--   $7: current_only (boolean)
SELECT v.object_key,
       v.object_version,
       v.is_delete_marker,
       v.size_bytes,
       v.md5_hash,
       v.body_blake3,
       v.last_modified,
       v.current_object_version
FROM (
    (
        SELECT o.object_key,
               ov.object_version,
               ov.is_delete_marker,
               ov.size_bytes,
               ov.md5_hash,
               ov.body_blake3,
               COALESCE(ov.last_modified, ov.created_at) AS last_modified,
               o.current_object_version
        FROM objects o
        JOIN object_versions ov ON ov.object_id = o.object_id
        WHERE o.bucket_id = $1
          AND o.deleted_at IS NULL
          AND ov.deleted_at IS NULL
          AND ov.object_version <= o.current_object_version
          AND ($2::text IS NULL OR o.object_key LIKE $2::text || '%')
          -- Bounded on both ends so the (bucket_id, object_key) index range cannot scan to
          -- partition end.
          AND ($6::text IS NULL OR o.object_key < $6::text COLLATE "C")
          -- Redundant but load-bearing: the precise marker clause below spans BOTH tables in its
          -- third branch, so the planner demotes the whole disjunction to a Filter and every page
          -- re-scans the bucket's key range from the start (measured: 439 pages x ~220k rows on a
          -- 438k-key bucket). This single-table lower bound is implied by that clause, so it
          -- changes no results, but it does give the (bucket_id, object_key) index an Index Cond —
          -- measured cost 84206 -> 5769.
          AND ($3::text IS NULL OR o.object_key >= $3::text)
          AND (
                $3::text IS NULL
                OR o.object_key > $3::text
                -- key-marker WITHOUT version-id-marker is EXCLUSIVE per AWS: resume strictly after
                -- the key. Making this branch unconditional would re-emit every version of the
                -- marker key, which loops forever for a client paginating on key-marker alone.
                OR (o.object_key = $3::text AND $4::bigint IS NOT NULL AND ov.object_version <= $4::bigint)
              )
          AND (ov.is_delete_marker OR ov.size_bytes > 0 OR (ov.md5_hash IS NOT NULL AND ov.md5_hash != ''))
          AND (NOT $7::boolean OR ov.object_version = o.current_object_version)
        ORDER BY o.object_key, ov.object_version DESC
        LIMIT $5::int
    )
    UNION ALL
    (
        SELECT n.object_key,
               ov.object_version,
               ov.is_delete_marker,
               ov.size_bytes,
               ov.md5_hash,
               ov.body_blake3,
               COALESCE(ov.last_modified, ov.created_at) AS last_modified,
               o.current_object_version
        FROM object_names n
        JOIN objects o ON o.object_id = n.object_id AND o.deleted_at IS NULL
        JOIN object_versions ov ON ov.object_id = o.object_id
        WHERE n.bucket_id = $1
          AND ov.deleted_at IS NULL
          AND ov.object_version <= o.current_object_version
          AND ($2::text IS NULL OR n.object_key LIKE $2::text || '%')
          AND ($6::text IS NULL OR n.object_key < $6::text COLLATE "C")
          AND ($3::text IS NULL OR n.object_key >= $3::text)
          AND (
                $3::text IS NULL
                OR n.object_key > $3::text
                OR (n.object_key = $3::text AND $4::bigint IS NOT NULL AND ov.object_version <= $4::bigint)
              )
          AND (ov.is_delete_marker OR ov.size_bytes > 0 OR (ov.md5_hash IS NOT NULL AND ov.md5_hash != ''))
          AND (NOT $7::boolean OR ov.object_version = o.current_object_version)
        ORDER BY n.object_key, ov.object_version DESC
        LIMIT $5::int
    )
) v
-- At most 2 x $5 rows reach here, so this merge is a small in-memory sort, not a scan.
ORDER BY v.object_key, v.object_version DESC
LIMIT $5::int
