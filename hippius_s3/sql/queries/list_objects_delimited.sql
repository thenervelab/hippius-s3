-- LS-1: delimiter rollup as a loose-index skip-scan, entirely in SQL.
-- Each recursive step fetches the next serveable key with a SCALAR subquery (reliably correlated to
-- the prior row's next_boundary), classifies it (content vs delimiter group), and advances: a content
-- key to key || chr(1); a group to the lexicographic successor of its common prefix. `kept` counts
-- only non-suppressed items so cp_floor-suppressed groups still advance the scan without consuming a
-- slot; recursion stops once kept reaches target+1. Mirrors Python _collect_page exactly — hence
-- gated behind HIPPIUS_LIST_OBJECTS_SQL_ROLLUP with a differential test proving equivalence.
--
-- Parameters: $1 bucket_id (uuid), $2 prefix (text|null), $3 cursor (text|null, inclusive lower
--             bound), $4 delimiter (text|null), $5 target (int, max-keys), $6 cp_floor (text|null)
WITH RECURSIVE p AS (
    SELECT
        $1::uuid AS bucket_id,
        $2::text AS prefix,
        $4::text AS delim,
        $6::text AS cp_floor,
        COALESCE(length($2::text), 0) AS plen,
        COALESCE(length($4::text), 0) AS dlen,
        $5::int AS target
),
walk AS (
    -- SEED: the first serveable object at or after the cursor.
    SELECT
        s.object_key, c.is_prefix, c.group_key, c.next_boundary, c.suppressed,
        (CASE WHEN c.suppressed THEN 0 ELSE 1 END)::int AS kept
    FROM p
    CROSS JOIN LATERAL (
        SELECT (
            SELECT k.object_key
            FROM (
                SELECT o.object_key, o.object_id, o.current_object_version
                FROM objects o
                WHERE o.bucket_id = p.bucket_id
                  AND o.deleted_at IS NULL
                  AND (p.prefix IS NULL OR o.object_key LIKE p.prefix || '%')
                  AND ($3::text IS NULL OR o.object_key >= $3::text)
                UNION ALL
                SELECT n.object_key, o.object_id, o.current_object_version
                FROM object_names n
                JOIN objects o ON o.object_id = n.object_id AND o.deleted_at IS NULL
                WHERE n.bucket_id = p.bucket_id
                  AND (p.prefix IS NULL OR n.object_key LIKE p.prefix || '%')
                  AND ($3::text IS NULL OR n.object_key >= $3::text)
            ) k
            WHERE (p.prefix IS NULL OR k.object_key LIKE p.prefix || '%')
              AND ($3::text IS NULL OR k.object_key >= $3::text)
              -- The key is listed only when its NEWEST admitted version is real content. A delete
              -- marker is zero-size with no md5, so it fails the "serveable" half of the predicate
              -- and has to be admitted explicitly — otherwise resolution falls through to the
              -- previous content version and lists a deleted key as though it were still there.
              -- NULL (no admitted version at all) coalesces to TRUE so the key is skipped, which
              -- preserves the EXISTS semantics this replaced.
              AND NOT COALESCE((
                  SELECT v.is_delete_marker
                  FROM object_versions v
                  WHERE v.object_id = k.object_id
                    AND v.object_version <= k.current_object_version
                    AND v.deleted_at IS NULL
                    AND (v.is_delete_marker OR v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
                  ORDER BY v.object_version DESC
                  LIMIT 1
              ), TRUE)
            ORDER BY k.object_key
            LIMIT 1
        ) AS object_key
    ) s
    CROSS JOIN LATERAL (
        SELECT
            (p.dlen > 0 AND strpos(substr(s.object_key, p.plen + 1), p.delim) > 0) AS is_prefix,
            CASE WHEN (p.dlen > 0 AND strpos(substr(s.object_key, p.plen + 1), p.delim) > 0)
                 THEN left(s.object_key, p.plen + strpos(substr(s.object_key, p.plen + 1), p.delim) - 1 + p.dlen)
                 END AS cpref
    ) cp
    CROSS JOIN LATERAL (
        SELECT
            cp.is_prefix,
            COALESCE(cp.cpref, s.object_key) AS group_key,
            CASE WHEN cp.is_prefix
                 THEN substr(cp.cpref, 1, length(cp.cpref) - 1) || chr(ascii(right(cp.cpref, 1)) + 1)
                 ELSE s.object_key || chr(1) END AS next_boundary,
            (cp.is_prefix AND p.cp_floor IS NOT NULL AND cp.cpref <= p.cp_floor) AS suppressed
    ) c
    WHERE s.object_key IS NOT NULL

    UNION ALL

    -- STEP: from the prior row's next_boundary, seek the next serveable object.
    SELECT
        s.object_key, c.is_prefix, c.group_key, c.next_boundary, c.suppressed,
        (w.kept + CASE WHEN c.suppressed THEN 0 ELSE 1 END)::int AS kept
    FROM walk w
    CROSS JOIN p
    CROSS JOIN LATERAL (
        SELECT (
            SELECT k.object_key
            FROM (
                SELECT o.object_key, o.object_id, o.current_object_version
                FROM objects o
                WHERE o.bucket_id = p.bucket_id
                  AND o.deleted_at IS NULL
                  AND (p.prefix IS NULL OR o.object_key LIKE p.prefix || '%')
                  AND o.object_key >= w.next_boundary
                UNION ALL
                SELECT n.object_key, o.object_id, o.current_object_version
                FROM object_names n
                JOIN objects o ON o.object_id = n.object_id AND o.deleted_at IS NULL
                WHERE n.bucket_id = p.bucket_id
                  AND (p.prefix IS NULL OR n.object_key LIKE p.prefix || '%')
                  AND n.object_key >= w.next_boundary
            ) k
            WHERE (p.prefix IS NULL OR k.object_key LIKE p.prefix || '%')
              AND k.object_key >= w.next_boundary
              -- The key is listed only when its NEWEST admitted version is real content. A delete
              -- marker is zero-size with no md5, so it fails the "serveable" half of the predicate
              -- and has to be admitted explicitly — otherwise resolution falls through to the
              -- previous content version and lists a deleted key as though it were still there.
              -- NULL (no admitted version at all) coalesces to TRUE so the key is skipped, which
              -- preserves the EXISTS semantics this replaced.
              AND NOT COALESCE((
                  SELECT v.is_delete_marker
                  FROM object_versions v
                  WHERE v.object_id = k.object_id
                    AND v.object_version <= k.current_object_version
                    AND v.deleted_at IS NULL
                    AND (v.is_delete_marker OR v.size_bytes > 0 OR (v.md5_hash IS NOT NULL AND v.md5_hash != ''))
                  ORDER BY v.object_version DESC
                  LIMIT 1
              ), TRUE)
            ORDER BY k.object_key
            LIMIT 1
        ) AS object_key
    ) s
    CROSS JOIN LATERAL (
        SELECT
            (p.dlen > 0 AND strpos(substr(s.object_key, p.plen + 1), p.delim) > 0) AS is_prefix,
            CASE WHEN (p.dlen > 0 AND strpos(substr(s.object_key, p.plen + 1), p.delim) > 0)
                 THEN left(s.object_key, p.plen + strpos(substr(s.object_key, p.plen + 1), p.delim) - 1 + p.dlen)
                 END AS cpref
    ) cp
    CROSS JOIN LATERAL (
        SELECT
            cp.is_prefix,
            COALESCE(cp.cpref, s.object_key) AS group_key,
            CASE WHEN cp.is_prefix
                 THEN substr(cp.cpref, 1, length(cp.cpref) - 1) || chr(ascii(right(cp.cpref, 1)) + 1)
                 ELSE s.object_key || chr(1) END AS next_boundary,
            (cp.is_prefix AND p.cp_floor IS NOT NULL AND cp.cpref <= p.cp_floor) AS suppressed
    ) c
    WHERE w.kept <= p.target AND s.object_key IS NOT NULL
)
SELECT
    w.is_prefix,
    w.group_key,
    w.object_key,
    m.size_bytes,
    m.md5_hash,
    m.created_at,
    m.body_blake3,
    w.next_boundary
FROM walk w
LEFT JOIN LATERAL (
    SELECT o.created_at, ov.size_bytes, ov.md5_hash, ov.body_blake3
    FROM objects o
    CROSS JOIN LATERAL (
        SELECT v.size_bytes, v.md5_hash, v.body_blake3
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
    WHERE o.bucket_id = (SELECT bucket_id FROM p)
      AND o.deleted_at IS NULL
      AND o.object_id = resolve_object_id((SELECT bucket_id FROM p), w.object_key)
) m ON NOT w.is_prefix
WHERE NOT w.suppressed
ORDER BY w.kept
LIMIT $5::int + 1;
