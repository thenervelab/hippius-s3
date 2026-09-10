-- One keyset page of a bucket's billable bytes. Step 2 of the chunked storage count.
--
-- WHY THIS EXISTS. The single-statement form (get_account_storage_bytes.sql) cannot finish for a
-- large bucket: one account's objects are concentrated in a single bucket holding a filesystem-style
-- workload of millions of small objects, measured at over a minute cold. The plans-cacher's asyncpg
-- timeout is 30s AND the replica's max_standby_streaming_delay is 30s, so it was cancelled every
-- cycle -- and because a single account's failure fails the whole cycle, the roll was never
-- published at all.
--
-- Chunking does NOT make the total work smaller; it makes no single STATEMENT long enough to hit
-- either 30s ceiling. Same bytes, same definition, spread over N round trips. It also stops the
-- count pinning the xmin horizon for a minute at a time.
--
-- Keyset, not OFFSET: objects has a UNIQUE (bucket_id, object_key), so object_key is a total order
-- within a bucket with no ties, and `> $2` resumes exactly where the last page stopped. OFFSET
-- would re-walk the prefix on every page and turn this quadratic.
--
-- PRECONDITION: object_key is never the empty string. The walk starts at `> ''`, so a '' key would
-- be skipped on every page forever -- a silent UNDER-count of a billed number, which is why it is
-- called out rather than left implicit. It holds structurally (a keyless S3 path is a BUCKET
-- operation, never PutObject) and was confirmed on prod: zero rows with object_key = ''. There is
-- no CHECK constraint enforcing it. If one is ever added, this is the query that depends on it;
-- if empty keys ever become reachable, this walk needs a NULL-cursor first page instead.
--
-- SNAPSHOT: the single-statement form ran in ONE MVCC snapshot; this runs one per page. An object
-- written into a key range the walk has already passed is missed until the next cycle, and one
-- written ahead of the cursor is included. That is a per-cycle skew of at most the writes landing
-- during the walk, on a number that is already a 10-minute-old estimate by design -- see the
-- enforcement-lag warning in workers/CLAUDE.md. It is not a new class of staleness, only slightly
-- more of it.
--
-- rows_seen counts PAGE rows, NOT joined rows. An object whose current version is soft-deleted or
-- is a delete marker contributes no row to the join, so counting joined rows would under-report the
-- page as short and stop the walk early -- silently under-counting the account. That distinction is
-- the whole reason the aggregates are separate subqueries over `page` rather than one join.
--
-- Parameters: $1: bucket_id (uuid), $2: object_key cursor (exclusive; '' starts), $3: page size
WITH page AS (
    SELECT o.object_id, o.object_key, o.current_object_version
    FROM objects o
    WHERE o.bucket_id = $1
      AND o.deleted_at IS NULL
      AND o.object_key > $2
    ORDER BY o.object_key
    LIMIT $3
)
SELECT
    (SELECT count(*) FROM page)::bigint AS rows_seen,
    (SELECT max(object_key) FROM page) AS last_key,
    COALESCE((
        SELECT SUM(ov.size_bytes)
        FROM page
        JOIN object_versions ov
          ON ov.object_id = page.object_id
         AND ov.object_version = page.current_object_version
         AND ov.deleted_at IS NULL
         AND NOT ov.is_delete_marker
    ), 0)::bigint AS bytes_used
