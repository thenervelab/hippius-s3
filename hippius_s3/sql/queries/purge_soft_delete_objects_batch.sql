-- One purge batch: soft-delete up to $2 live objects in a bucket and return, per object,
-- the logical bytes across ALL its versions. Soft-delete is self-consuming (deleted rows
-- leave the candidate set), so resuming a crashed job needs no cursor — just run the batch
-- again. SKIP LOCKED sidesteps rows a concurrent writer still holds; the purger's final
-- sweep picks them up.
--
-- Backends are NOT resolved here on purpose: the unpinner re-fetches them per request
-- (get_chunk_backend_identifiers), so an ARRAY_AGG across parts->part_chunks->chunk_backend
-- for 500 objects would be pure duplicated work — and on the multi-TB / million-chunk
-- objects this feature exists to purge, that aggregate is exactly what blows the batch's
-- statement_timeout. The purger enqueues with delete_backends=None; enqueue fans out to the
-- configured delete backends and the unpinner's own query is authoritative about what each
-- backend actually holds.
-- Parameters: $1: bucket_id, $2: batch size
WITH candidates AS (
    SELECT object_id
    FROM objects
    WHERE bucket_id = $1
      AND deleted_at IS NULL
    LIMIT $2
    FOR UPDATE SKIP LOCKED
),
del AS (
    UPDATE objects o
    SET deleted_at = now()
    FROM candidates c
    WHERE o.object_id = c.object_id
    RETURNING o.object_id
)
SELECT d.object_id,
       COALESCE(SUM(ov.size_bytes), 0)::bigint AS total_bytes
FROM del d
LEFT JOIN object_versions ov ON ov.object_id = d.object_id
GROUP BY d.object_id
