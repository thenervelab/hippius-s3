-- One purge batch: soft-delete up to $2 live objects in a bucket and return, per
-- object, the logical bytes across ALL its versions plus the distinct backends that
-- hold its chunks (feeds UnpinChainRequest.delete_backends, so the unpinner never
-- polls a backend that has nothing — the batch equivalent of get_object_backends.sql).
-- Soft-delete is self-consuming (deleted rows leave the candidate set), so resuming a
-- crashed job needs no cursor: just run the batch again. SKIP LOCKED sidesteps rows a
-- concurrent writer still holds; the purger's final sweep picks them up.
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
),
version_bytes AS (
    SELECT ov.object_id, SUM(ov.size_bytes)::bigint AS total_bytes
    FROM object_versions ov
    WHERE ov.object_id IN (SELECT object_id FROM del)
    GROUP BY ov.object_id
),
object_backends AS (
    SELECT p.object_id, ARRAY_AGG(DISTINCT cb.backend) AS backends
    FROM parts p
    JOIN part_chunks pc ON pc.part_id = p.part_id
    JOIN chunk_backend cb
      ON cb.chunk_id = pc.id
     AND NOT cb.deleted
     AND cb.backend_identifier IS NOT NULL
    WHERE p.object_id IN (SELECT object_id FROM del)
    GROUP BY p.object_id
)
SELECT d.object_id,
       COALESCE(vb.total_bytes, 0) AS total_bytes,
       COALESCE(ob.backends, '{}') AS backends
FROM del d
LEFT JOIN version_bytes vb ON vb.object_id = d.object_id
LEFT JOIN object_backends ob ON ob.object_id = d.object_id
