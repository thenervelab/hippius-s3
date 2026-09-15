-- Persist one slice's progress. Accumulates rather than assigns so a caller cannot lose a page by
-- recomputing the running total from a stale read.
UPDATE bucket_storage_verify_state
   SET cursor_key = $2,
       partial_bytes = partial_bytes + $3,
       objects_scanned = objects_scanned + $4,
       slices_done = slices_done + 1
 WHERE bucket_id = $1
RETURNING partial_bytes, objects_scanned, slices_done
