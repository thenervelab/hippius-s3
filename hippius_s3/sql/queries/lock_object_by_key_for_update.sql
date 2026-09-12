-- Serialize conditional writes (If-None-Match: *) on one key. First statement of the reserve
-- transaction, before upsert_object_basic touches the row, so conditional_write_state and the reserve
-- are atomic with respect to every other reserve of the same key (the upsert's ON CONFLICT takes this
-- same row lock, so unconditional PUTs queue behind it too).
--
-- Returns no row for a key that has never had an objects row: there is nothing to lock yet, and two
-- first writers racing each other are settled by the tail re-check (conditional_write_conflict).
--
-- $1 bucket_id (uuid), $2 object_key (text)
SELECT object_id
FROM objects
WHERE bucket_id = $1::uuid
  AND object_key = $2
FOR UPDATE
