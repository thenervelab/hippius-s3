-- The uploader's half of the drain hand-off: once every chunk of a part has a live
-- chunk_backend row, flip the drain's row from 'uploading' (SSD copy is the ONLY copy; pinned
-- against eviction) to 'replicated' (on the backend; the SSD copy is disposable read tier).
--
-- Guarded on status = 'uploading' so it is a no-op for a pool-era 'replicated' row (the
-- legacy global-queue uploader runs this too) and for a row a re-drive has returned to
-- 'pending' since the request was published. The drain's upload sweep flips from
-- chunk_backend coverage as the backstop if this statement is lost to a crash.
--
-- Params: $1 object_id (text — cephor stores it as text), $2 version (bigint), $3 part_number (bigint).
UPDATE cephor_replication_status
SET status = 'replicated', updated_at = now()
WHERE object_id = $1 AND version = $2 AND part_number = $3 AND status = 'uploading'
