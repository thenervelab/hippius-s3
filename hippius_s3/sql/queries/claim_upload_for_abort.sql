-- AbortMultipartUpload's claim: delete the upload row — and, by cascade, its parts — only while it
-- is still an upload in progress. Returns nothing when there is nothing it may abort; the caller
-- answers NoSuchUpload and cleans up nothing.
--
-- Two conditions, both about the parts being an OBJECT's data rather than an upload's:
--   * is_completed = FALSE. Serialises with CompleteMultipartUpload's conditional flip on this row,
--     so exactly one of them wins. NULL is not known to be open, so it is refused (fail closed).
--   * no serveable version behind the parts. A simple PUT and a streaming CopyObject commit their
--     finished version together with this row, then flip is_completed only after the address is
--     persisted. In that window the row looks open (it is listed by ListMultipartUploads), yet its
--     parts are a complete — possibly already Object-Locked — object.
--
-- The abandoned-upload reaper uses abort_multipart_upload.sql instead: it must still reap a simple
-- PUT whose address was never written.
--
-- Parameters: $1: upload_id
DELETE FROM multipart_uploads mu
WHERE mu.upload_id = $1
  AND mu.is_completed = FALSE
  AND NOT EXISTS (
      SELECT 1
      FROM parts p
      JOIN object_versions ov ON ov.object_id = p.object_id AND ov.object_version = p.object_version
      WHERE p.upload_id = mu.upload_id
        AND (ov.size_bytes > 0 OR (ov.md5_hash IS NOT NULL AND ov.md5_hash != ''))
  )
RETURNING mu.upload_id
