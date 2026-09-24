-- Delete a multipart upload that is still open, and (by cascade) its parts.
-- `= FALSE`, not "is not true": a completed upload's parts are its object's data, and a NULL is
-- not known to be open. Used by the abandoned-upload reaper; the API abort claims through
-- claim_upload_for_abort.sql, which also refuses a finished simple PUT still in its tail.
-- Parameters: $1: upload_id
DELETE FROM multipart_uploads
WHERE upload_id = $1
  AND is_completed = FALSE
RETURNING upload_id
