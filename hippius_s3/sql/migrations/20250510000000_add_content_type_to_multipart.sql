-- migrate:up

-- This is a no-op migration since content_type was already added in 20250508000000_multipart_upload.sql
SELECT 1;

-- migrate:down

-- No action needed for rollback since we didn't make any changes
SELECT 1;
