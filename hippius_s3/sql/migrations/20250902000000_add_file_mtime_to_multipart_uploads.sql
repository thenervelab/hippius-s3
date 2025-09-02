-- migrate:up

-- Add file_mtime column to multipart_uploads table
ALTER TABLE multipart_uploads ADD COLUMN file_mtime TIMESTAMP WITH TIME ZONE;

-- migrate:down

-- Remove file_mtime column from multipart_uploads table
ALTER TABLE multipart_uploads DROP COLUMN file_mtime;
