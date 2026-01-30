-- Add upload_backends column to object_versions.
-- Records which backends were targeted for upload at version creation time.
-- NULL for pre-existing rows (janitor falls back to config.upload_backends).
ALTER TABLE object_versions
  ADD COLUMN IF NOT EXISTS upload_backends text[] NULL;
