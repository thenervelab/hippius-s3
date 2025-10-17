-- migrate:up

-- Rename columns for clearer naming: version_seq -> object_version, current_version_seq -> current_object_version
-- PostgreSQL will automatically update constraint definitions when columns are renamed

-- Rename the columns
ALTER TABLE public.object_versions
  RENAME COLUMN version_seq TO object_version;

ALTER TABLE public.objects
  RENAME COLUMN current_version_seq TO current_object_version;

ALTER TABLE public.parts
  RENAME COLUMN object_version_seq TO object_version;

-- migrate:down

-- Reverse the migration: rename columns back to old names

-- Rename columns back
ALTER TABLE public.object_versions
  RENAME COLUMN object_version TO version_seq;

ALTER TABLE public.objects
  RENAME COLUMN current_object_version TO current_version_seq;

ALTER TABLE public.parts
  RENAME COLUMN object_version TO object_version_seq;
