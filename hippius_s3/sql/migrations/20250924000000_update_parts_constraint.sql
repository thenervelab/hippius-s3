-- migrate:up

-- Drop the old unique constraint on (upload_id, part_number)
ALTER TABLE parts DROP CONSTRAINT IF EXISTS parts_upload_id_part_number_key;

-- Make the object_id constraint the primary constraint by dropping and re-adding it
ALTER TABLE parts DROP CONSTRAINT IF EXISTS uq_parts_object_part;
ALTER TABLE parts ADD CONSTRAINT parts_object_id_part_number_key UNIQUE (object_id, part_number);

-- migrate:down

-- Restore the old constraint
ALTER TABLE parts DROP CONSTRAINT IF EXISTS parts_object_id_part_number_key;
ALTER TABLE parts ADD CONSTRAINT parts_upload_id_part_number_key UNIQUE (upload_id, part_number);
