-- migrate:up

ALTER TABLE parts
    ADD CONSTRAINT uq_parts_object_part UNIQUE (object_id, part_number);

-- migrate:down

ALTER TABLE parts
    DROP CONSTRAINT IF EXISTS uq_parts_object_part;
