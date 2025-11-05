-- migrate:up

ALTER TABLE part_chunks
ADD COLUMN pin_attempts INT NOT NULL DEFAULT 0,
ADD COLUMN last_pinned_at TIMESTAMPTZ DEFAULT NULL;

CREATE INDEX idx_part_chunks_pin_attempts ON part_chunks(pin_attempts) WHERE pin_attempts > 0;

-- migrate:down

DROP INDEX IF EXISTS idx_part_chunks_pin_attempts;
ALTER TABLE part_chunks DROP COLUMN IF EXISTS last_pinned_at;
ALTER TABLE part_chunks DROP COLUMN IF EXISTS pin_attempts;
