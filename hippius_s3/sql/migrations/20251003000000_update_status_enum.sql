-- migrate:up

ALTER TABLE objects DROP CONSTRAINT IF EXISTS objects_status_check;

ALTER TABLE objects ADD CONSTRAINT objects_status_check
CHECK (status = ANY (ARRAY['pending'::text, 'processing'::text, 'publishing'::text, 'published'::text, 'failed'::text, 'pinning'::text, 'uploaded'::text]));

ALTER TABLE objects ALTER COLUMN status DROP DEFAULT;

UPDATE objects
SET status = CASE
    WHEN status = 'pinning' THEN 'processing'
    WHEN status = 'uploaded' THEN 'published'
    ELSE status
END;

ALTER TABLE objects DROP CONSTRAINT IF EXISTS objects_status_check;

ALTER TABLE objects ADD CONSTRAINT objects_status_check
CHECK (status = ANY (ARRAY['pending'::text, 'processing'::text, 'publishing'::text, 'published'::text, 'failed'::text]));

ALTER TABLE objects ALTER COLUMN status SET DEFAULT 'pending';

CREATE INDEX IF NOT EXISTS idx_objects_status_processing ON objects(status) WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS idx_objects_status_publishing ON objects(status) WHERE status = 'publishing';

-- migrate:down

ALTER TABLE objects DROP CONSTRAINT IF EXISTS objects_status_check;

ALTER TABLE objects ADD CONSTRAINT objects_status_check
CHECK (status = ANY (ARRAY['publishing'::text, 'pinning'::text, 'uploaded'::text, 'failed'::text, 'processing'::text, 'published'::text]));

ALTER TABLE objects ALTER COLUMN status DROP DEFAULT;

UPDATE objects
SET status = CASE
    WHEN status = 'processing' THEN 'pinning'
    WHEN status = 'published' THEN 'uploaded'
    ELSE status
END;

ALTER TABLE objects DROP CONSTRAINT IF EXISTS objects_status_check;

ALTER TABLE objects ADD CONSTRAINT objects_status_check
CHECK (status = ANY (ARRAY['publishing'::text, 'pinning'::text, 'uploaded'::text, 'failed'::text]));

ALTER TABLE objects ALTER COLUMN status SET DEFAULT 'publishing';

DROP INDEX IF EXISTS idx_objects_status_processing;
DROP INDEX IF EXISTS idx_objects_status_publishing;
