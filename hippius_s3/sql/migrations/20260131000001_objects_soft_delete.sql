-- migrate:up
ALTER TABLE objects ADD COLUMN deleted_at TIMESTAMPTZ;
CREATE INDEX idx_objects_deleted ON objects(deleted_at) WHERE deleted_at IS NOT NULL;

-- migrate:down
DELETE FROM objects WHERE deleted_at IS NOT NULL;
ALTER TABLE objects DROP COLUMN deleted_at;
DROP INDEX IF EXISTS idx_objects_deleted;
