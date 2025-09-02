-- migrate:up
ALTER TABLE objects ADD COLUMN multipart BOOLEAN DEFAULT FALSE;

-- migrate:down
ALTER TABLE objects DROP COLUMN multipart;
