-- migrate:up

-- B1: the part numbers the client named in CompleteMultipartUpload. S3 allows completing an MPU
-- with a SUBSET of the uploaded parts (the rest are discarded), but the reader + ETag/size were
-- assembled from ALL parts rows for the version → extra bytes served + a wrong multipart ETag.
-- We can't safely delete the unlisted parts (multi-node SSD, in-flight drain, Arion pin leak via
-- the chunk_backend cascade), so instead we record the selected set here and FILTER the read to
-- it. NULL means "all parts" (every pre-existing object, and any completion that named the full
-- set) — fully backward-compatible.
ALTER TABLE object_versions ADD COLUMN completed_part_numbers integer[];

-- migrate:down

ALTER TABLE object_versions DROP COLUMN IF EXISTS completed_part_numbers;
