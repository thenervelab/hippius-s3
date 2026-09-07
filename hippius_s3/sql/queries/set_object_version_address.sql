-- Persist the main-account address on an object version for the drain-gated upload
-- promoter (s3-2.1 PR-7). Written by the api on the promoter path in place of the
-- PUT-time enqueue, so the promoter can build the UploadChainRequest by object_id.
UPDATE object_versions
SET address = $3
WHERE object_id = $1 AND object_version = $2;
