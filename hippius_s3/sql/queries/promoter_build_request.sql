-- Build the non-derivable fields the upload-promoter needs to reconstruct an
-- UploadChainRequest for an object version (s3-2.1 PR-7). The upload workers re-derive
-- parts/chunks by object_id; only bucket_name, object_key and the main-account address
-- need looking up (address persisted by the api on the promoter path).
SELECT b.bucket_name, o.object_key, ov.address
FROM objects o
JOIN object_versions ov ON ov.object_id = o.object_id AND ov.object_version = $2
JOIN buckets b ON b.bucket_id = o.bucket_id
WHERE o.object_id = $1;
