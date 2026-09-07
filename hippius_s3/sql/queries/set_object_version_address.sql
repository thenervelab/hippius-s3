-- Persist the main-account address on an object version for the drain-gated upload
-- promoter (s3-2.1 PR-7). Written by the api on the promoter path in place of the
-- PUT-time enqueue, so the promoter can build the UploadChainRequest by object_id.
--
-- $4 carries the gateway's billing-exemption verdict for the VERIFIED CALLER. It travels with
-- the address because the uploader cannot re-derive it: `address` is the bucket OWNER, and
-- exempting on that alone would exempt a third party writing into a service account's bucket.
UPDATE object_versions
SET address = $3,
    billing_bypass = $4
WHERE object_id = $1 AND object_version = $2;
