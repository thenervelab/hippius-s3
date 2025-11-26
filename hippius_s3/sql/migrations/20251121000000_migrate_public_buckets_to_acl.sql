-- migrate:up

-- Create public-read ACL for buckets with is_public=true that don't have an ACL yet
-- Using DISTINCT ON to handle duplicate bucket_name entries
INSERT INTO bucket_acls (bucket_name, owner_id, acl_json)
SELECT DISTINCT ON (b.bucket_name)
    b.bucket_name,
    b.main_account_id,
    jsonb_build_object(
        'owner', jsonb_build_object('id', b.main_account_id),
        'grants', jsonb_build_array(
            jsonb_build_object(
                'grantee', jsonb_build_object(
                    'type', 'CanonicalUser',
                    'id', b.main_account_id
                ),
                'permission', 'FULL_CONTROL'
            ),
            jsonb_build_object(
                'grantee', jsonb_build_object(
                    'type', 'Group',
                    'uri', 'http://acs.amazonaws.com/groups/global/AllUsers'
                ),
                'permission', 'READ'
            )
        )
    )
FROM buckets b
LEFT JOIN bucket_acls ba ON b.bucket_name = ba.bucket_name
WHERE b.is_public = true AND ba.bucket_name IS NULL
ORDER BY b.bucket_name, b.created_at DESC;

-- Create private ACL for buckets with is_public=false that don't have an ACL yet
-- Using DISTINCT ON to handle duplicate bucket_name entries
INSERT INTO bucket_acls (bucket_name, owner_id, acl_json)
SELECT DISTINCT ON (b.bucket_name)
    b.bucket_name,
    b.main_account_id,
    jsonb_build_object(
        'owner', jsonb_build_object('id', b.main_account_id),
        'grants', jsonb_build_array(
            jsonb_build_object(
                'grantee', jsonb_build_object(
                    'type', 'CanonicalUser',
                    'id', b.main_account_id
                ),
                'permission', 'FULL_CONTROL'
            )
        )
    )
FROM buckets b
LEFT JOIN bucket_acls ba ON b.bucket_name = ba.bucket_name
WHERE b.is_public = false AND ba.bucket_name IS NULL
ORDER BY b.bucket_name, b.created_at DESC;

-- Update is_public to false for all buckets since ACLs now handle permissions
UPDATE buckets SET is_public = false;

-- migrate:down

-- No easy rollback as we've lost the distinction between "originally public" and "made public via ACL"
-- But we can try to restore is_public based on ACLs (best effort)

UPDATE buckets b
SET is_public = true
FROM bucket_acls ba
WHERE b.bucket_name = ba.bucket_name
AND ba.acl_json->'grants' @> '[{"grantee":{"type":"Group","uri":"http://acs.amazonaws.com/groups/global/AllUsers"},"permission":"READ"}]';

-- We don't delete the ACLs in rollback because that would be destructive to new ACLs created after migration
