SELECT access_key_id, account_id, permission, bucket_scope, bucket_ids, created_at, updated_at
FROM sub_token_scopes
WHERE access_key_id = $1
