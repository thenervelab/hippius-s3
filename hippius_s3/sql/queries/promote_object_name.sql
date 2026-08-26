-- $1 bucket_id, $2 primary object_key being removed
-- Relocates a soft-deleted occupant of the dest key, then renames the primary.
-- Returns NULL when this is the last name (caller soft-deletes). Raises 23505
-- if a live primary still occupies dest.
SELECT promote_object_name($1::uuid, $2::text) AS object_id
