-- migrate:up

-- The Rust catch-up hears about a bucket only through import_dirty_buckets
-- (20261007160000_import_dirty.sql). bucket_public_objects, the whole-bucket
-- GetObject marker (20261012130000), came after that file and has no trigger,
-- so putting or removing that policy on a bucket with no public prefix and no
-- ACL change records nothing: after cutover the bucket would stay public on
-- Rust once Python made it private. The marker is a bucket setting, so it
-- notes the bucket, as bucket_acls and bucket_public_prefixes do. A cascade
-- delete from buckets notes it too; import_dirty_buckets has no foreign key.
--
-- CREATE TRIGGER takes ACCESS EXCLUSIVE on a one-row table; lock_timeout
-- bounds the wait because prod runs with lock_timeout = 0.
SET LOCAL lock_timeout = '3s';

CREATE TRIGGER import_dirty_bucket_public_objects
    AFTER INSERT OR UPDATE OR DELETE ON bucket_public_objects
    FOR EACH ROW EXECUTE FUNCTION import_dirty_bucket_row();

-- migrate:down

SET LOCAL lock_timeout = '3s';

DROP TRIGGER IF EXISTS import_dirty_bucket_public_objects ON bucket_public_objects;
