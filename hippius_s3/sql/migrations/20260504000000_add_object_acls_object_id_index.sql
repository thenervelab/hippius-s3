-- migrate:up

-- The fk_object_acls_object FK has ON DELETE CASCADE from objects(object_id),
-- but no leading-column index on object_acls(object_id). Both existing composite
-- indexes start with bucket_id (idx_object_acls_bucket_object,
-- object_acls_bucket_object_key), so cascade enforcement falls back to a seq
-- scan of object_acls per deleted object — painful when DELETE FROM buckets
-- cascades through tens of thousands of objects.
-- object_acls is small (~8 MB in prod), so a plain CREATE INDEX is fine.
CREATE INDEX IF NOT EXISTS idx_object_acls_object_id
    ON public.object_acls (object_id);

-- migrate:down

DROP INDEX IF EXISTS idx_object_acls_object_id;
