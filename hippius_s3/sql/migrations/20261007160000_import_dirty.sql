-- migrate:up

-- Notices for the Rust catch-up (hippius-s3-internal docs/migration-runbook.md).
-- One row per object key, and one per bucket, overwritten in place with the
-- transaction id of the latest change. The catch-up deletes a row only when
-- that id is still the one it read, so a newer change stays.
--
-- The trigger runs as the function owner. A write that cannot record its
-- notice fails with the write: a missed notice would leave Rust serving the
-- old object and the counts could still match.
--
-- CREATE TRIGGER takes ACCESS EXCLUSIVE. Prod lock_timeout is 0, so an
-- unbounded wait queues the data plane behind it. 3s fails the migration
-- instead; dbmate rolls the file back and the job retries.
SET LOCAL lock_timeout = '3s';

CREATE TABLE import_dirty (
    bucket_id  uuid NOT NULL,
    object_key text NOT NULL,
    object_id  uuid NOT NULL,
    txid       xid8 NOT NULL,
    PRIMARY KEY (bucket_id, object_key)
);

CREATE TABLE import_dirty_buckets (
    bucket_id uuid PRIMARY KEY,
    txid      xid8 NOT NULL
);

CREATE FUNCTION import_note_key(b uuid, k text, o uuid) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    INSERT INTO import_dirty (bucket_id, object_key, object_id, txid)
    VALUES (b, k, o, pg_current_xact_id())
    ON CONFLICT (bucket_id, object_key) DO UPDATE
        SET object_id = EXCLUDED.object_id,
            txid = EXCLUDED.txid;
END;
$$;

CREATE FUNCTION import_note_object(oid uuid) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    -- UNION, so a key stored as the object and again as an alias is one row.
    -- UNION ALL would update that row twice and abort the user's write.
    INSERT INTO import_dirty (bucket_id, object_key, object_id, txid)
    SELECT bucket_id, object_key, object_id, pg_current_xact_id()
    FROM (
        SELECT o.bucket_id, o.object_key, o.object_id
        FROM objects o
        WHERE o.object_id = oid
        UNION
        SELECT n.bucket_id, n.object_key, n.object_id
        FROM object_names n
        WHERE n.object_id = oid
    ) AS noticed
    ON CONFLICT (bucket_id, object_key) DO UPDATE
        SET object_id = EXCLUDED.object_id,
            txid = EXCLUDED.txid;
END;
$$;

CREATE FUNCTION import_note_bucket(b uuid) RETURNS void
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    INSERT INTO import_dirty_buckets (bucket_id, txid)
    VALUES (b, pg_current_xact_id())
    ON CONFLICT (bucket_id) DO UPDATE
        SET txid = EXCLUDED.txid;
END;
$$;

CREATE FUNCTION import_dirty_object_row() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM import_note_key(OLD.bucket_id, OLD.object_key, OLD.object_id);
        RETURN NULL;
    END IF;
    IF TG_OP = 'UPDATE' AND OLD.object_key IS DISTINCT FROM NEW.object_key THEN
        PERFORM import_note_key(OLD.bucket_id, OLD.object_key, OLD.object_id);
    END IF;
    PERFORM import_note_object(NEW.object_id);
    RETURN NULL;
END;
$$;

CREATE FUNCTION import_dirty_version_row() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM import_note_object(
        CASE WHEN TG_OP = 'DELETE' THEN OLD.object_id ELSE NEW.object_id END
    );
    RETURN NULL;
END;
$$;

CREATE FUNCTION import_dirty_name_row() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    IF TG_OP = 'DELETE' THEN
        PERFORM import_note_key(OLD.bucket_id, OLD.object_key, OLD.object_id);
        RETURN NULL;
    END IF;
    PERFORM import_note_object(NEW.object_id);
    RETURN NULL;
END;
$$;

CREATE FUNCTION import_dirty_acl_row() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM import_note_object(
        CASE WHEN TG_OP = 'DELETE' THEN OLD.object_id ELSE NEW.object_id END
    );
    RETURN NULL;
END;
$$;

CREATE FUNCTION import_dirty_part_row() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    oid uuid;
BEGIN
    oid := CASE WHEN TG_OP = 'DELETE' THEN OLD.object_id ELSE NEW.object_id END;
    IF oid IS NULL AND TG_OP = 'UPDATE' THEN
        oid := OLD.object_id;
    END IF;
    IF oid IS NOT NULL THEN
        PERFORM import_note_object(oid);
    END IF;
    RETURN NULL;
END;
$$;

-- chunk_backend is the hot path: one row per chunk as it reaches HCFS.
-- Two primary-key lookups resolve the object. A part that is not bound to
-- an object yet leaves no notice; the parts trigger notes it when it is.
CREATE FUNCTION import_dirty_chunk_row() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
DECLARE
    noted_chunk bigint;
    noted_object uuid;
BEGIN
    IF TG_OP = 'DELETE' THEN
        noted_chunk := OLD.chunk_id;
        IF OLD.backend IS DISTINCT FROM 'arion' THEN
            RETURN NULL;
        END IF;
    ELSE
        noted_chunk := NEW.chunk_id;
        IF NEW.backend IS DISTINCT FROM 'arion' THEN
            RETURN NULL;
        END IF;
    END IF;
    -- noted_chunk, not cid: part_chunks.cid is a column, and a variable of
    -- that name makes this lookup abort the chunk write.
    SELECT p.object_id INTO noted_object
    FROM part_chunks pc
    JOIN parts p ON p.part_id = pc.part_id
    WHERE pc.id = noted_chunk AND p.object_id IS NOT NULL;
    IF noted_object IS NOT NULL THEN
        PERFORM import_note_object(noted_object);
    END IF;
    RETURN NULL;
END;
$$;

CREATE FUNCTION import_dirty_bucket_row() RETURNS trigger
LANGUAGE plpgsql
SECURITY DEFINER
SET search_path = pg_catalog, public
AS $$
BEGIN
    PERFORM import_note_bucket(
        CASE WHEN TG_OP = 'DELETE' THEN OLD.bucket_id ELSE NEW.bucket_id END
    );
    RETURN NULL;
END;
$$;

-- zz_ sorts after the storage_delta triggers. AFTER ROW triggers fire in
-- name order, and those lock the version while this one locks the notice.
-- The other order deadlocks two finalizes of the same key.
CREATE TRIGGER zz_import_dirty_objects
    AFTER INSERT OR UPDATE OR DELETE ON objects
    FOR EACH ROW EXECUTE FUNCTION import_dirty_object_row();

CREATE TRIGGER zz_import_dirty_object_versions
    AFTER INSERT OR UPDATE OR DELETE ON object_versions
    FOR EACH ROW EXECUTE FUNCTION import_dirty_version_row();

CREATE TRIGGER import_dirty_object_names
    AFTER INSERT OR UPDATE OR DELETE ON object_names
    FOR EACH ROW EXECUTE FUNCTION import_dirty_name_row();

CREATE TRIGGER import_dirty_object_acls
    AFTER INSERT OR UPDATE OR DELETE ON object_acls
    FOR EACH ROW EXECUTE FUNCTION import_dirty_acl_row();

CREATE TRIGGER import_dirty_parts
    AFTER INSERT OR UPDATE OR DELETE ON parts
    FOR EACH ROW EXECUTE FUNCTION import_dirty_part_row();

CREATE TRIGGER import_dirty_chunk_backend
    AFTER INSERT OR UPDATE OR DELETE ON chunk_backend
    FOR EACH ROW EXECUTE FUNCTION import_dirty_chunk_row();

CREATE TRIGGER import_dirty_buckets
    AFTER INSERT OR UPDATE OR DELETE ON buckets
    FOR EACH ROW EXECUTE FUNCTION import_dirty_bucket_row();

CREATE TRIGGER import_dirty_bucket_acls
    AFTER INSERT OR UPDATE OR DELETE ON bucket_acls
    FOR EACH ROW EXECUTE FUNCTION import_dirty_bucket_row();

CREATE TRIGGER import_dirty_bucket_prefixes
    AFTER INSERT OR UPDATE OR DELETE ON bucket_public_prefixes
    FOR EACH ROW EXECUTE FUNCTION import_dirty_bucket_row();

-- migrate:down

SET LOCAL lock_timeout = '3s';

DROP TRIGGER IF EXISTS import_dirty_bucket_prefixes ON bucket_public_prefixes;
DROP TRIGGER IF EXISTS import_dirty_bucket_acls ON bucket_acls;
DROP TRIGGER IF EXISTS import_dirty_buckets ON buckets;
DROP TRIGGER IF EXISTS import_dirty_chunk_backend ON chunk_backend;
DROP TRIGGER IF EXISTS import_dirty_parts ON parts;
DROP TRIGGER IF EXISTS import_dirty_object_acls ON object_acls;
DROP TRIGGER IF EXISTS import_dirty_object_names ON object_names;
DROP TRIGGER IF EXISTS zz_import_dirty_object_versions ON object_versions;
DROP TRIGGER IF EXISTS zz_import_dirty_objects ON objects;

DROP FUNCTION IF EXISTS import_dirty_bucket_row();
DROP FUNCTION IF EXISTS import_dirty_chunk_row();
DROP FUNCTION IF EXISTS import_dirty_part_row();
DROP FUNCTION IF EXISTS import_dirty_acl_row();
DROP FUNCTION IF EXISTS import_dirty_name_row();
DROP FUNCTION IF EXISTS import_dirty_version_row();
DROP FUNCTION IF EXISTS import_dirty_object_row();
DROP FUNCTION IF EXISTS import_note_bucket(uuid);
DROP FUNCTION IF EXISTS import_note_object(uuid);
DROP FUNCTION IF EXISTS import_note_key(uuid, text, uuid);

DROP TABLE IF EXISTS import_dirty_buckets;
DROP TABLE IF EXISTS import_dirty;
