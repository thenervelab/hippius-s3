\restrict DjFHVU1F1k4MaKkXJIWywMbBKKTT0LV7DEjep1dYYtoAJuglgNNseKPtVQV71kx

-- Dumped from database version 15.14 (Debian 15.14-1.pgdg13+1)
-- Dumped by pg_dump version 17.6 (Debian 17.6-0+deb13u1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: public; Type: SCHEMA; Schema: -; Owner: -
--

-- *not* creating schema, since initdb creates it


--
-- Name: SCHEMA public; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA public IS '';


--
-- Name: version_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.version_type AS ENUM (
    'user',
    'migration'
);


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: bucket_acls; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.bucket_acls (
    id integer NOT NULL,
    bucket_name character varying(255) NOT NULL,
    owner_id character varying(255) NOT NULL,
    acl_json jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: bucket_acls_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.bucket_acls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: bucket_acls_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.bucket_acls_id_seq OWNED BY public.bucket_acls.id;


--
-- Name: buckets; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.buckets (
    bucket_id uuid NOT NULL,
    bucket_name text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    is_public boolean DEFAULT false,
    tags jsonb DEFAULT '{}'::jsonb,
    main_account_id text NOT NULL
);


--
-- Name: cids; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cids (
    id uuid NOT NULL,
    cid text NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: encryption_keys; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.encryption_keys (
    id integer NOT NULL,
    subaccount_id character varying(255) NOT NULL,
    encryption_key_b64 text NOT NULL,
    created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP
);


--
-- Name: TABLE encryption_keys; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.encryption_keys IS 'Stores versioned encryption keys per subaccount ID (never deleted, always use most recent)';


--
-- Name: COLUMN encryption_keys.subaccount_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.encryption_keys.subaccount_id IS 'Subaccount identifier for key association';


--
-- Name: COLUMN encryption_keys.encryption_key_b64; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.encryption_keys.encryption_key_b64 IS 'Base64 encoded encryption key';


--
-- Name: encryption_keys_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.encryption_keys_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: encryption_keys_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.encryption_keys_id_seq OWNED BY public.encryption_keys.id;


--
-- Name: files; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.files (
    file_id uuid NOT NULL,
    ipfs_cid text,
    file_name text NOT NULL,
    content_type text NOT NULL,
    file_size bigint NOT NULL,
    created_at timestamp with time zone NOT NULL,
    metadata jsonb,
    cid_id uuid
);


--
-- Name: multipart_uploads; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.multipart_uploads (
    upload_id uuid NOT NULL,
    bucket_id uuid NOT NULL,
    object_key text NOT NULL,
    initiated_at timestamp with time zone NOT NULL,
    is_completed boolean DEFAULT false,
    content_type text,
    metadata jsonb,
    file_mtime timestamp with time zone,
    object_id uuid
);


--
-- Name: object_acls; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.object_acls (
    id integer NOT NULL,
    bucket_name character varying(255) NOT NULL,
    object_key text NOT NULL,
    owner_id character varying(255) NOT NULL,
    acl_json jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: object_acls_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.object_acls_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: object_acls_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.object_acls_id_seq OWNED BY public.object_acls.id;


--
-- Name: object_versions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.object_versions (
    object_id uuid NOT NULL,
    object_version bigint NOT NULL,
    version_type public.version_type DEFAULT 'user'::public.version_type NOT NULL,
    storage_version smallint NOT NULL,
    size_bytes bigint NOT NULL,
    content_type text NOT NULL,
    metadata jsonb,
    md5_hash text,
    ipfs_cid text,
    cid_id uuid,
    multipart boolean DEFAULT false,
    status character varying(50) DEFAULT 'publishing'::character varying,
    append_version integer DEFAULT 0 NOT NULL,
    manifest_cid text,
    manifest_built_for_version integer,
    manifest_built_at timestamp with time zone,
    last_append_at timestamp with time zone DEFAULT now() NOT NULL,
    last_modified timestamp with time zone DEFAULT now(),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT object_versions_status_check CHECK (((status)::text = ANY (ARRAY['publishing'::text, 'pinning'::text, 'uploaded'::text, 'failed'::text])))
);


--
-- Name: objects; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.objects (
    object_id uuid NOT NULL,
    bucket_id uuid NOT NULL,
    object_key text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    current_object_version bigint DEFAULT 1 NOT NULL
);


--
-- Name: part_chunks; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.part_chunks (
    id bigint NOT NULL,
    part_id uuid NOT NULL,
    chunk_index integer NOT NULL,
    cid text,
    cipher_size_bytes bigint NOT NULL,
    plain_size_bytes bigint,
    checksum bytea,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    pin_attempts integer DEFAULT 0 NOT NULL,
    last_pinned_at timestamp with time zone,
    CONSTRAINT part_chunks_chunk_index_check CHECK ((chunk_index >= 0)),
    CONSTRAINT part_chunks_cipher_size_bytes_check CHECK ((cipher_size_bytes >= 0)),
    CONSTRAINT part_chunks_plain_size_bytes_check CHECK ((plain_size_bytes >= 0))
);


--
-- Name: part_chunks_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.part_chunks_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: part_chunks_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.part_chunks_id_seq OWNED BY public.part_chunks.id;


--
-- Name: parts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.parts (
    part_id uuid NOT NULL,
    upload_id uuid NOT NULL,
    part_number integer NOT NULL,
    ipfs_cid text NOT NULL,
    size_bytes bigint NOT NULL,
    etag text NOT NULL,
    uploaded_at timestamp with time zone NOT NULL,
    cid_id uuid,
    object_id uuid,
    chunk_size_bytes integer,
    object_version bigint
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying NOT NULL
);


--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.users (
    main_account_id text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: bucket_acls id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bucket_acls ALTER COLUMN id SET DEFAULT nextval('public.bucket_acls_id_seq'::regclass);


--
-- Name: encryption_keys id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.encryption_keys ALTER COLUMN id SET DEFAULT nextval('public.encryption_keys_id_seq'::regclass);


--
-- Name: object_acls id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.object_acls ALTER COLUMN id SET DEFAULT nextval('public.object_acls_id_seq'::regclass);


--
-- Name: part_chunks id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.part_chunks ALTER COLUMN id SET DEFAULT nextval('public.part_chunks_id_seq'::regclass);


--
-- Name: bucket_acls bucket_acls_bucket_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bucket_acls
    ADD CONSTRAINT bucket_acls_bucket_name_key UNIQUE (bucket_name);


--
-- Name: bucket_acls bucket_acls_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.bucket_acls
    ADD CONSTRAINT bucket_acls_pkey PRIMARY KEY (id);


--
-- Name: buckets buckets_name_owner_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.buckets
    ADD CONSTRAINT buckets_name_owner_unique UNIQUE (bucket_name, main_account_id);


--
-- Name: buckets buckets_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.buckets
    ADD CONSTRAINT buckets_pkey PRIMARY KEY (bucket_id);


--
-- Name: cids cids_cid_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cids
    ADD CONSTRAINT cids_cid_key UNIQUE (cid);


--
-- Name: cids cids_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cids
    ADD CONSTRAINT cids_pkey PRIMARY KEY (id);


--
-- Name: encryption_keys encryption_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.encryption_keys
    ADD CONSTRAINT encryption_keys_pkey PRIMARY KEY (id);


--
-- Name: files files_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_pkey PRIMARY KEY (file_id);


--
-- Name: multipart_uploads multipart_uploads_bucket_id_object_key_upload_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.multipart_uploads
    ADD CONSTRAINT multipart_uploads_bucket_id_object_key_upload_id_key UNIQUE (bucket_id, object_key, upload_id);


--
-- Name: multipart_uploads multipart_uploads_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.multipart_uploads
    ADD CONSTRAINT multipart_uploads_pkey PRIMARY KEY (upload_id);


--
-- Name: object_acls object_acls_bucket_name_object_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.object_acls
    ADD CONSTRAINT object_acls_bucket_name_object_key_key UNIQUE (bucket_name, object_key);


--
-- Name: object_acls object_acls_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.object_acls
    ADD CONSTRAINT object_acls_pkey PRIMARY KEY (id);


--
-- Name: object_versions object_versions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.object_versions
    ADD CONSTRAINT object_versions_pkey PRIMARY KEY (object_id, object_version);


--
-- Name: objects objects_bucket_id_object_key_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.objects
    ADD CONSTRAINT objects_bucket_id_object_key_key UNIQUE (bucket_id, object_key);


--
-- Name: objects objects_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.objects
    ADD CONSTRAINT objects_pkey PRIMARY KEY (object_id);


--
-- Name: part_chunks part_chunks_part_id_chunk_index_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.part_chunks
    ADD CONSTRAINT part_chunks_part_id_chunk_index_key UNIQUE (part_id, chunk_index);


--
-- Name: part_chunks part_chunks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.part_chunks
    ADD CONSTRAINT part_chunks_pkey PRIMARY KEY (id);


--
-- Name: parts parts_object_version_part_unique; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.parts
    ADD CONSTRAINT parts_object_version_part_unique UNIQUE (object_id, object_version, part_number);


--
-- Name: parts parts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.parts
    ADD CONSTRAINT parts_pkey PRIMARY KEY (part_id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (main_account_id);


--
-- Name: idx_bucket_acls_bucket_name; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bucket_acls_bucket_name ON public.bucket_acls USING btree (bucket_name);


--
-- Name: idx_bucket_acls_owner_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_bucket_acls_owner_id ON public.bucket_acls USING btree (owner_id);


--
-- Name: idx_buckets_main_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_buckets_main_account ON public.buckets USING btree (main_account_id);


--
-- Name: idx_buckets_name_owner; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_buckets_name_owner ON public.buckets USING btree (bucket_name, main_account_id);


--
-- Name: idx_cids_cid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_cids_cid ON public.cids USING btree (cid);


--
-- Name: idx_encryption_keys_subaccount_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_encryption_keys_subaccount_created ON public.encryption_keys USING btree (subaccount_id, created_at DESC);


--
-- Name: idx_files_cid_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_files_cid_id ON public.files USING btree (cid_id);


--
-- Name: idx_files_created_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_files_created_at ON public.files USING btree (created_at);


--
-- Name: idx_files_ipfs_cid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_files_ipfs_cid ON public.files USING btree (ipfs_cid);


--
-- Name: idx_multipart_uploads_bucket; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_multipart_uploads_bucket ON public.multipart_uploads USING btree (bucket_id);


--
-- Name: idx_multipart_uploads_object_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_multipart_uploads_object_id ON public.multipart_uploads USING btree (object_id);


--
-- Name: idx_object_acls_bucket_object; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_object_acls_bucket_object ON public.object_acls USING btree (bucket_name, object_key);


--
-- Name: idx_object_acls_owner_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_object_acls_owner_id ON public.object_acls USING btree (owner_id);


--
-- Name: idx_object_versions_cid_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_object_versions_cid_id ON public.object_versions USING btree (cid_id);


--
-- Name: idx_object_versions_ipfs_cid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_object_versions_ipfs_cid ON public.object_versions USING btree (ipfs_cid);


--
-- Name: idx_object_versions_manifest_builder; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_object_versions_manifest_builder ON public.object_versions USING btree (last_append_at, append_version) WHERE ((manifest_built_for_version IS NULL) OR (append_version > manifest_built_for_version));


--
-- Name: idx_object_versions_object_created_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_object_versions_object_created_desc ON public.object_versions USING btree (object_id, created_at DESC);


--
-- Name: idx_object_versions_object_type_created_desc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_object_versions_object_type_created_desc ON public.object_versions USING btree (object_id, version_type, created_at DESC);


--
-- Name: idx_object_versions_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_object_versions_status ON public.object_versions USING btree (status);


--
-- Name: idx_objects_bucket_prefix; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_objects_bucket_prefix ON public.objects USING btree (bucket_id, object_key);


--
-- Name: idx_part_chunks_pin_attempts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_part_chunks_pin_attempts ON public.part_chunks USING btree (pin_attempts) WHERE (pin_attempts > 0);


--
-- Name: idx_parts_cid_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_parts_cid_id ON public.parts USING btree (cid_id);


--
-- Name: idx_parts_object_version; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_parts_object_version ON public.parts USING btree (object_id, object_version);


--
-- Name: idx_parts_upload; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_parts_upload ON public.parts USING btree (upload_id);


--
-- Name: part_chunks_part_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX part_chunks_part_idx ON public.part_chunks USING btree (part_id);


--
-- Name: files files_cid_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.files
    ADD CONSTRAINT files_cid_id_fkey FOREIGN KEY (cid_id) REFERENCES public.cids(id);


--
-- Name: buckets fk_buckets_main_account; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.buckets
    ADD CONSTRAINT fk_buckets_main_account FOREIGN KEY (main_account_id) REFERENCES public.users(main_account_id) ON DELETE CASCADE;


--
-- Name: multipart_uploads fk_multipart_uploads_bucket; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.multipart_uploads
    ADD CONSTRAINT fk_multipart_uploads_bucket FOREIGN KEY (bucket_id) REFERENCES public.buckets(bucket_id) ON DELETE CASCADE;


--
-- Name: objects fk_objects_bucket; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.objects
    ADD CONSTRAINT fk_objects_bucket FOREIGN KEY (bucket_id) REFERENCES public.buckets(bucket_id) ON DELETE CASCADE;


--
-- Name: parts fk_parts_upload; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.parts
    ADD CONSTRAINT fk_parts_upload FOREIGN KEY (upload_id) REFERENCES public.multipart_uploads(upload_id) ON DELETE CASCADE;


--
-- Name: multipart_uploads multipart_uploads_object_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.multipart_uploads
    ADD CONSTRAINT multipart_uploads_object_id_fkey FOREIGN KEY (object_id) REFERENCES public.objects(object_id) ON DELETE SET NULL;


--
-- Name: object_versions object_versions_cid_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.object_versions
    ADD CONSTRAINT object_versions_cid_id_fkey FOREIGN KEY (cid_id) REFERENCES public.cids(id);


--
-- Name: object_versions object_versions_object_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.object_versions
    ADD CONSTRAINT object_versions_object_id_fkey FOREIGN KEY (object_id) REFERENCES public.objects(object_id) ON DELETE CASCADE;


--
-- Name: objects objects_current_version_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.objects
    ADD CONSTRAINT objects_current_version_fk FOREIGN KEY (object_id, current_object_version) REFERENCES public.object_versions(object_id, object_version) ON DELETE RESTRICT DEFERRABLE INITIALLY DEFERRED;


--
-- Name: part_chunks part_chunks_part_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.part_chunks
    ADD CONSTRAINT part_chunks_part_id_fkey FOREIGN KEY (part_id) REFERENCES public.parts(part_id) ON DELETE CASCADE;


--
-- Name: parts parts_cid_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.parts
    ADD CONSTRAINT parts_cid_id_fkey FOREIGN KEY (cid_id) REFERENCES public.cids(id);


--
-- Name: parts parts_object_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.parts
    ADD CONSTRAINT parts_object_id_fkey FOREIGN KEY (object_id) REFERENCES public.objects(object_id) ON DELETE CASCADE;


--
-- Name: parts parts_object_version_fk; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.parts
    ADD CONSTRAINT parts_object_version_fk FOREIGN KEY (object_id, object_version) REFERENCES public.object_versions(object_id, object_version) ON DELETE CASCADE;


--
-- PostgreSQL database dump complete
--

\unrestrict DjFHVU1F1k4MaKkXJIWywMbBKKTT0LV7DEjep1dYYtoAJuglgNNseKPtVQV71kx


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20241201000001'),
    ('20241202000001'),
    ('20250506000000'),
    ('20250507000000'),
    ('20250508000000'),
    ('20250509000000'),
    ('20250510000000'),
    ('20250511000000'),
    ('20250602000000'),
    ('20250603000000'),
    ('20250821000000'),
    ('20250901000000'),
    ('20250902000000'),
    ('20250902000001'),
    ('20250902000002'),
    ('20250903000000'),
    ('20250903000001'),
    ('20250903000004'),
    ('20250912000000'),
    ('20250912000001'),
    ('20250913000000'),
    ('20250922000000'),
    ('20250924000000'),
    ('20250924000002'),
    ('20250925000000'),
    ('20251003000000'),
    ('20251006000000'),
    ('20251007000001'),
    ('20251008000001'),
    ('20251009000002'),
    ('20251017000000'),
    ('20251105000000');
