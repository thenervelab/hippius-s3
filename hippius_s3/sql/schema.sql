\restrict msKcG2aq3CN88RTem08SSYW5fMfxTw5sUbs0NnVV3fWngwDng2LQLlYciXbdaoA

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

SET default_tablespace = '';

SET default_table_access_method = heap;

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
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    version character varying NOT NULL
);


--
-- Name: encryption_keys id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.encryption_keys ALTER COLUMN id SET DEFAULT nextval('public.encryption_keys_id_seq'::regclass);


--
-- Name: encryption_keys encryption_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.encryption_keys
    ADD CONSTRAINT encryption_keys_pkey PRIMARY KEY (id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: idx_encryption_keys_subaccount_created; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_encryption_keys_subaccount_created ON public.encryption_keys USING btree (subaccount_id, created_at DESC);


--
-- PostgreSQL database dump complete
--

\unrestrict msKcG2aq3CN88RTem08SSYW5fMfxTw5sUbs0NnVV3fWngwDng2LQLlYciXbdaoA


--
-- Dbmate schema migrations
--

INSERT INTO public.schema_migrations (version) VALUES
    ('20241201000001'),
    ('20241202000001');
