-- migrate:up

-- Queryable index of which parts are materialized on the cache FS. Written by the
-- upload/download pipelines when a part's meta.json lands; deleted by the janitor after
-- eviction; reconciled by the walk sweep (backfill missing rows, drop rows whose dir is
-- gone). ADVISORY by design: a missing row only delays eviction until the sweep backfills
-- it, a stale row costs one stat — neither direction can cause an unsafe delete, because
-- the per-part replication gate runs at delete time regardless.
CREATE TABLE IF NOT EXISTS fs_cache_inventory (
    object_id      TEXT        NOT NULL,
    object_version BIGINT      NOT NULL,
    part_number    BIGINT      NOT NULL,
    cached_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (object_id, object_version, part_number)
);

-- Oldest-first candidate scans and the keyset cursor both walk this order.
CREATE INDEX IF NOT EXISTS fs_cache_inventory_cached_at
    ON fs_cache_inventory (cached_at, object_id, object_version, part_number);

-- Single-instance janitor's durable state (keyset cursor, shard counter). One row per key;
-- JSONB so cursor shape can evolve without migrations.
CREATE TABLE IF NOT EXISTS janitor_state (
    key        TEXT        PRIMARY KEY,
    value      JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- migrate:down

DROP TABLE IF EXISTS janitor_state;
DROP INDEX IF EXISTS fs_cache_inventory_cached_at;
DROP TABLE IF EXISTS fs_cache_inventory;
