"""The catch-up notice migration has to fail closed, on every write path.

A table this list misses is a change Rust never hears about. The object stays
wrong after cutover and a key count can still match. The lock guard covers the
wait; this covers the shape.
"""

from __future__ import annotations

import pathlib


_MIGRATION = (
    pathlib.Path(__file__).resolve().parents[2]
    / "hippius_s3"
    / "sql"
    / "migrations"
    / "20261007160000_import_dirty.sql"
)

# Every writer that can change what the importer copies. chunk_backend is the
# one that retries a key skipped because its file was still on an ingest SSD.
_TRIGGERS = (
    "zz_import_dirty_objects",
    "zz_import_dirty_object_versions",
    "import_dirty_object_names",
    "import_dirty_object_acls",
    "import_dirty_parts",
    "import_dirty_chunk_backend",
    "import_dirty_buckets",
    "import_dirty_bucket_acls",
    "import_dirty_bucket_prefixes",
)


def _up(text: str) -> str:
    return text.split("-- migrate:down", 1)[0]


def test_import_dirty_records_every_writer_and_bounds_the_lock() -> None:
    text = _MIGRATION.read_text()
    up = _up(text)

    assert "SET LOCAL lock_timeout = '3s';" in up
    assert "pg_current_xact_id()" in up
    assert "ON CONFLICT (bucket_id, object_key) DO UPDATE" in up
    assert "ON CONFLICT (bucket_id) DO UPDATE" in up
    assert "SECURITY DEFINER" in up
    assert "SET search_path = pg_catalog, public" in up
    for name in _TRIGGERS:
        assert f"CREATE TRIGGER {name}" in up, name
    # AFTER ROW triggers fire in name order. These two have to run after the
    # storage-delta triggers, which lock the version. Locking the notice first
    # deadlocks two finalizes of one key.
    assert "zz_import_dirty_objects" > "objects_storage_delta_upd"
    assert "zz_import_dirty_object_versions" > "object_versions_storage_delta_upd"
    for table in (
        "objects",
        "object_versions",
        "object_names",
        "object_acls",
        "parts",
        "chunk_backend",
        "buckets",
        "bucket_acls",
        "bucket_public_prefixes",
    ):
        assert f"ON {table}\n" in up or f"ON {table} " in up, table


def test_import_dirty_down_drops_what_up_created() -> None:
    text = _MIGRATION.read_text()
    down = text.split("-- migrate:down", 1)[1]

    assert "SET LOCAL lock_timeout = '3s';" in down
    assert "DROP TABLE IF EXISTS import_dirty;" in down
    assert "DROP TABLE IF EXISTS import_dirty_buckets;" in down
    for name in _TRIGGERS:
        assert f"DROP TRIGGER IF EXISTS {name}" in down, name
