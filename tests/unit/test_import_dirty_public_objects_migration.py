"""The whole-bucket GetObject marker has to leave a catch-up notice.

bucket_public_objects arrived after the notice migration, so it needs its own
trigger. Without it a marker put or removed is a change Rust never hears
about, and a bucket Python made private stays public after cutover.
"""

from __future__ import annotations

import pathlib


_MIGRATION = (
    pathlib.Path(__file__).resolve().parents[2]
    / "hippius_s3"
    / "sql"
    / "migrations"
    / "20261020120000_import_dirty_bucket_public_objects.sql"
)


def test_the_marker_notes_its_bucket_and_bounds_the_lock() -> None:
    up, down = _MIGRATION.read_text().split("-- migrate:down", 1)

    assert "SET LOCAL lock_timeout = '3s';" in up
    assert "CREATE TRIGGER import_dirty_bucket_public_objects" in up
    assert "AFTER INSERT OR UPDATE OR DELETE ON bucket_public_objects" in up
    # The bucket-level notice function, as bucket_acls and
    # bucket_public_prefixes use: the marker is a bucket setting.
    assert "EXECUTE FUNCTION import_dirty_bucket_row()" in up

    assert "SET LOCAL lock_timeout = '3s';" in down
    assert "DROP TRIGGER IF EXISTS import_dirty_bucket_public_objects ON bucket_public_objects;" in down


def test_it_runs_after_the_table_and_the_notice_function_exist() -> None:
    migrations = _MIGRATION.parent
    assert (migrations / "20261007160000_import_dirty.sql").exists()
    assert (migrations / "20261012130000_bucket_public_objects.sql").exists()
    assert _MIGRATION.name > "20261012130000_bucket_public_objects.sql"
