"""The catch-up notices get an index on txid, built without blocking writes.

The Rust import reads the notices past a saved snapshot. Without an index on
txid every cycle walks all of import_dirty, and that walk sets the length of
the cutover's write freeze.
"""

from __future__ import annotations

import pathlib


_MIGRATION = (
    pathlib.Path(__file__).resolve().parents[2]
    / "hippius_s3"
    / "sql"
    / "migrations"
    / "20261020180000_import_dirty_txid_index.sql"
)


def test_the_index_is_built_concurrently_in_one_statement() -> None:
    up, down = _MIGRATION.read_text().split("-- migrate:down", 1)

    assert up.startswith("-- migrate:up transaction:false")
    statements = [line for line in up.splitlines() if line.strip() and not line.lstrip().startswith("--")]
    # One statement: a multi-statement body would run in an implicit transaction.
    assert statements == ["CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_import_dirty_txid ON import_dirty (txid);"]
    assert "DROP INDEX IF EXISTS idx_import_dirty_txid;" in down


def test_it_runs_after_the_table_exists() -> None:
    migrations = _MIGRATION.parent
    assert (migrations / "20261007160000_import_dirty.sql").exists()
    assert _MIGRATION.name > "20261007160000_import_dirty.sql"
