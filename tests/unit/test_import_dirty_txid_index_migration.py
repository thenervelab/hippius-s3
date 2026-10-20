"""The catch-up notices get an index on (txid, bucket_id, object_key).

The Rust import pages through the notices past a saved snapshot in that order.
On production the index is built out of band, CONCURRENTLY, before the deploy:
every API pod runs `dbmate up` at once, and a build there would race, outlast
the startup probe, or block every object write. So the migration builds it only
on a small table and does nothing on a large one.
"""

from __future__ import annotations

import pathlib
import re


_MIGRATIONS = pathlib.Path(__file__).resolve().parents[2] / "hippius_s3" / "sql" / "migrations"
_MIGRATION = _MIGRATIONS / "20261020180000_import_dirty_txid_index.sql"


def _halves() -> tuple[str, str]:
    up, down = _MIGRATION.read_text().split("-- migrate:down", 1)
    return up, down


def _code(text: str) -> str:
    return "\n".join(line for line in text.splitlines() if not line.lstrip().startswith("--"))


def test_up_builds_only_on_a_small_table_and_bounds_the_lock() -> None:
    up = _code(_halves()[0])

    assert "SET LOCAL lock_timeout = '3s';" in up
    assert "CONCURRENTLY" not in up, "a CONCURRENTLY build belongs out of band, not in dbmate"
    # The build sits inside the size guard, so no deploy builds it on production.
    guard = up.index("LIMIT 100000) t) < 100000 THEN")
    build = up.index("CREATE INDEX idx_import_dirty_txid ON import_dirty (txid, bucket_id, object_key);")
    assert guard < build < up.index("END IF;")
    assert "to_regclass('idx_import_dirty_txid') IS NULL" in up


def test_down_drops_concurrently_in_one_statement() -> None:
    down = _halves()[1]

    options, body = down.split("\n", 1)
    assert options == " transaction:false"
    statements = [s.strip() for s in _code(body).split(";") if s.strip()]
    assert statements == ["DROP INDEX CONCURRENTLY IF EXISTS idx_import_dirty_txid"]


def test_the_out_of_band_build_in_the_header_matches_the_index() -> None:
    header = _halves()[0]
    built = re.search(
        r"CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_import_dirty_txid\s*--\s*ON import_dirty \(([^)]*)\)",
        header,
    )
    assert built, "the header gives the exact out-of-band statement"
    assert built.group(1) == "txid, bucket_id, object_key"


def test_it_runs_after_the_table_and_the_marker_trigger() -> None:
    assert (_MIGRATIONS / "20261007160000_import_dirty.sql").exists()
    assert (_MIGRATIONS / "20261020120000_import_dirty_bucket_public_objects.sql").exists()
    newer = [p.name for p in _MIGRATIONS.glob("*.sql") if p.name > _MIGRATION.name]
    assert newer == [], f"a migration dated after this one hides it from the worker gate: {newer}"
