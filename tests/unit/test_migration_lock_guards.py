"""Any migration that locks a hot table must bound how long it waits for the lock.

Production runs `lock_timeout = 0` -- unbounded. So a migration that takes ACCESS EXCLUSIVE on a
table the data plane is writing does not merely pause that table: it WAITS behind whatever query is
in flight, and every request arriving meanwhile queues behind the pending lock request. On the big
tables that is a data-plane stall for as long as the wait lasts, and nothing bounds it.

`SET LOCAL lock_timeout` converts that into a failed migration, which is the correct outcome -- the
migration job retries, and a failure inside dbmate's single transaction rolls back the whole file
INCLUDING its schema_migrations row, so there is no half-applied state.

This has bitten this repo before (see hippius_s3/sql/migrations/ history around 20260822120001 and
20260902100000), and it bit again in this release: 20260913090000 added a column to
`multipart_uploads` -- 216.8M rows, 68 GB, ~8.5 writes/s measured on production -- with no guard.

The check is deliberately mechanical rather than clever. A migration either says the words or it
does not, and a reviewer should not have to hold the prod row counts in their head.
"""

from __future__ import annotations

import pathlib
import re


_MIGRATIONS = pathlib.Path(__file__).resolve().parents[2] / "hippius_s3" / "sql" / "migrations"

# Tables where an ACCESS EXCLUSIVE wait is a data-plane stall. Row counts are production, measured.
_HOT_TABLES = (
    "objects",  # ~166.8M rows / 165 GB
    "object_versions",  # ~170.6M rows / 91 GB
    "parts",  # ~180.1M rows / 73 GB
    "part_chunks",  # ~208.7M rows / 40 GB
    "chunk_backend",  # ~413.1M rows / 77 GB
    "multipart_uploads",  # ~216.8M rows / 68 GB, ~8.5 writes/s
)

# Statements that take a table-level lock strong enough to queue the data plane behind them.
# `\b` keeps `objects` from matching `object_versions`.
_LOCKING = re.compile(
    r"\b(?:ALTER\s+TABLE|CREATE\s+TRIGGER|DROP\s+TRIGGER|CREATE\s+INDEX|DROP\s+INDEX|TRUNCATE)\b"
    r"[\s\S]{0,200}?\b(?:" + "|".join(_HOT_TABLES) + r")\b",
    re.I,
)
_GUARDED = re.compile(r"\bSET\s+LOCAL\s+lock_timeout\b", re.I)
# CREATE INDEX CONCURRENTLY cannot run in a transaction, so SET LOCAL does not apply to it; those
# migrations carry `transaction:false` and take a weaker lock by design.
_CONCURRENTLY = re.compile(r"\bCONCURRENTLY\b", re.I)


def _up_half(text: str) -> str:
    return text.split("-- migrate:down", 1)[0]


# BASELINE. 42 migrations predate this convention and are already applied in every environment,
# including production -- editing them changes nothing anywhere and is pure churn. The guard
# therefore covers migrations from this release's boundary onward, which is exactly the set that can
# still be applied to a production database for the first time.
#
# Do NOT move this forward to make a new migration pass. If a migration dated after this needs a
# table lock, it needs the guard; that is the whole point.
_GUARD_FROM = "20260910000000"


def test_every_migration_that_locks_a_hot_table_bounds_the_wait() -> None:
    offenders: list[str] = []

    for path in sorted(_MIGRATIONS.glob("*.sql")):
        if path.name < _GUARD_FROM:
            continue
        up = _up_half(path.read_text())
        if not _LOCKING.search(up):
            continue
        if _CONCURRENTLY.search(up):
            continue
        if not _GUARDED.search(up):
            offenders.append(path.name)

    assert not offenders, (
        "these migrations take a table lock on a hot table without `SET LOCAL lock_timeout = '3s'`. "
        "Production runs lock_timeout = 0, so each one can wait indefinitely while queueing the "
        "data plane behind it:\n  " + "\n  ".join(offenders)
    )


def test_the_detector_actually_matches_the_statements_it_claims_to() -> None:
    """Guard the guard: a regex that matches nothing gives a clean sweep, which is the worst pass.

    Exactly this class of false-clean has bitten twice in this work already, so the detector is
    asserted against the real statements rather than trusted.
    """
    assert _LOCKING.search("ALTER TABLE multipart_uploads ADD COLUMN x BOOLEAN NOT NULL DEFAULT FALSE;")
    assert _LOCKING.search("CREATE TRIGGER foo AFTER UPDATE ON object_versions FOR EACH ROW EXECUTE FUNCTION f();")
    assert _LOCKING.search("DROP TRIGGER IF EXISTS foo ON objects;")
    # Must NOT flag a lock on some unrelated small table.
    assert not _LOCKING.search("ALTER TABLE storage_delta_ledger SET (fillfactor = 70);")
    # `\bobjects\b` must not match object_versions' name as a substring of something else.
    assert not _LOCKING.search("ALTER TABLE bucket_storage_usage SET (fillfactor = 70);")
    assert _GUARDED.search("SET LOCAL lock_timeout = '3s';")
    assert not _GUARDED.search("SET lock_timeout = '3s';")  # session-scoped leaks across migrations


def test_at_least_one_migration_is_actually_scanned() -> None:
    """If the glob or the split ever breaks, the suite above passes by examining nothing."""
    scanned = [p.name for p in _MIGRATIONS.glob("*.sql") if _LOCKING.search(_up_half(p.read_text()))]

    assert len(scanned) >= 3, f"only {len(scanned)} migrations matched as hot-table-locking: {scanned}"


def test_the_baseline_leaves_this_releases_migrations_in_scope() -> None:
    """A cutoff is only honest if it does not quietly exclude the work it was introduced alongside.

    The storage-rollup migration locks `objects` and `object_versions` and is guarded; if a future
    edit pushed _GUARD_FROM past it, the guard would still pass while covering nothing that matters.
    """
    in_scope = [p.name for p in sorted(_MIGRATIONS.glob("*.sql")) if p.name >= _GUARD_FROM]

    assert "20260910120000_storage_usage_rollup.sql" in in_scope, (
        "_GUARD_FROM has been moved past the rollup migration, which takes ACCESS EXCLUSIVE on both "
        "166M-row tables -- the guard is now covering nothing load-bearing"
    )
    assert len(in_scope) >= 5, f"only {len(in_scope)} migrations are in scope; the cutoff looks wrong"
