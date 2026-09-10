"""`objects` must be locked before `object_versions`. Never the reverse.

storage_usage_objects_update_trigger takes a FOR NO KEY UPDATE on the outgoing version while its
firing statement already holds the `objects` row -- that lock is what stops the counter over-billing
under concurrent overwrites (20260910180000). The cost is a lock-order invariant: a transaction that
locks `object_versions` first and `objects` second deadlocks against every single reserve.

No current path does. The only transaction touching both -- delete_object_endpoint.py -- locks the
objects row up front via lock_object_and_get_version (`FOR UPDATE OF o`), deliberately, with a
comment saying so. Verified empirically too: a six-path adversarial probe over 2,880 operations on
one contended object found zero deadlocks, while a control that injects the reverse order produces
236.

This test is what stops the next transaction breaking it, because the failure would not show up in
any single-threaded test -- only as production deadlocks under concurrency.
"""

from __future__ import annotations

import pathlib
import re


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]

# Statements that take a row lock on object_versions (an UPDATE/DELETE locks the row it touches).
OBJECT_VERSIONS_WRITERS = frozenset(
    {
        "clear_version_lock_after_bypass",
        "create_migration_version",
        "delete_version_and_parts",
        "set_object_version_address",
        "set_object_version_address_if_null",
        "set_object_version_lock",
        "soft_delete_object_version",
        "update_object_metadata",
        "update_object_version_body_blake3",
        "update_object_version_envelope",
        "update_object_version_metadata",
    }
)

# Statements that take a row lock on objects.
OBJECTS_WRITERS = frozenset(
    {
        "abort_cleanup_orphan_version",
        "hard_delete_object",
        "insert_delete_marker",
        "purge_soft_delete_objects_batch",
        "repoint_current_version_after_delete",
        "soft_delete_object",
        "swap_current_version_cas",
        "upsert_object_basic",
        "upsert_object_multipart",
        "upsert_object_with_cid",
    }
)

# Statements that take an explicit objects row lock, satisfying the invariant for whatever follows.
OBJECTS_LOCKERS = frozenset({"lock_object_and_get_version"})

_TX = re.compile(r"\btransaction\(\)")
_QUERY = re.compile(r'get_query\(\s*"([a-z0-9_]+)"\s*\)')


def _transaction_blocks() -> list[tuple[pathlib.Path, int, list[str]]]:
    """Every `transaction()` block in the app sources, with the query names it runs, in order.

    Indentation-based rather than AST-based on purpose: it must also catch a block that calls a
    query through a helper on the same line, which an AST walk of `async with` bodies would miss.
    """
    blocks: list[tuple[pathlib.Path, int, list[str]]] = []
    for path in sorted((REPO_ROOT / "hippius_s3").rglob("*.py")) + sorted((REPO_ROOT / "workers").rglob("*.py")):
        lines = path.read_text(errors="ignore").splitlines()
        for i, line in enumerate(lines):
            if not _TX.search(line):
                continue
            indent = len(line) - len(line.lstrip())
            names: list[str] = []
            for cur in lines[i + 1 :]:
                if cur.strip() and (len(cur) - len(cur.lstrip())) <= indent:
                    break
                names.extend(_QUERY.findall(cur))
            blocks.append((path.relative_to(REPO_ROOT), i + 1, names))
    return blocks


def test_the_scanner_actually_finds_the_known_transaction() -> None:
    """A scanner pointed at nothing returns a clean sweep, which is the most dangerous kind of pass.

    This exact mistake happened while investigating the bug: a stray `cd` left the analysis scanning
    a directory with no Python files and it cheerfully reported zero transactions. So anchor on a
    block that is known to exist.
    """
    blocks = _transaction_blocks()

    assert blocks, "found no transaction() blocks at all -- the scanner is looking in the wrong place"
    assert any(
        "delete_object_endpoint" in str(p) and "soft_delete_object_version" in names for p, _, names in blocks
    ), "the versioned-delete transaction was not found; the scanner is broken, not the code"


def test_no_transaction_locks_object_versions_before_objects() -> None:
    offenders = []
    for path, line, names in _transaction_blocks():
        ov = [i for i, n in enumerate(names) if n in OBJECT_VERSIONS_WRITERS]
        ob = [i for i, n in enumerate(names) if n in OBJECTS_WRITERS]
        lockers = [i for i, n in enumerate(names) if n in OBJECTS_LOCKERS]
        if not (ov and ob):
            continue
        # Safe if an explicit objects lock is taken before the first object_versions write, or if
        # every objects write already precedes it.
        if lockers and min(lockers) < min(ov):
            continue
        if max(ob) < min(ov):
            continue
        offenders.append(f"{path}:{line} -> {names}")

    assert not offenders, (
        "These transactions lock object_versions before objects, which deadlocks against every "
        "reserve (storage_usage_objects_update_trigger locks the outgoing version while holding "
        "the objects row). Take an objects row lock first -- lock_object_and_get_version does "
        f"this -- or reorder the statements:\n  " + "\n  ".join(offenders)
    )


def test_the_versioned_delete_transaction_still_locks_objects_up_front() -> None:
    """Named explicitly because it is the only transaction that touches both tables, so it is the
    one that would break first, and its safety currently rests on a single `FOR UPDATE OF o`."""
    lock_sql = (REPO_ROOT / "hippius_s3/sql/queries/lock_object_and_get_version.sql").read_text()

    assert "FOR UPDATE OF o" in lock_sql, "lock_object_and_get_version no longer locks the objects row"

    matching = [
        names
        for path, _, names in _transaction_blocks()
        if "delete_object_endpoint" in str(path) and "soft_delete_object_version" in names
    ]
    assert matching, "the versioned-delete transaction disappeared; re-check the invariant by hand"
    for names in matching:
        assert names.index("lock_object_and_get_version") < names.index("soft_delete_object_version")
