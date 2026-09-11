"""Static guard: nothing may lock an object_versions row before its objects row.

The storage-usage trigger on `objects` reads the outgoing and incoming version sizes under
FOR NO KEY UPDATE, and it runs while its statement already holds the objects row. Its lock order is
therefore objects -> object_versions, on EVERY overwrite. A transaction that takes them the other
way round closes a cycle and Postgres breaks it by failing somebody's PUT or DELETE.

Verified, not theorised: injecting the forbidden order against a concurrent reserve produces
DeadlockDetectedError (40P01). See
tests/integration/test_storage_usage_rollup.py::test_taking_the_version_row_before_the_objects_row_deadlocks.

Every current path conforms, and two of them do so deliberately -- lock_object_and_get_version.sql
(`FOR UPDATE OF o`) and abort_cleanup_orphan_version.sql (a leading objects-locking CTE). This test
exists so the NEXT one does too, because nothing about `UPDATE object_versions ...` followed by
`UPDATE objects ...` looks like it touches billing.
"""

import ast
import re
from pathlib import Path


_ROOT = Path(__file__).parents[2]
_QUERIES = _ROOT / "hippius_s3" / "sql" / "queries"

# `\bobjects\b` cannot match "object_versions", so these stay disjoint.
_WRITES_VERSIONS = re.compile(r"\b(?:UPDATE|DELETE\s+FROM|INSERT\s+INTO)\s+object_versions\b", re.I)
_WRITES_OBJECTS = re.compile(r"\b(?:UPDATE|DELETE\s+FROM|INSERT\s+INTO)\s+objects\b", re.I)
_LOCKS_OBJECTS = re.compile(r"\bFROM\s+objects\b[\s\S]*?\bFOR\s+(?:NO\s+KEY\s+)?UPDATE\b", re.I)


def _classify(sql: str) -> str | None:
    """'objects' if the statement locks or writes the objects row, 'versions' if only versions.

    An UPDATE/INSERT/DELETE on `objects` IS an objects row lock, so it counts the same as an
    explicit FOR UPDATE: either way the transaction holds that row from then on.
    """
    if _LOCKS_OBJECTS.search(sql) or _WRITES_OBJECTS.search(sql):
        return "objects"
    if _WRITES_VERSIONS.search(sql):
        return "versions"
    return None


def _query_kinds() -> dict[str, str]:
    return {p.stem: kind for p in _QUERIES.glob("*.sql") if (kind := _classify(p.read_text())) is not None}


def _events(node: ast.AST, kinds: dict[str, str]) -> list[tuple[int, str, str]]:
    """Every objects/object_versions touch inside a subtree, in source order."""
    found: list[tuple[int, str, str]] = []
    for child in ast.walk(node):
        # get_query("name")
        if (
            isinstance(child, ast.Call)
            and isinstance(child.func, ast.Name)
            and child.func.id == "get_query"
            and child.args
            and isinstance(child.args[0], ast.Constant)
            and isinstance(child.args[0].value, str)
        ):
            name = child.args[0].value
            if name in kinds:
                found.append((child.lineno, kinds[name], name))
        # An inline SQL string literal.
        elif isinstance(child, ast.Constant) and isinstance(child.value, str):
            kind = _classify(child.value)
            if kind is not None:
                found.append((child.lineno, kind, child.value.split()[0:3] and " ".join(child.value.split()[:3])))
    return sorted(found)


def _transaction_blocks() -> list[tuple[Path, int, list[tuple[int, str, str]]]]:
    """Every `async with ... transaction()` block, with the table touches inside it."""
    kinds = _query_kinds()
    blocks: list[tuple[Path, int, list[tuple[int, str, str]]]] = []

    roots = [_ROOT / "hippius_s3", _ROOT / "workers", _ROOT / "cacher"]
    for path in sorted(p for root in roots if root.is_dir() for p in root.rglob("*.py")):
        tree = ast.parse(path.read_text())
        for node in ast.walk(tree):
            if not isinstance(node, (ast.With, ast.AsyncWith)):
                continue
            opens_transaction = any(
                isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute) and call.func.attr == "transaction"
                for item in node.items
                for call in ast.walk(item.context_expr)
            )
            if not opens_transaction:
                continue
            events = [e for stmt in node.body for e in _events(stmt, kinds)]
            if events:
                blocks.append((path, node.lineno, events))
    return blocks


def test_the_query_classifier_recognises_the_known_paths() -> None:
    """Guard the guard: a classifier that silently matches nothing would pass every assertion.

    This has already bitten once during this investigation -- a scan run from the wrong directory
    reported "0 transactions" and looked like a clean bill of health.
    """
    kinds = _query_kinds()

    assert kinds["lock_object_and_get_version"] == "objects"
    assert kinds["abort_cleanup_orphan_version"] == "objects"
    assert kinds["soft_delete_object"] == "objects"
    assert kinds["repoint_current_version_after_delete"] == "objects"
    assert kinds["update_object_version_metadata"] == "versions"
    assert kinds["soft_delete_object_version"] == "versions"
    # The four data-modifying-CTE upserts write both tables in one statement, and the objects half
    # takes the row lock, so they must classify as `objects`.
    for name in ("upsert_object_basic", "upsert_object_multipart", "upsert_object_with_cid", "insert_delete_marker"):
        assert kinds[name] == "objects", name


def test_transaction_scan_finds_the_paths_it_is_supposed_to_check() -> None:
    """Same reason: prove the AST walk actually reaches the code before trusting its verdict."""
    blocks = _transaction_blocks()
    files = {path.name for path, _, _ in blocks}

    assert "delete_object_endpoint.py" in files, "the versioned-DELETE transaction was not found"
    assert "object_writer.py" in files, "the PUT reserve transaction was not found"


def test_no_transaction_locks_a_version_row_before_its_objects_row() -> None:
    """The invariant. A transaction touching BOTH tables must touch `objects` first.

    If you are here because this failed: your transaction writes object_versions and then objects.
    That is the order that deadlocks against every concurrent overwrite of the same key. Take the
    objects row up front -- `lock_object_and_get_version.sql` is the pattern, and
    delete_object_endpoint.py shows it in use.
    """
    offenders = []
    for path, lineno, events in _transaction_blocks():
        kinds = [kind for _, kind, _ in events]
        if "versions" in kinds and "objects" in kinds and kinds[0] == "versions":
            offenders.append(f"{path.relative_to(_ROOT)}:{lineno} -> {[(k, n) for _, k, n in events]}")

    assert not offenders, "transactions locking object_versions before objects:\n" + "\n".join(offenders)
