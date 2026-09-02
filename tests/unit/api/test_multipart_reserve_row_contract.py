"""Every key multipart.py reads off the reserve row must be a column that query actually returns.

This exists because of a live 500. The Object Lock default-retention write at
CreateMultipartUpload read `upsert_result["object_version"]`, but
`upsert_object_multipart.sql` returns that column as `current_object_version` — the name
multipart.py already uses for the same row twenty lines earlier. Result:

    InternalError ... Error initiating multipart upload: 'object_version'

on every CreateMultipartUpload against a bucket carrying a default retention.

The reason it reached a deployment is the part worth guarding. A lock-enabled bucket with NO
default rule never reaches that code — `lock_for_new_version` returns None and the
whole block is skipped — so the unit suite, the e2e suite and every CI job stayed green. It was
found only by re-running an acceptance probe against the deployed build.

A behavioural test would have to stand up the entire initiate path (KMS, envelope write, DB
transaction) just to catch a typo'd dict key. This asserts the contract directly instead: it reads
the real SQL and the real source, and fails on ANY key that the query does not return — including
ones added long after this was written. That is the class of bug, not just the instance.
"""

from __future__ import annotations

import re
from pathlib import Path


REPO = Path(__file__).resolve().parents[3]
SQL = REPO / "hippius_s3" / "sql" / "queries" / "upsert_object_multipart.sql"
SOURCE = REPO / "hippius_s3" / "api" / "s3" / "multipart.py"


def _returned_columns(sql_text: str) -> set[str]:
    """Column names the FINAL top-level SELECT projects.

    The file is a CTE chain, so the trailing SELECT after the last `)` is the one whose columns
    asyncpg hands back. Aliased as `u.object_id` / `iv.content_type`, so the name is the part
    after the dot.
    """
    tail = sql_text[sql_text.rindex("\n)") :]
    select_body = tail[tail.index("SELECT") + len("SELECT") : tail.index("FROM")]
    cols: set[str] = set()
    for raw in select_body.split(","):
        token = raw.strip().rstrip(",").split()[0] if raw.strip() else ""
        if not token:
            continue
        cols.add(token.split(".")[-1].strip().lower())
    return cols


def _keys_read_from(source_text: str, variable: str) -> set[str]:
    return set(re.findall(rf'{variable}\[\s*"([^"]+)"\s*\]', source_text))


def test_upsert_object_multipart_returns_current_object_version() -> None:
    """Pins the specific name. `object_version` is the inner CTE's column, not the projected one."""
    cols = _returned_columns(SQL.read_text())
    assert "current_object_version" in cols
    assert "object_version" not in cols, (
        "upsert_object_multipart now projects `object_version` — the guard below and the call "
        "sites that read `current_object_version` need revisiting together"
    )


def test_every_reserve_row_key_multipart_reads_is_actually_returned() -> None:
    """The class guard: no key read off the reserve row may be absent from the query's output."""
    cols = _returned_columns(SQL.read_text())
    read = _keys_read_from(SOURCE.read_text(), "upsert_result")

    assert read, "found no upsert_result[...] reads — the regex or the variable name drifted"
    missing = {k for k in read if k.lower() not in cols}
    assert not missing, (
        f"multipart.py reads {sorted(missing)} off the upsert_object_multipart reserve row, but "
        f"that query only returns {sorted(cols)}. Each one is a KeyError -> 500 on the path that "
        f"reaches it."
    )
