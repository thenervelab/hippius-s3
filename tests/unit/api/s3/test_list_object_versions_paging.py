"""Pagination tests for ``_collect_page`` against a fake that HONOURS the cursor.

The existing `_FakePool` in test_list_object_versions_xml.py returns the same fixed rows for any
arguments, so the multi-batch `while True` loop in `_collect_page` executes exactly once and the
cursor arithmetic is never exercised. Every keyset bug is therefore invisible to it.

`_CursorPool` below reimplements the marker clause of `list_object_versions.sql` faithfully —
crucially including the asymmetry that makes the key marker EXCLUSIVE on the key when no version
marker is supplied, and INCLUSIVE (``object_version <= $4``) when one is. That asymmetry is what
the delimiter-collapse cursor jump has to be paired with correctly.
"""

from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any

import pytest

from hippius_s3.api.s3.buckets import list_object_versions_endpoint as mod


TS = datetime(2026, 8, 22, 9, 32, 42, tzinfo=timezone.utc)


def _row(key: str, version: int, current: int, *, marker: bool = False) -> dict[str, Any]:
    return {
        "object_key": key,
        "object_version": version,
        "is_delete_marker": marker,
        "size_bytes": 0 if marker else 11,
        "md5_hash": None if marker else "abc",
        "body_blake3": None,
        "last_modified": TS,
        "current_object_version": current,
    }


class _CursorPool:
    """Mirrors list_object_versions.sql's WHERE/ORDER BY/LIMIT, including the marker asymmetry."""

    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows
        self.batches: list[tuple[Any, ...]] = []

    async def fetch(self, _query: str, *args: Any) -> list[Any]:
        _bucket_id, prefix, ckey, cversion, limit, upper, current_only = args
        self.batches.append(args)
        if len(self.batches) > 50:
            raise AssertionError("pagination did not terminate — cursor is not advancing")

        out = []
        for r in self.rows:
            key, ver = r["object_key"], r["object_version"]
            if prefix is not None and not key.startswith(prefix):
                continue
            if upper is not None and not key < upper:
                continue
            if current_only and ver != r["current_object_version"]:
                continue
            if ckey is not None:
                # $3 alone is exclusive on the key; with $4 the same key resumes at <= $4.
                if not (key > ckey or (key == ckey and cversion is not None and ver <= cversion)):
                    continue
            out.append(r)

        out.sort(key=lambda r: (r["object_key"], -r["object_version"]))
        return out[:limit]


async def _collect(rows: list[dict[str, Any]], **kw: Any) -> tuple[list[Any], bool, Any, Any]:
    pool = _CursorPool(rows)
    kw.setdefault("prefix", None)
    kw.setdefault("delimiter", None)
    kw.setdefault("key_marker", None)
    kw.setdefault("version_marker", None)
    kw.setdefault("current_only", False)
    entries, truncated, nkey, nver = await mod._collect_page(pool, "bkt-1", **kw)
    return entries, truncated, nkey, nver


def _names(entries: list[tuple[str, Any]]) -> list[str]:
    return [e[1] if e[0] == "prefix" else e[1]["object_key"] for e in entries]


# --- The delimiter-collapse cursor jump ----------------------------------------------------


@pytest.mark.asyncio
async def test_key_equal_to_the_group_resume_string_is_not_skipped() -> None:
    """`_prefix_resume('v1/')` is 'v10' — which can be a real key.

    `_prefix_resume` yields an INCLUSIVE boundary (list_objects.sql resumes on `>=`), but this
    query's key marker is EXCLUSIVE on the key when no version marker is given. Pairing the two
    dropped the key whose name IS the resume string, and reported IsTruncated=false while doing it
    — a silent short read. Delimiter '/' makes such collisions routine: 'v1/' -> 'v10',
    '2026/' -> '20260'.
    """
    # target=2 -> batch_limit=3, and the group is exactly 3 rows, so the first batch is entirely
    # consumed by the collapsed group. Reaching "v10" REQUIRES the cursor jump to be correct;
    # sizing the group smaller lets every row arrive in one batch and the test passes either way.
    rows = [
        _row("v1/a", 1, 1),
        _row("v1/b", 1, 1),
        _row("v1/c", 1, 1),
        _row("v10", 1, 1),
    ]
    entries, truncated, _, _ = await _collect(rows, delimiter="/", target=2)

    assert _names(entries) == ["v1/", "v10"]
    assert truncated is False


@pytest.mark.asyncio
async def test_group_spanning_a_whole_batch_still_yields_the_successor_key() -> None:
    """The collapsed group fills an entire SQL batch, so the jump is the only way forward."""
    rows = [_row(f"a/{i}", 1, 1) for i in range(6)] + [_row("a0", 1, 1), _row("b", 1, 1)]
    entries, truncated, _, _ = await _collect(rows, delimiter="/", target=3)

    assert _names(entries) == ["a/", "a0", "b"]
    assert truncated is False


@pytest.mark.asyncio
async def test_multiple_consecutive_groups_each_advance() -> None:
    rows = [
        _row("a/1", 1, 1),
        _row("a/2", 1, 1),
        _row("b/1", 1, 1),
        _row("b/2", 1, 1),
        _row("c", 1, 1),
    ]
    entries, truncated, _, _ = await _collect(rows, delimiter="/", target=10)

    assert _names(entries) == ["a/", "b/", "c"]
    assert truncated is False


# --- Multi-version keys across page boundaries ---------------------------------------------


@pytest.mark.asyncio
async def test_versions_of_one_key_split_across_pages_resume_exactly() -> None:
    """A key with more versions than max-keys must resume mid-key without repeating or skipping."""
    rows = [_row("k", v, 3) for v in (3, 2, 1)] + [_row("z", 1, 1)]

    page1, trunc1, nkey, nver = await _collect(rows, target=2)
    assert [e[1]["object_version"] for e in page1] == [3, 2]
    assert trunc1 is True
    assert (nkey, nver) == ("k", 1)

    page2, trunc2, _, _ = await _collect(rows, target=2, key_marker=nkey, version_marker=nver)
    assert [(e[1]["object_key"], e[1]["object_version"]) for e in page2] == [("k", 1), ("z", 1)]
    assert trunc2 is False


@pytest.mark.asyncio
async def test_full_walk_visits_every_row_exactly_once() -> None:
    """Drive the whole pagination loop and assert the union is the input, with no duplicates."""
    rows = [_row(f"k{i}", v, 3) for i in range(5) for v in (3, 2, 1)]
    seen: list[tuple[str, int]] = []
    key_marker: str | None = None
    version_marker: int | None = None

    for _ in range(50):
        entries, truncated, key_marker, version_marker = await _collect(
            rows, target=2, key_marker=key_marker, version_marker=version_marker
        )
        seen.extend((e[1]["object_key"], e[1]["object_version"]) for e in entries)
        if not truncated:
            break
    else:
        raise AssertionError("pagination never terminated")

    expected = [(r["object_key"], r["object_version"]) for r in rows]
    assert sorted(seen) == sorted(expected)
    assert len(seen) == len(set(seen)), "a row was emitted on two different pages"


# --- Degenerate and boundary inputs --------------------------------------------------------


@pytest.mark.asyncio
async def test_empty_bucket() -> None:
    entries, truncated, nkey, nver = await _collect([], target=10)
    assert entries == []
    assert truncated is False
    assert nkey is None and nver is None


@pytest.mark.asyncio
async def test_exactly_max_keys_is_not_truncated() -> None:
    rows = [_row("a", 1, 1), _row("b", 1, 1)]
    entries, truncated, _, _ = await _collect(rows, target=2)
    assert _names(entries) == ["a", "b"]
    assert truncated is False


@pytest.mark.asyncio
async def test_one_over_max_keys_is_truncated_at_the_right_row() -> None:
    rows = [_row("a", 1, 1), _row("b", 1, 1), _row("c", 1, 1)]
    entries, truncated, nkey, nver = await _collect(rows, target=2)
    assert _names(entries) == ["a", "b"]
    assert truncated is True
    assert (nkey, nver) == ("c", 1)


@pytest.mark.asyncio
async def test_delete_markers_count_toward_max_keys_and_paginate() -> None:
    rows = [_row("a", 2, 2, marker=True), _row("a", 1, 2), _row("b", 1, 1)]
    entries, truncated, nkey, nver = await _collect(rows, target=2)
    assert [e[0] for e in entries] == ["marker", "version"]
    assert truncated is True
    assert (nkey, nver) == ("b", 1)


@pytest.mark.asyncio
async def test_prefix_bounds_the_walk() -> None:
    rows = [_row("aa", 1, 1), _row("ab", 1, 1), _row("b", 1, 1)]
    entries, truncated, _, _ = await _collect(rows, prefix="a", target=10)
    assert _names(entries) == ["aa", "ab"]
    assert truncated is False


@pytest.mark.asyncio
async def test_current_only_returns_one_entry_per_key() -> None:
    rows = [_row("k", 3, 3), _row("k", 2, 3), _row("k", 1, 3)]
    entries, truncated, _, _ = await _collect(rows, target=10, current_only=True)
    assert [e[1]["object_version"] for e in entries] == [3]
    assert truncated is False
