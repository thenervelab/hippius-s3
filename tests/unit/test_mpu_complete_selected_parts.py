"""B3: `mpu_complete(selected_parts=...)` sentinel discipline.

`selected_parts=None` means "the client didn't name a subset — complete every uploaded part".
`selected_parts=[]` means "the client named an EMPTY subset". Those are different intents, and an
empty list must NOT be collapsed into the full-set sentinel: a `[]` that silently becomes "all parts"
would let a caller assemble the entire object while asking for none of it.

These tests drive the real `ObjectWriter.mpu_complete` against a fake asyncpg pool so the ETag/size
filtering and the persisted `completed_part_numbers` reflect exactly what the sentinel logic decides.
"""

from __future__ import annotations

import hashlib
from typing import Any

import pytest

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.utils import get_query
from hippius_s3.writer.object_writer import ObjectWriter


# Two uploaded parts. ETags are `<hex-md5>-<n>`; mpu_complete hashes the concatenated per-part
# digests, so the hex prefixes must be valid even-length hex for bytes.fromhex.
_ETAG_ROWS = [
    {"etag": "aabbccdd-1", "part_number": 1},
    {"etag": "11223344-1", "part_number": 2},
]
_SIZE_ROWS = [
    {"size_bytes": 100, "part_number": 1},
    {"size_bytes": 200, "part_number": 2},
]
_FULL_SIZE = 300  # 100 + 200: the size an accidental "all parts" completion would produce.

# MPU-3: `list_parts_for_version` is SELECT * — the endpoint's already-fetched rows carry BOTH etag
# and size_bytes (ordered by part_number). Passing them lets mpu_complete skip its two re-reads.
_DB_PARTS = [
    {"etag": "aabbccdd-1", "part_number": 1, "size_bytes": 100},
    {"etag": "11223344-1", "part_number": 2, "size_bytes": 200},
]


class _FakeTxn:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


class _FakeConn:
    def __init__(self) -> None:
        # Each entry is (query, args) so a test can inspect what got persisted.
        self.executed: list[tuple[str, tuple[Any, ...]]] = []

    def transaction(self) -> _FakeTxn:
        return _FakeTxn()

    async def execute(self, query: str, *args: Any) -> None:
        self.executed.append((query, args))


class _FakeAcquire:
    def __init__(self, conn: _FakeConn) -> None:
        self._conn = conn

    async def __aenter__(self) -> _FakeConn:
        return self._conn

    async def __aexit__(self, *_exc: Any) -> bool:
        return False


class _FakePool:
    """Just enough asyncpg surface for mpu_complete: two `fetch` queries + a scoped `acquire`."""

    def __init__(self) -> None:
        self.conn = _FakeConn()
        self.fetch_calls = 0

    async def fetch(self, query: str, *_args: Any) -> list[dict[str, Any]]:
        self.fetch_calls += 1
        if query == get_query("get_parts_etags_for_version"):
            return _ETAG_ROWS
        if query == get_query("list_parts_for_version"):
            return _SIZE_ROWS
        raise AssertionError(f"unexpected query in mpu_complete: {query!r}")

    def acquire(self) -> _FakeAcquire:
        return _FakeAcquire(self.conn)


def _writer(tmp_path: Any) -> tuple[ObjectWriter, _FakePool]:
    pool = _FakePool()
    # mpu_complete never touches fs_store, but the constructor requires a usable cache dir; point it
    # at a tmp dir so it doesn't try to create the prod default (/var/lib/hippius).
    fs_store = FileSystemPartsStore(str(tmp_path))
    writer = ObjectWriter(pool=pool, redis_client=object(), fs_store=fs_store)  # type: ignore[arg-type]
    return writer, pool


async def _complete(writer: ObjectWriter, **overrides: Any) -> Any:
    kwargs: dict[str, Any] = {
        "bucket_name": "bkt",
        "object_id": "11111111-2222-3333-4444-555555555555",
        "object_key": "k",
        "upload_id": "up-1",
        "object_version": 1,
        "address": "acct",
    }
    kwargs.update(overrides)
    return await writer.mpu_complete(**kwargs)


def _persisted_completed_part_numbers(pool: _FakePool) -> Any:
    """The `completed_part_numbers` bound into the object_versions UPDATE (its 5th positional arg)."""
    update = next(q for q in pool.conn.executed if "completed_part_numbers" in q[0])
    return update[1][4]


@pytest.mark.asyncio
async def test_empty_selected_parts_is_empty_subset_not_full_object(tmp_path: Any) -> None:
    """`selected_parts=[]` must complete as an EMPTY subset (0 parts), not silently assemble all."""
    writer, pool = _writer(tmp_path)

    res = await _complete(writer, selected_parts=[])

    assert res.size_bytes == 0, "empty selection was silently treated as 'all parts' (full-set completion)"
    assert res.etag == hashlib.md5(b"").hexdigest() + "-0"
    # The empty subset is persisted explicitly ([] not NULL) so the reader serves zero parts.
    assert _persisted_completed_part_numbers(pool) == []


@pytest.mark.asyncio
async def test_none_selected_parts_completes_full_object(tmp_path: Any) -> None:
    """`selected_parts=None` (default) keeps the unchanged 'complete every part' behaviour."""
    writer, pool = _writer(tmp_path)

    res = await _complete(writer)  # selected_parts defaults to None

    assert res.size_bytes == _FULL_SIZE
    assert res.etag.endswith("-2")
    # Full-set completion leaves the column NULL (no per-read filter stored).
    assert _persisted_completed_part_numbers(pool) is None


@pytest.mark.asyncio
async def test_strict_subset_selects_only_named_parts(tmp_path: Any) -> None:
    """A strict subset (part 1 only) reflects only that part's bytes/ETag and persists the filter."""
    writer, pool = _writer(tmp_path)

    res = await _complete(writer, selected_parts=[1])

    assert res.size_bytes == 100
    assert res.etag.endswith("-1")
    assert _persisted_completed_part_numbers(pool) == [1]


@pytest.mark.asyncio
async def test_db_parts_passthrough_skips_both_reads(tmp_path: Any) -> None:
    """MPU-3: when the endpoint's parts rows are passed, mpu_complete issues zero parts fetches."""
    writer, pool = _writer(tmp_path)

    res = await _complete(writer, db_parts=_DB_PARTS)

    assert res.size_bytes == _FULL_SIZE
    assert res.etag.endswith("-2")
    assert pool.fetch_calls == 0, "db_parts supplied — mpu_complete must not re-read the parts table"


@pytest.mark.asyncio
async def test_db_parts_result_matches_fallback(tmp_path: Any) -> None:
    """The ETag/size computed from db_parts must be byte-identical to the DB-read fallback."""
    w_fb, _ = _writer(tmp_path)
    fallback = await _complete(w_fb)  # reads the two queries

    w_db, _ = _writer(tmp_path)
    passthrough = await _complete(w_db, db_parts=_DB_PARTS)

    assert (passthrough.etag, passthrough.size_bytes) == (fallback.etag, fallback.size_bytes)


@pytest.mark.asyncio
async def test_db_parts_strict_subset(tmp_path: Any) -> None:
    """Subset filtering works off db_parts too — only the named part's bytes/ETag/filter."""
    writer, pool = _writer(tmp_path)

    res = await _complete(writer, selected_parts=[1], db_parts=_DB_PARTS)

    assert res.size_bytes == 100
    assert res.etag.endswith("-1")
    assert pool.fetch_calls == 0
    assert _persisted_completed_part_numbers(pool) == [1]
