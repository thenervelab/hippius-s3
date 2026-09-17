"""Task 3.1: every pipeline that materializes a part on the cache FS records an
fs_cache_inventory row so the janitor's SQL discovery can find it.

Each pipeline is exercised through its real code path with the DB/KEK helpers stubbed;
`record_cached` is patched at its import module so a mock counts the calls regardless of
which pipeline imported it. The resilience cases keep the *real* `record_cached` and make
the underlying DB write raise, proving the internal swallow shields the pipeline (there is
deliberately no try/except at the callsites).
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any
from typing import AsyncIterator
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.cache.fs_store import FileSystemPartsStore
from hippius_s3.config import get_config
from hippius_s3.writer.object_writer import ObjectWriter
from tests.unit._fake_pool import make_fake_pool


RECORD_TARGET = "hippius_s3.repositories.fs_cache_inventory.record_cached"


class DummyRedis:
    async def delete(self, *_a: Any, **_k: Any) -> int:
        return 1

    async def setex(self, *_a: Any, **_k: Any) -> None:
        return None

    async def set(self, *_a: Any, **_k: Any) -> None:
        return None


async def _body(*pieces: bytes) -> AsyncIterator[bytes]:
    for p in pieces:
        yield p


# --------------------------------------------------------------------------- simple PUT


def _simple_put_writer(tmp_path: Any, monkeypatch: Any, pool: Any) -> ObjectWriter:
    cfg = get_config()
    monkeypatch.setattr("hippius_s3.writer.object_writer.get_config", lambda: cfg)

    async def fake_kek(*, bucket_id: str) -> tuple[str, bytes]:
        return ("kek-1", b"\x00" * 32)

    async def fake_upsert(db: Any, **kw: Any) -> dict:
        return {"object_id": kw["object_id"], "current_object_version": 1}

    async def fake_ensure(db: Any, **kw: Any) -> str:
        return str(uuid.uuid4())

    async def fake_parts(db: Any, **kw: Any) -> None:
        return None

    monkeypatch.setattr("hippius_s3.services.kek_service.get_or_create_active_bucket_kek", fake_kek)
    monkeypatch.setattr("hippius_s3.writer.object_writer.upsert_object_basic", fake_upsert)
    monkeypatch.setattr("hippius_s3.writer.object_writer.ensure_upload_row", fake_ensure)
    monkeypatch.setattr("hippius_s3.writer.object_writer.upsert_part_placeholder", fake_parts)

    return ObjectWriter(pool=pool, redis_client=DummyRedis(), fs_store=FileSystemPartsStore(str(tmp_path)))


async def _run_simple_put(writer: ObjectWriter, *pieces: bytes) -> Any:
    return await writer.put_simple_stream_full(
        bucket_id=str(uuid.uuid4()),
        bucket_name="bkt",
        object_id=str(uuid.uuid4()),
        object_key="k/obj.json",
        account_address="acct",
        content_type="application/json",
        metadata={},
        body_iter=_body(*pieces),
    )


@pytest.mark.asyncio
async def test_simple_put_records_inventory_once(tmp_path: Any, monkeypatch: Any) -> None:
    pool = make_fake_pool()
    writer = _simple_put_writer(tmp_path, monkeypatch, pool)
    with patch(RECORD_TARGET, new_callable=AsyncMock) as rec:
        res = await _run_simple_put(writer, b"hello world")

    assert rec.await_count == 1
    handle, object_id, object_version, part_number = rec.await_args.args
    # Autocommit contract: recorded on the held conn, NEVER while a transaction is open — a failed
    # INSERT inside the tail transaction would poison it and break the commit. FakeConn exposes
    # in_transaction (False here, post-commit); the pool has no such attribute.
    assert handle is not pool
    assert getattr(handle, "in_transaction", None) is False
    assert object_id == res.object_id
    assert object_version == 1
    assert part_number == 1


@pytest.mark.asyncio
async def test_simple_put_records_after_tail_commit(tmp_path: Any, monkeypatch: Any) -> None:
    """The advisory write must land on the held conn AFTER the tail transaction commits (txn_exit)."""
    pool = make_fake_pool()
    writer = _simple_put_writer(tmp_path, monkeypatch, pool)
    await _run_simple_put(writer, b"payload")

    inv_idx = next(i for i, e in enumerate(pool.events) if "fs_cache_inventory" in (e.get("query") or ""))
    inv_event = pool.events[inv_idx]
    last_txn_exit = max(i for i, e in enumerate(pool.events) if e["method"] == "txn_exit")
    assert inv_idx > last_txn_exit
    assert inv_event["in_txn"] is False
    assert inv_event["conn"] != "pool"  # reuses the held tail conn, not a fresh pool checkout


@pytest.mark.asyncio
async def test_simple_put_survives_inventory_failure(tmp_path: Any, monkeypatch: Any) -> None:
    def _router(method: str, query: str | None, _args: tuple) -> Any:
        if query and "fs_cache_inventory" in query:
            raise RuntimeError("advisory write boom")
        return None

    writer = _simple_put_writer(tmp_path, monkeypatch, make_fake_pool(_router))
    # Real record_cached runs; its internal swallow must keep the PUT succeeding.
    res = await _run_simple_put(writer, b"payload")
    assert res.size_bytes == len(b"payload")


# --------------------------------------------------------------------------- MPU part


class _MpuPool:
    async def fetchrow(self, *_a: Any, **_k: Any) -> Any:
        return None

    async def fetchval(self, *_a: Any, **_k: Any) -> Any:
        return "part-id"

    async def execute(self, *_a: Any, **_k: Any) -> Any:
        return None


class _MpuPoolFailingInventory:
    async def fetchrow(self, *_a: Any, **_k: Any) -> Any:
        return None

    async def fetchval(self, *_a: Any, **_k: Any) -> Any:
        return "part-id"

    async def execute(self, query: str, *_a: Any, **_k: Any) -> Any:
        if "fs_cache_inventory" in query:
            raise RuntimeError("advisory write boom")
        return None


def _mpu_writer(tmp_path: Any, monkeypatch: Any, pool: Any) -> ObjectWriter:
    cfg = get_config()
    monkeypatch.setattr("hippius_s3.writer.object_writer.get_config", lambda: cfg)

    async def fake_dek(*_a: Any, **_k: Any) -> bytes:
        return b"\x00" * 32

    async def fake_parts(db: Any, **kw: Any) -> None:
        return None

    monkeypatch.setattr("hippius_s3.writer.object_writer.upsert_part_placeholder", fake_parts)
    writer = ObjectWriter(pool=pool, redis_client=DummyRedis(), fs_store=FileSystemPartsStore(str(tmp_path)))
    monkeypatch.setattr(writer, "_ensure_and_get_v5_dek", fake_dek)
    return writer


async def _run_mpu_part(writer: ObjectWriter, object_id: str, part_number: int) -> Any:
    return await writer.mpu_upload_part_stream(
        upload_id="upload",
        object_id=object_id,
        object_version=3,
        bucket_name="bkt",
        bucket_id="bkt",
        account_address="acct",
        part_number=part_number,
        body_iter=_body(b"mpu-bytes"),
    )


@pytest.mark.asyncio
async def test_mpu_part_records_inventory_once(tmp_path: Any, monkeypatch: Any) -> None:
    writer = _mpu_writer(tmp_path, monkeypatch, _MpuPool())
    object_id = str(uuid.uuid4())
    with patch(RECORD_TARGET, new_callable=AsyncMock) as rec:
        await _run_mpu_part(writer, object_id, part_number=4)

    assert rec.await_count == 1
    _conn, rec_object_id, object_version, part_number = rec.await_args.args
    assert str(rec_object_id) == object_id
    assert object_version == 3
    assert part_number == 4


@pytest.mark.asyncio
async def test_mpu_part_survives_inventory_failure(tmp_path: Any, monkeypatch: Any) -> None:
    writer = _mpu_writer(tmp_path, monkeypatch, _MpuPoolFailingInventory())
    res = await _run_mpu_part(writer, str(uuid.uuid4()), part_number=1)
    assert res.size_bytes == len(b"mpu-bytes")


# --------------------------------------------------------------------------- downloader

OBJ = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
