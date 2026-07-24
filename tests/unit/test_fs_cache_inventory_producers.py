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
from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import PartChunkSpec
from hippius_s3.queue import PartToDownload
from hippius_s3.workers.downloader import process_download_request
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


def _dcr(num_parts: int, chunks_per_part: int) -> DownloadChainRequest:
    parts = [
        PartToDownload(part_number=pn, chunks=[PartChunkSpec(index=ci, cid=None) for ci in range(chunks_per_part)])
        for pn in range(1, num_parts + 1)
    ]
    return DownloadChainRequest(
        object_id=OBJ,
        object_version=1,
        object_key="test/object.bin",
        bucket_name="test-bucket",
        object_storage_version=5,
        address="5Addr",
        subaccount="5Addr",
        substrate_url="http://test",
        size=num_parts * chunks_per_part * 4 * 1024 * 1024,
        multipart=num_parts > 1,
        chunks=parts,
    )


def _dl_config() -> Any:
    c = MagicMock()
    c.downloader_semaphore = 4
    c.downloader_chunk_retries = 1
    c.downloader_retry_base_seconds = 0.0
    c.downloader_retry_jitter_seconds = 0.0
    return c


def _dl_pool(execute_side_effect: Any = None) -> Any:
    conn = AsyncMock()

    async def _fetchrow(sql: str, *_a: Any) -> Any:
        if "size_bytes" in sql:
            return {"size_bytes": 4096, "chunk_size_bytes": 4 * 1024 * 1024}
        if "backend_identifier" in sql or "chunk_backend" in sql:
            return {"backend_identifier": "arion-id"}
        return None

    async def _fetch(sql: str, *_a: Any) -> Any:
        return []

    conn.fetchrow = AsyncMock(side_effect=_fetchrow)
    conn.fetch = AsyncMock(side_effect=_fetch)
    pool = MagicMock()
    pool.acquire = MagicMock(
        return_value=MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock(return_value=False))
    )
    pool.execute = AsyncMock(side_effect=execute_side_effect)
    return pool


def _dl_obj_cache() -> Any:
    cache = MagicMock()
    cache.notify_chunk = AsyncMock()
    cache.redis = MagicMock()
    cache.redis.eval = AsyncMock(return_value=1)
    return cache


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_downloader_records_inventory_on_part_completion(
    mock_metrics: Any, mock_config: Any, tmp_path: Path
) -> None:
    mock_config.return_value = _dl_config()
    mock_metrics.return_value = MagicMock()
    fs_store = FileSystemPartsStore(str(tmp_path))
    pool = _dl_pool()

    with patch(RECORD_TARGET, new_callable=AsyncMock) as rec:
        result = await process_download_request(
            _dcr(2, 1),
            backend_name="arion",
            fetch_fn=AsyncMock(return_value=b"y" * 4096),
            db_pool=pool,
            obj_cache=_dl_obj_cache(),
            fs_store=fs_store,
        )

    assert result is True
    # One record per completed part (2 parts), not per eager meta write.
    assert rec.await_count == 2
    recorded = {(a.args[2], a.args[3]) for a in rec.await_args_list}
    assert recorded == {(1, 1), (1, 2)}


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_downloader_no_record_when_part_incomplete(mock_metrics: Any, mock_config: Any, tmp_path: Path) -> None:
    """The eager meta.json is written up-front, but a failed fetch leaves the part incomplete;
    the inventory row must NOT be recorded off the eager meta write."""
    mock_config.return_value = _dl_config()
    mock_metrics.return_value = MagicMock()
    fs_store = FileSystemPartsStore(str(tmp_path))
    pool = _dl_pool()

    with patch(RECORD_TARGET, new_callable=AsyncMock) as rec:
        result = await process_download_request(
            _dcr(1, 1),
            backend_name="arion",
            fetch_fn=AsyncMock(side_effect=RuntimeError("backend down")),
            db_pool=pool,
            obj_cache=_dl_obj_cache(),
            fs_store=fs_store,
        )

    assert result is False
    # Eager meta.json exists (part is "known"), yet the part never completed.
    assert await fs_store.get_meta(OBJ, 1, 1) is not None
    assert rec.await_count == 0


@pytest.mark.asyncio
@patch("hippius_s3.workers.downloader.get_config")
@patch("hippius_s3.workers.downloader.get_metrics_collector")
async def test_downloader_survives_inventory_failure(mock_metrics: Any, mock_config: Any, tmp_path: Path) -> None:
    mock_config.return_value = _dl_config()
    mock_metrics.return_value = MagicMock()
    fs_store = FileSystemPartsStore(str(tmp_path))
    pool = _dl_pool(execute_side_effect=RuntimeError("advisory write boom"))

    # Real record_cached runs against a pool whose execute raises; the swallow keeps the fill green.
    result = await process_download_request(
        _dcr(1, 1),
        backend_name="arion",
        fetch_fn=AsyncMock(return_value=b"y" * 4096),
        db_pool=pool,
        obj_cache=_dl_obj_cache(),
        fs_store=fs_store,
    )
    assert result is True
