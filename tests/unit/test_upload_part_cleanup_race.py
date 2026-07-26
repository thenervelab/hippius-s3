"""Duplicate-UploadPart cleanup race (2026-07-22 / 2026-07-26 prod data loss).

Clients hedge UploadPart with concurrent duplicate PUTs of the same
(object, version, part_number); all attempts share ONE part dir on the SSD.
Chunk writes are atomic tmp+rename and byte-identical across duplicates
(deterministic AES-GCM nonces), so concurrent writers are harmless — but a
loser that fails/cancels AFTER another attempt published (meta.json + parts
row + 200 to the client) must NOT delete the shared part dir: that destroys
acknowledged data.

These tests publish a part as attempt A, then drive attempt B through each
failure path and assert A's published data survives B's cleanup.
"""

from __future__ import annotations

import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from typing import AsyncIterator

import pytest

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.config import get_config
from hippius_s3.writer.object_writer import ObjectWriter
from hippius_s3.writer.types import AppendPreconditionFailed


PART_BODY = b"abcdefgh"  # 2 chunks at chunk_size=4


class DummyRedis:
    async def delete(self, *_args: Any, **_kwargs: Any) -> int:
        return 1

    async def setex(self, *_args: Any, **_kwargs: Any) -> None:
        return None


class DummyPool:
    async def fetchrow(self, *_args: Any, **_kwargs: Any) -> dict:
        return {"bucket_id": "bucket", "storage_version": 5}

    async def fetchval(self, *_args: Any, **_kwargs: Any) -> str:
        return "part-id"

    async def execute(self, *_args: Any, **_kwargs: Any) -> None:
        return None


def _make_writer(pool: Any, fs_store: FileSystemPartsStore, monkeypatch: pytest.MonkeyPatch) -> ObjectWriter:
    writer = ObjectWriter(pool=pool, redis_client=DummyRedis(), fs_store=fs_store)

    async def fake_ensure_dek(*_args: Any, **_kwargs: Any) -> bytes:
        return b"\x00" * 32

    monkeypatch.setattr(writer, "_ensure_and_get_v5_dek", fake_ensure_dek)
    return writer


async def _publish_attempt_a(
    writer: ObjectWriter,
    *,
    object_id: str,
    object_version: int,
    part_number: int,
    upload_id: str,
) -> None:
    """Attempt A completes fully: all chunks + meta.json on FS + parts row + 200 to the client."""

    async def body() -> AsyncIterator[bytes]:
        yield PART_BODY

    res = await writer.mpu_upload_part_stream(
        upload_id=upload_id,
        object_id=object_id,
        object_version=object_version,
        bucket_name="bucket",
        bucket_id="bucket",
        account_address="acct",
        part_number=part_number,
        body_iter=body(),
    )
    assert res.size_bytes == len(PART_BODY)


def _read_chunks(fs_store: FileSystemPartsStore, object_id: str, version: int, part: int) -> dict[str, bytes]:
    part_dir = Path(fs_store.part_path(object_id, version, part))
    return {p.name: p.read_bytes() for p in part_dir.glob("chunk_*.bin")}


@pytest.fixture()
def small_chunks(monkeypatch: pytest.MonkeyPatch) -> None:
    cfg = get_config()
    monkeypatch.setattr(cfg, "object_chunk_size_bytes", 4)
    monkeypatch.setattr(cfg, "cache_ttl_seconds", 60)
    monkeypatch.setattr("hippius_s3.writer.object_writer.get_config", lambda: cfg)


@pytest.mark.asyncio
async def test_mpu_duplicate_failure_after_publish_preserves_part(tmp_path, monkeypatch, small_chunks):
    """A duplicate UploadPart attempt that dies mid-stream must not wipe the published part dir."""
    object_id = str(uuid.uuid4())
    fs_store = FileSystemPartsStore(str(tmp_path))
    writer = _make_writer(DummyPool(), fs_store, monkeypatch)

    await _publish_attempt_a(writer, object_id=object_id, object_version=1, part_number=1, upload_id="upload")
    published = _read_chunks(fs_store, object_id, 1, 1)
    assert set(published) == {"chunk_0.bin", "chunk_1.bin"}

    async def dying_body() -> AsyncIterator[bytes]:
        yield PART_BODY[:4]
        raise ConnectionError("client disconnected mid-stream")

    with pytest.raises(ConnectionError):
        await writer.mpu_upload_part_stream(
            upload_id="upload",
            object_id=object_id,
            object_version=1,
            bucket_name="bucket",
            bucket_id="bucket",
            account_address="acct",
            part_number=1,
            body_iter=dying_body(),
        )

    meta = await fs_store.get_meta(object_id, 1, 1)
    assert meta is not None, "published meta.json was destroyed by the loser's cleanup"
    assert int(meta["num_chunks"]) == 2
    assert _read_chunks(fs_store, object_id, 1, 1) == published
    assert await fs_store.get_chunk(object_id, 1, 1, 0) == published["chunk_0.bin"]
    assert await fs_store.get_chunk(object_id, 1, 1, 1) == published["chunk_1.bin"]


class _AppendFakeConn:
    """Just enough of an asyncpg connection for append_stream's two transactions."""

    def __init__(self, pool: "_AppendFakePool") -> None:
        self._pool = pool

    async def fetchrow(self, query: str, *args: Any) -> Any:
        return self._pool.route_fetchrow(query)

    async def fetchval(self, query: str, *args: Any) -> Any:
        return self._pool.route_fetchval(query)

    async def fetch(self, query: str, *args: Any) -> list:
        return []

    async def execute(self, query: str, *args: Any) -> None:
        return None

    def transaction(self) -> Any:
        @asynccontextmanager
        async def _cm() -> Any:
            yield

        return _cm()


class _AppendFakePool:
    """Drives append_stream to the finalize CAS, where a concurrent winner already bumped
    append_version — the exact shape of a hedged duplicate losing after the winner published."""

    def __init__(self, *, object_id: str, expected_version: int, next_part: int) -> None:
        self.object_id = object_id
        self.expected_version = expected_version
        self.next_part = next_part

    def route_fetchrow(self, query: str) -> Any:
        if "md5_hash" in query and "FOR UPDATE" in query:
            # Finalize CAS read: the winner already advanced append_version.
            return {
                "append_version": self.expected_version + 1,
                "md5_hash": "0" * 32,
                "has_etag_md5s": True,
            }
        if "append_version" in query:
            return {"append_version": self.expected_version}
        if "upload_id FROM multipart_uploads" in query:
            return {"upload_id": "11111111-2222-3333-4444-555555555555"}
        raise AssertionError(f"unexpected fetchrow: {query}")

    def route_fetchval(self, query: str) -> Any:
        if "MAX(part_number)" in query:
            return self.next_part
        if "append_version" in query:
            return self.expected_version
        if "INSERT INTO parts" in query:
            return "part-id"
        raise AssertionError(f"unexpected fetchval: {query}")

    async def fetchrow(self, query: str, *args: Any) -> Any:
        if "o.object_id" in query:
            return {"object_id": self.object_id, "cov": 1}
        return self.route_fetchrow(query)

    async def fetchval(self, query: str, *args: Any) -> Any:
        return self.route_fetchval(query)

    async def execute(self, query: str, *args: Any) -> None:
        return None

    def acquire(self, **_kwargs: Any) -> Any:
        conn = _AppendFakeConn(self)

        @asynccontextmanager
        async def _cm() -> Any:
            yield conn

        return _cm()


@pytest.mark.asyncio
async def test_append_cas_loser_cleanup_preserves_published_part(tmp_path, monkeypatch, small_chunks):
    """An append CAS loser (winner finalized the same part first) must not wipe the part dir."""
    object_id = str(uuid.uuid4())
    fs_store = FileSystemPartsStore(str(tmp_path))

    pool = _AppendFakePool(object_id=object_id, expected_version=7, next_part=3)
    writer = _make_writer(pool, fs_store, monkeypatch)

    # The winner's publish of (object, cov=1, part=3): same bytes the loser will re-write
    # (deterministic nonces make duplicate ciphertexts identical).
    await _publish_attempt_a(writer, object_id=object_id, object_version=1, part_number=3, upload_id="upload")
    published = _read_chunks(fs_store, object_id, 1, 3)
    assert set(published) == {"chunk_0.bin", "chunk_1.bin"}

    async def body() -> AsyncIterator[bytes]:
        yield PART_BODY

    with pytest.raises(AppendPreconditionFailed):
        await writer.append_stream(
            bucket_id="bucket",
            bucket_name="bucket",
            object_key="k",
            expected_version=7,
            account_address="acct",
            body_iter=body(),
        )

    meta = await fs_store.get_meta(object_id, 1, 3)
    assert meta is not None, "published meta.json was destroyed by the CAS loser's cleanup"
    assert int(meta["num_chunks"]) == 2
    assert _read_chunks(fs_store, object_id, 1, 3) == published
    assert await fs_store.get_chunk(object_id, 1, 3, 0) == published["chunk_0.bin"]
    assert await fs_store.get_chunk(object_id, 1, 3, 1) == published["chunk_1.bin"]
