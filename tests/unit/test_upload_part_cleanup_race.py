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

import asyncio
import json
import uuid
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any
from typing import AsyncIterator

import pytest

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.config import get_config
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.writer.object_writer import ObjectWriter
from hippius_s3.writer.types import AppendPreconditionFailed


PART_BODY = b"abcdefgh"  # 2 chunks at chunk_size=4
SUITE_ID = "hip-enc/aes256gcm"
TEST_DEK = b"\x00" * 32  # matches _make_writer's stubbed _ensure_and_get_v5_dek


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


def _read_plaintext(
    fs_store: FileSystemPartsStore,
    object_id: str,
    version: int,
    part: int,
    *,
    upload_id: str,
) -> bytes:
    """Decrypt the published part exactly as a reader would: meta.json says how many chunks.

    Assert on THIS, not on ciphertext bytes — nonces are random since d5e6b939, so two attempts
    encrypting the same plaintext produce different ciphertext, and the corruption this file
    guards against is wrong PLAINTEXT under a valid AEAD tag.
    """
    part_dir = Path(fs_store.part_path(object_id, version, part))
    meta = json.loads((part_dir / "meta.json").read_text())
    adapter = CryptoService.get_adapter(SUITE_ID)
    out = bytearray()
    for index in range(int(meta["num_chunks"])):
        out += adapter.decrypt_chunk(
            (part_dir / f"chunk_{index}.bin").read_bytes(),
            key=TEST_DEK,
            bucket_id="bucket",
            object_id=object_id,
            part_number=part,
            chunk_index=index,
            upload_id=upload_id,
        )
    return bytes(out)


class _WriteSignallingStore(FileSystemPartsStore):
    """The real store, plus an event fired once a chunk write has landed on disk.

    Lets a test order an attempt's failure strictly AFTER its write completed — the window
    consumer cancellation cannot reach, because by then the bytes are already on the disk.
    Both write entry points are hooked so the same test drives either implementation.
    """

    def __init__(self, root: str) -> None:
        super().__init__(root)
        self.chunk_written = asyncio.Event()

    async def set_chunk(self, *args: Any, **kwargs: Any) -> None:
        await super().set_chunk(*args, **kwargs)
        self.chunk_written.set()

    async def stage_chunk(self, *args: Any, **kwargs: Any) -> None:
        await super().stage_chunk(*args, **kwargs)
        self.chunk_written.set()


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


@pytest.mark.asyncio
async def test_failed_attempt_preserves_unpublished_chunks(tmp_path, monkeypatch, small_chunks):
    """B fails while A is mid-write (A's chunks on disk, meta NOT yet written): A's chunks survive.

    Pins "failure paths never delete chunk files" even for a dir that is not yet published —
    guards against a future meta-guarded delete creeping back into the failure cleanup.
    """
    object_id = str(uuid.uuid4())
    fs_store = FileSystemPartsStore(str(tmp_path))
    writer = _make_writer(DummyPool(), fs_store, monkeypatch)

    # Attempt A mid-write: chunks landed, meta.json not yet written.
    await fs_store.set_chunk(object_id, 1, 1, 0, b"A-chunk-0")
    await fs_store.set_chunk(object_id, 1, 1, 1, b"A-chunk-1")
    part_dir = Path(fs_store.part_path(object_id, 1, 1))
    assert not (part_dir / "meta.json").exists()

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

    # B's failure deleted nothing: both chunk files survive and chunk_1 still holds A's
    # bytes. chunk_0's content is deliberately unasserted — B's consumer may or may not
    # have flushed its first chunk (atomic rename over A's) before the failure propagated.
    on_disk = _read_chunks(fs_store, object_id, 1, 1)
    assert {"chunk_0.bin", "chunk_1.bin"}.issubset(set(on_disk))
    assert on_disk["chunk_1.bin"] == b"A-chunk-1"
    assert not (part_dir / "meta.json").exists()


@pytest.mark.asyncio
async def test_publish_trims_stale_chunk_tail(tmp_path, monkeypatch, small_chunks):
    """Publishing with num_chunks=N must delete stale chunk files with index >= N.

    The drain replicates a part only when the SSD chunk set is EXACTLY
    {0..num_chunks-1} (partdrain.rs IncompleteSource gate); a tail left by a
    larger earlier attempt would strand the part — never replicated, never evicted.
    """
    object_id = str(uuid.uuid4())
    fs_store = FileSystemPartsStore(str(tmp_path))
    writer = _make_writer(DummyPool(), fs_store, monkeypatch)

    # A larger earlier attempt died after writing 5 chunks (no meta).
    for i in range(5):
        await fs_store.set_chunk(object_id, 1, 1, i, f"stale-{i}".encode())

    await _publish_attempt_a(writer, object_id=object_id, object_version=1, part_number=1, upload_id="upload")

    part_dir = Path(fs_store.part_path(object_id, 1, 1))
    assert sorted(p.name for p in part_dir.iterdir()) == ["chunk_0.bin", "chunk_1.bin", "meta.json"]
    meta = await fs_store.get_meta(object_id, 1, 1)
    assert meta is not None and int(meta["num_chunks"]) == 2


@pytest.mark.asyncio
async def test_trim_failure_is_loud_but_publish_succeeds(tmp_path, monkeypatch, small_chunks, caplog):
    """A trim failure leaves a stranded-part risk: log ERROR, but the client's 200 stands."""
    import logging

    object_id = str(uuid.uuid4())
    fs_store = FileSystemPartsStore(str(tmp_path))
    writer = _make_writer(DummyPool(), fs_store, monkeypatch)

    # Stale tail where one entry is a directory — unlink fails on it (real-FS fault injection).
    await fs_store.set_chunk(object_id, 1, 1, 2, b"stale-2")
    part_dir = Path(fs_store.part_path(object_id, 1, 1))
    (part_dir / "chunk_3.bin").mkdir()

    with caplog.at_level(logging.ERROR, logger="hippius_s3.cache.fs_store"):
        await _publish_attempt_a(writer, object_id=object_id, object_version=1, part_number=1, upload_id="upload")

    assert not (part_dir / "chunk_2.bin").exists()
    assert (part_dir / "meta.json").exists()
    errors = [r for r in caplog.records if r.levelno >= logging.ERROR]
    assert any(object_id in r.getMessage() for r in errors), "surviving tail must be loud"


@pytest.mark.asyncio
async def test_interleaved_attempt_write_landing_after_publish_leaves_plaintext_intact(
    tmp_path, monkeypatch, small_chunks
):
    """B is mid-stream while A publishes; B's next chunk lands AFTER A's 200, then B dies.

    The case consumer cancellation cannot reach: the write is already on disk before the
    failure propagates, so nothing the failure path does can take it back. Two attempts must
    therefore never write the same file names in the first place.
    """
    object_id = str(uuid.uuid4())
    fs_store = _WriteSignallingStore(str(tmp_path))
    writer = _make_writer(DummyPool(), fs_store, monkeypatch)

    b_first_chunk_queued = asyncio.Event()
    a_published = asyncio.Event()

    async def b_body() -> AsyncIterator[bytes]:
        yield b"ZZZZ"  # chunk 0, while A has not published yet
        b_first_chunk_queued.set()
        await a_published.wait()
        fs_store.chunk_written.clear()
        yield b"YYYY"  # chunk 1, lands after A's part was published and acknowledged
        await fs_store.chunk_written.wait()
        raise ConnectionError("client disconnected mid-stream")

    b_task = asyncio.create_task(
        writer.mpu_upload_part_stream(
            upload_id="upload",
            object_id=object_id,
            object_version=1,
            bucket_name="bucket",
            bucket_id="bucket",
            account_address="acct",
            part_number=1,
            body_iter=b_body(),
        )
    )
    await b_first_chunk_queued.wait()

    await _publish_attempt_a(writer, object_id=object_id, object_version=1, part_number=1, upload_id="upload")
    a_published.set()

    with pytest.raises(ConnectionError):
        await b_task

    assert _read_plaintext(fs_store, object_id, 1, 1, upload_id="upload") == PART_BODY


@pytest.mark.asyncio
async def test_later_attempt_replaces_the_whole_part_not_a_prefix_of_it(tmp_path, monkeypatch, small_chunks):
    """A duplicate that SUCCEEDS after A must leave its own complete set, never a mixture."""
    object_id = str(uuid.uuid4())
    fs_store = FileSystemPartsStore(str(tmp_path))
    writer = _make_writer(DummyPool(), fs_store, monkeypatch)

    await _publish_attempt_a(writer, object_id=object_id, object_version=1, part_number=1, upload_id="upload")

    async def b_body() -> AsyncIterator[bytes]:
        yield b"wxyz"  # 1 chunk — shorter than A's 2, so a prefix-overwrite would strand a tail

    res = await writer.mpu_upload_part_stream(
        upload_id="upload",
        object_id=object_id,
        object_version=1,
        bucket_name="bucket",
        bucket_id="bucket",
        account_address="acct",
        part_number=1,
        body_iter=b_body(),
    )

    assert res.size_bytes == 4
    part_dir = Path(fs_store.part_path(object_id, 1, 1))
    assert sorted(p.name for p in part_dir.iterdir()) == ["chunk_0.bin", "meta.json"]
    assert _read_plaintext(fs_store, object_id, 1, 1, upload_id="upload") == b"wxyz"


@pytest.mark.asyncio
async def test_partial_publish_unpublishes_rather_than_leaving_a_mixed_set(tmp_path, monkeypatch, small_chunks):
    """A publish that fails part-way must remove meta.json, not leave a half-swapped part readable.

    Renaming a set of files is not atomic, so an FS error mid-publish CAN leave one attempt's
    chunks next to another's. That state must at least be invisible: with meta.json gone the
    reader treats the part as a cache miss and the drain as IncompleteSource, instead of both
    serving a mixture that AEAD-verifies as the wrong plaintext.
    """
    object_id = str(uuid.uuid4())
    fs_store = FileSystemPartsStore(str(tmp_path))
    writer = _make_writer(DummyPool(), fs_store, monkeypatch)

    async def a_body() -> AsyncIterator[bytes]:
        yield b"abcdefghijkl"  # 3 chunks

    await writer.mpu_upload_part_stream(
        upload_id="upload",
        object_id=object_id,
        object_version=1,
        bucket_name="bucket",
        bucket_id="bucket",
        account_address="acct",
        part_number=1,
        body_iter=a_body(),
    )

    # Real-FS fault injection: a directory where chunk_2.bin belongs fails the third rename
    # (and, before this change, the third chunk write) after the first two already landed.
    part_dir = Path(fs_store.part_path(object_id, 1, 1))
    (part_dir / "chunk_2.bin").unlink()
    (part_dir / "chunk_2.bin").mkdir()

    async def b_body() -> AsyncIterator[bytes]:
        yield b"MMMMNNNNOOOO"

    with pytest.raises(OSError):
        await writer.mpu_upload_part_stream(
            upload_id="upload",
            object_id=object_id,
            object_version=1,
            bucket_name="bucket",
            bucket_id="bucket",
            account_address="acct",
            part_number=1,
            body_iter=b_body(),
        )

    assert not (part_dir / "meta.json").exists(), "a half-published part must not stay readable"


@pytest.mark.asyncio
async def test_failed_attempt_leaves_no_staged_files_behind(tmp_path, monkeypatch, small_chunks):
    """A dying attempt cleans up its own in-progress files — they are private to it, so unlike
    the published chunk names there is no ambiguity about whose data they are."""
    object_id = str(uuid.uuid4())
    fs_store = _WriteSignallingStore(str(tmp_path))
    writer = _make_writer(DummyPool(), fs_store, monkeypatch)

    await _publish_attempt_a(writer, object_id=object_id, object_version=1, part_number=1, upload_id="upload")

    async def dying_body() -> AsyncIterator[bytes]:
        fs_store.chunk_written.clear()
        yield b"ZZZZ"
        await fs_store.chunk_written.wait()
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

    part_dir = Path(fs_store.part_path(object_id, 1, 1))
    assert sorted(p.name for p in part_dir.iterdir()) == ["chunk_0.bin", "chunk_1.bin", "meta.json"]


@pytest.mark.asyncio
async def test_a_cancelled_attempt_still_discards_its_staged_chunks(tmp_path, monkeypatch, small_chunks):
    """Cancellation is a BaseException, so `except Exception` would skip cleanup for the very
    case whose staged bytes most need dropping — and nothing else can reap an unpublished part
    dir before the drain's 24h orphan sweep."""
    object_id = str(uuid.uuid4())
    fs_store = _WriteSignallingStore(str(tmp_path))
    writer = _make_writer(DummyPool(), fs_store, monkeypatch)

    async def stalling_body() -> AsyncIterator[bytes]:
        fs_store.chunk_written.clear()
        yield b"ZZZZ"
        await fs_store.chunk_written.wait()
        await asyncio.sleep(30)  # still streaming when the client goes away
        yield b"YYYY"

    task = asyncio.create_task(
        writer.mpu_upload_part_stream(
            upload_id="upload",
            object_id=object_id,
            object_version=1,
            bucket_name="bucket",
            bucket_id="bucket",
            account_address="acct",
            part_number=1,
            body_iter=stalling_body(),
        )
    )
    await fs_store.chunk_written.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    part_dir = Path(fs_store.part_path(object_id, 1, 1))
    assert list(part_dir.iterdir()) == [], "a cancelled attempt leaves nothing behind"


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
        self.executed: list[str] = []

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
        self.executed.append(query)
        return None

    def acquire(self, **_kwargs: Any) -> Any:
        conn = _AppendFakeConn(self)

        @asynccontextmanager
        async def _cm() -> Any:
            yield conn

        return _cm()


@pytest.mark.asyncio
async def test_append_cas_loser_unpublishes_part_and_number_is_reusable(tmp_path, monkeypatch, small_chunks):
    """A CAS loser must un-publish its part dir: meta.json deleted, chunks kept, parts row gone.

    Part-number reservation is FOR-UPDATE-serialized, so no live winner shares the loser's
    dir — but the loser's meta.json already landed (mpu_upload_part_stream returned before the
    finalize CAS ran). Leaving it would let a future append that reuses the number inherit
    stale readiness over mixed content. Chunks stay: failure paths never delete chunk files.
    """
    object_id = str(uuid.uuid4())
    fs_store = FileSystemPartsStore(str(tmp_path))

    pool = _AppendFakePool(object_id=object_id, expected_version=7, next_part=3)
    writer = _make_writer(pool, fs_store, monkeypatch)

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

    part_dir = Path(fs_store.part_path(object_id, 1, 3))
    assert not (part_dir / "meta.json").exists(), "CAS loser must un-publish its meta.json"
    loser_chunks = _read_chunks(fs_store, object_id, 1, 3)
    assert set(loser_chunks) == {"chunk_0.bin", "chunk_1.bin"}, "chunks must survive (leak beats loss)"
    assert any("DELETE FROM parts" in q for q in pool.executed), "loser's parts row must be deleted"

    # A later append reuses part 3 with different, SHORTER content: it must publish cleanly
    # and its trim must remove the loser's stale chunk_1 tail (exact-set drain gate).
    async def reuse_body() -> AsyncIterator[bytes]:
        yield b"zzzz"  # 1 chunk at chunk_size=4

    res = await writer.mpu_upload_part_stream(
        upload_id="11111111-2222-3333-4444-555555555555",
        object_id=object_id,
        object_version=1,
        bucket_name="bucket",
        bucket_id="bucket",
        account_address="acct",
        part_number=3,
        body_iter=reuse_body(),
    )
    assert res.size_bytes == 4
    assert sorted(p.name for p in part_dir.iterdir()) == ["chunk_0.bin", "meta.json"]
    meta = await fs_store.get_meta(object_id, 1, 3)
    assert meta is not None and int(meta["num_chunks"]) == 1
