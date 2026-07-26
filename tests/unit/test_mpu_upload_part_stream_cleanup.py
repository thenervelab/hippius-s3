import uuid
from typing import AsyncIterator

import pytest

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.config import get_config
from hippius_s3.writer.object_writer import ObjectWriter


@pytest.mark.asyncio
async def test_mpu_upload_part_stream_partial_failure_leaves_no_serveable_data(tmp_path, monkeypatch):
    """Genuine-failure (no concurrent winner) cleanup: the partial part must NOT become serveable.

    On a failed part write we deliberately do NOT rmtree the shared part dir (that is the data-loss
    bug — see _cleanup_partial). Instead the partial is left meta-less: no meta.json, no `parts` row,
    so it is invisible to the drain/reader and reclaimable by the janitor's stale-parts reap. This
    asserts the safe post-condition: get_meta() is None and get_chunk() returns None (gated on meta),
    and the Redis best-effort cleanup still fired.
    """
    cfg = get_config()
    original_chunk_size = cfg.object_chunk_size_bytes
    original_max_part = cfg.max_multipart_part_size
    original_ttl = cfg.cache_ttl_seconds

    cfg.object_chunk_size_bytes = 4
    cfg.max_multipart_part_size = 5
    cfg.cache_ttl_seconds = 60

    async def fake_ensure_dek(*_args, **_kwargs) -> bytes:
        return b"\x00" * 32

    monkeypatch.setattr("hippius_s3.writer.object_writer.get_config", lambda: cfg)

    object_id = str(uuid.uuid4())
    fs_store = FileSystemPartsStore(str(tmp_path))

    class DummyPool:
        async def fetchrow(self, *_args, **_kwargs):
            return {"bucket_id": "bucket", "storage_version": 5, "kek_id": "kek-1", "wrapped_dek": b"\x00" * 48}

        async def fetchval(self, *_args, **_kwargs):
            return "part-id"

        async def execute(self, *_args, **_kwargs):
            return None

    redis_deletes: list[tuple] = []

    class DummyRedis:
        async def delete(self, *args, **_kwargs):
            redis_deletes.append(args)
            return 1

        async def setex(self, *_args, **_kwargs):
            return None

    async def body_iter() -> AsyncIterator[bytes]:
        yield b"abcd"
        yield b"ef"

    writer = ObjectWriter(pool=DummyPool(), redis_client=DummyRedis(), fs_store=fs_store)
    monkeypatch.setattr(writer, "_ensure_and_get_v5_dek", fake_ensure_dek)

    try:
        with pytest.raises(ValueError, match="part_size_exceeds_max"):
            await writer.mpu_upload_part_stream(
                upload_id="upload",
                object_id=object_id,
                object_version=1,
                bucket_name="bucket",
                bucket_id="bucket",
                account_address="acct",
                part_number=1,
                body_iter=body_iter(),
            )

        # The partial is meta-less: not serveable, drain-invisible, GC-able by the stale-parts reap.
        assert await fs_store.get_meta(object_id, 1, 1) is None
        assert await fs_store.get_chunk(object_id, 1, 1, 0) is None
        # Best-effort Redis cleanup of the partial's keys still fired.
        assert redis_deletes, "expected _cleanup_partial to still purge the partial's Redis keys"
    finally:
        cfg.object_chunk_size_bytes = original_chunk_size
        cfg.max_multipart_part_size = original_max_part
        cfg.cache_ttl_seconds = original_ttl


@pytest.mark.asyncio
async def test_mpu_upload_part_stream_failure_never_destroys_a_concurrent_winner(tmp_path, monkeypatch):
    """DATA-LOSS regression guard: a losing duplicate attempt's cleanup must NOT delete the winner.

    Duplicate/hedged UploadParts share ONE deterministic part dir. Here a WINNER has already
    completed the part (chunks + meta committed, client got its 200). A second attempt for the SAME
    (object_id, object_version, part_number) then fails mid-write, triggering _cleanup_partial. The
    winner's committed data MUST survive. Before the fix, _cleanup_partial rmtree'd the shared dir
    and destroyed it (parts 431/437/457 of beam-dev/100gbdestination1).
    """
    cfg = get_config()
    original_chunk_size = cfg.object_chunk_size_bytes
    original_max_part = cfg.max_multipart_part_size
    original_ttl = cfg.cache_ttl_seconds

    cfg.object_chunk_size_bytes = 4
    cfg.max_multipart_part_size = 5
    cfg.cache_ttl_seconds = 60

    async def fake_ensure_dek(*_args, **_kwargs) -> bytes:
        return b"\x00" * 32

    monkeypatch.setattr("hippius_s3.writer.object_writer.get_config", lambda: cfg)

    object_id = str(uuid.uuid4())
    fs_store = FileSystemPartsStore(str(tmp_path))

    # Simulate the WINNER: a fully-committed part (2 chunks + meta) that a client already 200'd.
    await fs_store.set_chunk(object_id, 1, 1, 0, b"WINNER-CHUNK-0")
    await fs_store.set_chunk(object_id, 1, 1, 1, b"WINNER-CHUNK-1")
    await fs_store.set_meta(object_id, 1, 1, chunk_size=4, num_chunks=2, size_bytes=8)

    class DummyPool:
        async def fetchrow(self, *_args, **_kwargs):
            return {"bucket_id": "bucket", "storage_version": 5, "kek_id": "kek-1", "wrapped_dek": b"\x00" * 48}

        async def fetchval(self, *_args, **_kwargs):
            return "part-id"

        async def execute(self, *_args, **_kwargs):
            return None

    class DummyRedis:
        async def delete(self, *_args, **_kwargs):
            return 1

        async def setex(self, *_args, **_kwargs):
            return None

    async def loser_body() -> AsyncIterator[bytes]:
        yield b"abcd"
        yield b"ef"

    writer = ObjectWriter(pool=DummyPool(), redis_client=DummyRedis(), fs_store=fs_store)
    monkeypatch.setattr(writer, "_ensure_and_get_v5_dek", fake_ensure_dek)

    try:
        with pytest.raises(ValueError, match="part_size_exceeds_max"):
            await writer.mpu_upload_part_stream(
                upload_id="upload",
                object_id=object_id,
                object_version=1,
                bucket_name="bucket",
                bucket_id="bucket",
                account_address="acct",
                part_number=1,
                body_iter=loser_body(),
            )

        # Winner's committed data is intact: meta present and its chunks readable.
        meta = await fs_store.get_meta(object_id, 1, 1)
        assert meta is not None and int(meta["num_chunks"]) == 2
        # chunk_1 was never touched by the loser (it only reached chunk_0) — it must be verbatim.
        assert await fs_store.get_chunk(object_id, 1, 1, 1) == b"WINNER-CHUNK-1"
    finally:
        cfg.object_chunk_size_bytes = original_chunk_size
        cfg.max_multipart_part_size = original_max_part
        cfg.cache_ttl_seconds = original_ttl


@pytest.mark.asyncio
async def test_mpu_part_uses_passed_bucket_id_no_internal_query(tmp_path, monkeypatch):
    """MPU-2: mpu_upload_part_stream takes bucket_id from the caller (the already-fetched MPU row)
    and no longer runs its own objects⋈object_versions resolution query."""
    cfg = get_config()
    saved = (cfg.object_chunk_size_bytes, cfg.max_multipart_part_size, cfg.cache_ttl_seconds)
    cfg.object_chunk_size_bytes = 4
    cfg.max_multipart_part_size = 5  # force the oversize raise so we stop before the DB tail
    cfg.cache_ttl_seconds = 60
    monkeypatch.setattr("hippius_s3.writer.object_writer.get_config", lambda: cfg)

    fetchrow_queries: list[str] = []

    class RecordingPool:
        async def fetchrow(self, query, *_args, **_kwargs):
            fetchrow_queries.append(query)
            return {"bucket_id": "should-not-be-used", "storage_version": 5}

        async def fetchval(self, *_args, **_kwargs):
            return "part-id"

        async def execute(self, *_args, **_kwargs):
            return None

    async def fake_ensure_dek(*_a, **_k) -> bytes:
        return b"\x00" * 32

    async def body_iter():
        yield b"abcd"
        yield b"ef"

    fs_store = FileSystemPartsStore(str(tmp_path))
    writer = ObjectWriter(pool=RecordingPool(), redis_client=None, fs_store=fs_store)
    monkeypatch.setattr(writer, "_ensure_and_get_v5_dek", fake_ensure_dek)

    try:
        with pytest.raises(ValueError, match="part_size_exceeds_max"):
            await writer.mpu_upload_part_stream(
                upload_id="upload",
                object_id=str(uuid.uuid4()),
                object_version=1,
                bucket_name="bucket",
                bucket_id="bkt-xyz",
                account_address="acct",
                part_number=1,
                body_iter=body_iter(),
            )
        assert not any("bucket_id" in (q or "") for q in fetchrow_queries), (
            "must not run the internal bucket_id/storage_version resolution query"
        )
    finally:
        cfg.object_chunk_size_bytes, cfg.max_multipart_part_size, cfg.cache_ttl_seconds = saved
