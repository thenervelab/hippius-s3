"""Tests for the v5_missing_envelope_metadata fallback.

When an object is being overwritten, the current version may have NULL kek_id/wrapped_dek
for a brief window. The reader should fall back to the previous version rather than crash.
"""

from __future__ import annotations

from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest


class FakeObjCache:
    def __init__(self, exist_results: list[bool]) -> None:
        self.chunks_exist_batch = AsyncMock(return_value=exist_results)
        self.chunk_exists = AsyncMock()

    def build_chunk_key(self, object_id: str, object_version: int, part_number: int, chunk_index: int) -> str:
        return f"obj:{object_id}:v:{object_version}:part:{part_number}:chunk:{chunk_index}"


class FakeDB:
    def __init__(self, fetchrow_returns: dict | None = None, prev_version: int | None = None) -> None:
        self.fetch = AsyncMock(return_value=[])
        self.fetchrow = AsyncMock(return_value=fetchrow_returns)
        # The envelope fallback asks for the highest SERVEABLE version below the current one
        # (get_prev_serveable_version). Default to version-1 so the existing cases keep their
        # shape; the sparse-numbering case overrides it.
        default = (fetchrow_returns or {}).get("object_version")
        self.fetchval = AsyncMock(return_value=prev_version if prev_version is not None else default)


def _make_info(
    object_id: str = "obj-1",
    object_version: int = 2,
    kek_id: str | None = "kek-1",
    wrapped_dek: bytes | None = b"\x00" * 32,
    bucket_id: str = "bucket-1",
) -> dict:
    return {
        "object_id": object_id,
        "object_version": object_version,
        "current_object_version": object_version,
        "storage_version": 5,
        "bucket_id": bucket_id,
        "upload_id": "upload-1",
        "object_key": "test.bin",
        "bucket_name": "test-bucket",
        "size_bytes": 8000,
        "multipart": False,
        "enc_suite_id": "hip-enc/aes256gcm",
        "kek_id": kek_id,
        "wrapped_dek": wrapped_dek,
    }


def _make_plan_items(count: int) -> list:
    from hippius_s3.reader.types import ChunkPlanItem

    return [ChunkPlanItem(part_number=1, chunk_index=i) for i in range(count)]


# Common patches for all tests
_PATCHES = [
    patch("hippius_s3.services.object_reader.build_chunk_plan"),
    patch("hippius_s3.services.object_reader.read_parts_list"),
    patch("hippius_s3.services.object_reader.require_supported_storage_version", return_value=5),
    patch("hippius_s3.services.object_reader.get_config"),
    patch("hippius_s3.services.object_reader.unwrap_dek", return_value=b"\x01" * 32),
    patch(
        "hippius_s3.services.object_reader.get_bucket_kek_bytes",
        new_callable=AsyncMock,
        return_value=b"\x02" * 32,
    ),
    patch("hippius_s3.services.object_reader.CryptoService"),
]


def _apply_patches(mocks: list) -> None:
    """Configure common mock returns for the patched dependencies."""
    # mocks order matches _PATCHES: plan, read_parts, storage, config, unwrap, kek, crypto
    mock_plan, mock_read_parts, _, mock_config, _, _, mock_crypto = mocks
    mock_plan.return_value = _make_plan_items(1)
    mock_read_parts.return_value = [{"part_number": 1, "cid": "cid1"}]
    mock_crypto.is_supported_suite_id.return_value = True
    mock_config.return_value = MagicMock()


@pytest.mark.asyncio
async def test_envelope_present_no_fallback():
    """When envelope is present, no fallback is needed."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])
        obj_cache = FakeObjCache([True])
        db = FakeDB()
        info = _make_info(object_version=5, kek_id="kek-1", wrapped_dek=b"\x00" * 32)

        from hippius_s3.services.object_reader import build_stream_context

        ctx = await build_stream_context(db, None, obj_cache, info, rng=None, address="addr1")

        assert ctx.object_version == 5
        # DB should NOT have been queried for a previous version
        db.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_fallback_to_previous_version_on_missing_kek():
    """When kek_id is NULL on current version, falls back to previous version."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])

        prev_info = _make_info(object_version=4, kek_id="kek-1", wrapped_dek=b"\x00" * 32)
        db = FakeDB(fetchrow_returns=prev_info)
        obj_cache = FakeObjCache([True])
        info = _make_info(object_version=5, kek_id=None, wrapped_dek=None)

        from hippius_s3.services.object_reader import build_stream_context

        ctx = await build_stream_context(db, None, obj_cache, info, rng=None, address="addr1")

        # Should have served the previous version
        assert ctx.object_version == 4
        db.fetchrow.assert_awaited_once()


@pytest.mark.asyncio
async def test_fallback_to_previous_version_on_missing_wrapped_dek():
    """When only wrapped_dek is NULL (kek_id present), still falls back."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])

        prev_info = _make_info(object_version=9, kek_id="kek-1", wrapped_dek=b"\x00" * 32)
        db = FakeDB(fetchrow_returns=prev_info)
        obj_cache = FakeObjCache([True])
        info = _make_info(object_version=10, kek_id="kek-1", wrapped_dek=None)

        from hippius_s3.services.object_reader import build_stream_context

        ctx = await build_stream_context(db, None, obj_cache, info, rng=None, address="addr1")

        assert ctx.object_version == 9


@pytest.mark.asyncio
async def test_first_version_no_fallback_raises():
    """Version 1 with missing envelope has no previous version — must raise."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])

        obj_cache = FakeObjCache([True])
        db = FakeDB()
        info = _make_info(object_version=1, kek_id=None, wrapped_dek=None)

        from hippius_s3.services.object_reader import build_stream_context

        with pytest.raises(RuntimeError, match="v5_missing_envelope_metadata"):
            await build_stream_context(db, None, obj_cache, info, rng=None, address="addr1")


@pytest.mark.asyncio
async def test_previous_version_not_found_raises():
    """If the previous version doesn't exist in DB, must raise."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])

        db = FakeDB(fetchrow_returns=None)
        obj_cache = FakeObjCache([True])
        info = _make_info(object_version=5, kek_id=None, wrapped_dek=None)

        from hippius_s3.services.object_reader import build_stream_context

        with pytest.raises(RuntimeError, match="v5_missing_envelope_metadata"):
            await build_stream_context(db, None, obj_cache, info, rng=None, address="addr1")


@pytest.mark.asyncio
async def test_previous_version_also_missing_envelope_raises():
    """If the previous version also has NULL envelope, must raise (no infinite recursion)."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])

        # Previous version exists but also has NULL envelope
        prev_info = _make_info(object_version=4, kek_id=None, wrapped_dek=None)
        db = FakeDB(fetchrow_returns=prev_info)
        obj_cache = FakeObjCache([True])
        info = _make_info(object_version=5, kek_id=None, wrapped_dek=None)

        from hippius_s3.services.object_reader import build_stream_context

        with pytest.raises(RuntimeError, match="v5_missing_envelope_metadata"):
            await build_stream_context(db, None, obj_cache, info, rng=None, address="addr1")


@pytest.mark.asyncio
async def test_missing_bucket_id_raises_even_with_fallback():
    """If bucket_id itself is missing, fallback shouldn't be attempted."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])

        obj_cache = FakeObjCache([True])
        db = FakeDB()
        info = _make_info(object_version=5, kek_id="kek-1", wrapped_dek=b"\x00" * 32, bucket_id="")

        from hippius_s3.services.object_reader import build_stream_context

        with pytest.raises(RuntimeError, match="v5_missing_envelope_metadata"):
            await build_stream_context(db, None, obj_cache, info, rng=None, address="addr1")


@pytest.mark.asyncio
async def test_fallback_logs_warning(caplog):
    """The fallback should log a warning for observability."""
    import logging

    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])

        prev_info = _make_info(object_version=99, kek_id="kek-1", wrapped_dek=b"\x00" * 32)
        db = FakeDB(fetchrow_returns=prev_info)
        obj_cache = FakeObjCache([True])
        info = _make_info(object_version=100, kek_id=None, wrapped_dek=None)

        from hippius_s3.services.object_reader import build_stream_context

        with caplog.at_level(logging.WARNING):
            ctx = await build_stream_context(db, None, obj_cache, info, rng=None, address="addr1")

        assert ctx.object_version == 99
        assert any(
            "Envelope missing on v100" in rec.message and "falling back to v99" in rec.message for rec in caplog.records
        )


@pytest.mark.asyncio
async def test_cold_fallback_enqueues_download():
    """Regression (A1): a cold read of the fallback version (chunks NOT on FS) must enqueue a
    DownloadChainRequest. Previously the fallback returned a `pipeline` source without enqueuing
    anything, so the streamer hung on pub/sub until the wait timed out."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
        patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock) as m_enqueue,
        patch("hippius_s3.services.object_reader.resolve_object_backends", new_callable=AsyncMock, return_value=[]),
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])
        m3.return_value.download_coalesce_lock_ttl_seconds = 120
        m3.return_value.substrate_url = ""

        prev_info = _make_info(object_version=4, kek_id="kek-1", wrapped_dek=b"\x00" * 32)
        db = FakeDB(fetchrow_returns=prev_info)
        obj_cache = FakeObjCache([False])  # fallback version's chunk is cold (not on FS)
        info = _make_info(object_version=5, kek_id=None, wrapped_dek=None)

        redis = AsyncMock()
        redis.set = AsyncMock(return_value=True)

        from hippius_s3.services.object_reader import build_stream_context

        ctx = await build_stream_context(db, redis, obj_cache, info, rng=None, address="addr1")

        assert ctx.object_version == 4
        assert ctx.source == "pipeline"
        # The download must be enqueued for the version actually served (v4). Before the fix the
        # fallback enqueued nothing, so the streamer waited on v4 chunks that were never fetched.
        enqueued_versions = [call.args[0].object_version for call in m_enqueue.await_args_list]
        assert 4 in enqueued_versions, "the fallback version must be enqueued for download"


@pytest.mark.asyncio
async def test_warm_fallback_does_not_enqueue():
    """A warm fallback (chunks already on FS) must NOT enqueue — no wasted download work."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
        patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock) as m_enqueue,
        patch("hippius_s3.services.object_reader.resolve_object_backends", new_callable=AsyncMock, return_value=[]),
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])
        m3.return_value.download_coalesce_lock_ttl_seconds = 120
        m3.return_value.substrate_url = ""

        prev_info = _make_info(object_version=4, kek_id="kek-1", wrapped_dek=b"\x00" * 32)
        db = FakeDB(fetchrow_returns=prev_info)
        obj_cache = FakeObjCache([True])  # fallback version's chunk is warm
        info = _make_info(object_version=5, kek_id=None, wrapped_dek=None)

        from hippius_s3.services.object_reader import build_stream_context

        ctx = await build_stream_context(db, AsyncMock(), obj_cache, info, rng=None, address="addr1")

        assert ctx.object_version == 4
        assert ctx.source == "cache"
        m_enqueue.assert_not_awaited()


@pytest.mark.asyncio
async def test_cold_fallback_with_none_redis_does_not_crash():
    """A cold fallback read with redis=None (unit callers) must not crash on redis.set — the
    coalesce lock's try/except fails open (behaves as acquired), so the download is still enqueued.
    The main read path relies on this same fail-open behavior for redis=None."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
        patch("hippius_s3.services.object_reader.enqueue_download_request", new_callable=AsyncMock) as m_enqueue,
        patch("hippius_s3.services.object_reader.resolve_object_backends", new_callable=AsyncMock, return_value=[]),
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])
        m3.return_value.download_coalesce_lock_ttl_seconds = 120
        m3.return_value.substrate_url = ""

        prev_info = _make_info(object_version=4, kek_id="kek-1", wrapped_dek=b"\x00" * 32)
        db = FakeDB(fetchrow_returns=prev_info)
        obj_cache = FakeObjCache([False])  # cold
        info = _make_info(object_version=5, kek_id=None, wrapped_dek=None)

        from hippius_s3.services.object_reader import build_stream_context

        ctx = await build_stream_context(db, None, obj_cache, info, rng=None, address="addr1")

        assert ctx.object_version == 4
        assert ctx.source == "pipeline"
        # fail-open on the None redis.set: the fallback version is still enqueued (no crash)
        enqueued_versions = [call.args[0].object_version for call in m_enqueue.await_args_list]
        assert 4 in enqueued_versions


@pytest.mark.asyncio
async def test_fallback_queries_the_highest_serveable_version_below_current():
    """The fallback resolves its target by query, not by decrementing. Version numbers are sparse:
    an aborted MPU retains its reserved row and the migrator mints versions out of band, so
    version-1 can be a placeholder with no envelope."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])

        prev_info = _make_info(object_version=41, kek_id="kek-1", wrapped_dek=b"\x00" * 32)
        db = FakeDB(fetchrow_returns=prev_info)
        obj_cache = FakeObjCache([True])
        info = _make_info(object_version=42, kek_id=None, wrapped_dek=None)
        info["bucket_name"] = "my-bucket"
        info["object_key"] = "my-key.bin"

        from hippius_s3.services.object_reader import build_stream_context

        await build_stream_context(db, None, obj_cache, info, rng=None, address="addr1")

        # Verify the fallback query was called with correct args
        call_args = db.fetchrow.call_args
        assert call_args[0][1] == "my-bucket"
        assert call_args[0][2] == "my-key.bin"
        assert call_args[0][3] == 41  # whatever get_prev_serveable_version resolved to


@pytest.mark.asyncio
async def test_fallback_skips_a_gap_in_version_numbers():
    """v42 has no envelope and v41 is a retained abort placeholder, so the fallback must land on
    v40. Decrementing would query v41, get nothing back, and turn a recoverable read into a 500."""
    with (
        _PATCHES[0] as m0,
        _PATCHES[1] as m1,
        _PATCHES[2] as m2,
        _PATCHES[3] as m3,
        _PATCHES[4] as m4,
        _PATCHES[5] as m5,
        _PATCHES[6] as m6,
    ):
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])

        prev_info = _make_info(object_version=40, kek_id="kek-1", wrapped_dek=b"\x00" * 32)
        db = FakeDB(fetchrow_returns=prev_info, prev_version=40)
        info = _make_info(object_version=42, kek_id=None, wrapped_dek=None)

        from hippius_s3.services.object_reader import build_stream_context

        await build_stream_context(db, None, FakeObjCache([True]), info, rng=None, address="addr1")

        assert db.fetchval.await_args[0][2] == 42, "the gap query must be anchored at the current version"
        assert db.fetchrow.call_args[0][3] == 40, "the fallback skipped past the placeholder to v40"
