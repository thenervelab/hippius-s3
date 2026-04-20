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
    def __init__(self, fetchrow_returns: dict | None = None) -> None:
        self.fetch = AsyncMock(return_value=[])
        self.fetchrow = AsyncMock(return_value=fetchrow_returns)


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
    with _PATCHES[0] as m0, _PATCHES[1] as m1, _PATCHES[2] as m2, _PATCHES[3] as m3, \
         _PATCHES[4] as m4, _PATCHES[5] as m5, _PATCHES[6] as m6:
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
    with _PATCHES[0] as m0, _PATCHES[1] as m1, _PATCHES[2] as m2, _PATCHES[3] as m3, \
         _PATCHES[4] as m4, _PATCHES[5] as m5, _PATCHES[6] as m6:
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
    with _PATCHES[0] as m0, _PATCHES[1] as m1, _PATCHES[2] as m2, _PATCHES[3] as m3, \
         _PATCHES[4] as m4, _PATCHES[5] as m5, _PATCHES[6] as m6:
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
    with _PATCHES[0] as m0, _PATCHES[1] as m1, _PATCHES[2] as m2, _PATCHES[3] as m3, \
         _PATCHES[4] as m4, _PATCHES[5] as m5, _PATCHES[6] as m6:
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
    with _PATCHES[0] as m0, _PATCHES[1] as m1, _PATCHES[2] as m2, _PATCHES[3] as m3, \
         _PATCHES[4] as m4, _PATCHES[5] as m5, _PATCHES[6] as m6:
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
    with _PATCHES[0] as m0, _PATCHES[1] as m1, _PATCHES[2] as m2, _PATCHES[3] as m3, \
         _PATCHES[4] as m4, _PATCHES[5] as m5, _PATCHES[6] as m6:
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
    with _PATCHES[0] as m0, _PATCHES[1] as m1, _PATCHES[2] as m2, _PATCHES[3] as m3, \
         _PATCHES[4] as m4, _PATCHES[5] as m5, _PATCHES[6] as m6:
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

    with _PATCHES[0] as m0, _PATCHES[1] as m1, _PATCHES[2] as m2, _PATCHES[3] as m3, \
         _PATCHES[4] as m4, _PATCHES[5] as m5, _PATCHES[6] as m6:
        _apply_patches([m0, m1, m2, m3, m4, m5, m6])

        prev_info = _make_info(object_version=99, kek_id="kek-1", wrapped_dek=b"\x00" * 32)
        db = FakeDB(fetchrow_returns=prev_info)
        obj_cache = FakeObjCache([True])
        info = _make_info(object_version=100, kek_id=None, wrapped_dek=None)

        from hippius_s3.services.object_reader import build_stream_context

        with caplog.at_level(logging.WARNING):
            ctx = await build_stream_context(db, None, obj_cache, info, rng=None, address="addr1")

        assert ctx.object_version == 99
        assert any("Envelope missing on v100" in rec.message and "falling back to v99" in rec.message for rec in caplog.records)


@pytest.mark.asyncio
async def test_fallback_queries_correct_version():
    """The fallback query uses version-1, not version-2 or some other number."""
    with _PATCHES[0] as m0, _PATCHES[1] as m1, _PATCHES[2] as m2, _PATCHES[3] as m3, \
         _PATCHES[4] as m4, _PATCHES[5] as m5, _PATCHES[6] as m6:
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
        assert call_args[0][3] == 41  # version - 1
