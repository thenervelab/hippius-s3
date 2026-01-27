"""Unit tests for copy_service_v5 fast path copy functions."""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from hippius_s3.api.s3.errors import S3Error
from hippius_s3.services.copy_service_v5 import copy_chunk_cids
from hippius_s3.services.copy_service_v5 import rewrap_encryption_envelope


@pytest.mark.skip(reason="Complex mocking with many nested DB calls; covered by E2E tests")
async def test_create_destination_objects_missing_dest_part():
    """Test error when destination part is not created.

    Note: This test is skipped because it requires extensive mocking of nested
    database calls. The error path is indirectly tested through E2E tests that
    would fail if this logic broke.
    """
    pass


@pytest.mark.asyncio
async def test_copy_chunk_cids_success():
    """Test successful copying of chunk CIDs."""
    db = AsyncMock()

    dest_obj_id = str(uuid.uuid4())

    dest_part = {"part_id": "part-789"}
    db.fetchrow.return_value = dest_part

    chunk_rows = [
        (0, "bafychunk1", 1024, 1000),
        (1, "bafychunk2", 2048, 2000),
        (2, "bafychunk3", 512, 500),
    ]

    await copy_chunk_cids(
        db=db,
        chunk_rows=chunk_rows,
        object_id=dest_obj_id,
        dest_object_version=1,
    )

    assert db.execute.call_count == 3

    first_call_args = db.execute.call_args_list[0][0]
    assert first_call_args[1] == "part-789"
    assert first_call_args[2] == 0
    assert first_call_args[3] == "bafychunk1"


@pytest.mark.asyncio
async def test_copy_chunk_cids_missing_destination_part():
    """Test error when destination part not found during chunk copy."""
    db = AsyncMock()
    dest_obj_id = str(uuid.uuid4())

    db.fetchrow.return_value = None

    chunk_rows = [
        (0, "bafychunk1", 1024, 1000),
    ]

    with pytest.raises(S3Error) as exc_info:
        await copy_chunk_cids(
            db=db,
            chunk_rows=chunk_rows,
            object_id=dest_obj_id,
            dest_object_version=1,
        )

    assert exc_info.value.code == "InternalError"
    assert "Destination part not found" in exc_info.value.message


@pytest.mark.asyncio
async def test_copy_chunk_cids_with_optional_sizes():
    """Test copying chunks when optional sizes are None."""
    db = AsyncMock()

    dest_obj_id = str(uuid.uuid4())

    dest_part = {"part_id": "part-789"}
    db.fetchrow.return_value = dest_part

    chunk_rows = [
        (0, "bafychunk1", None, None),
        (1, "bafychunk2", 2048, None),
    ]

    await copy_chunk_cids(
        db=db,
        chunk_rows=chunk_rows,
        object_id=dest_obj_id,
        dest_object_version=1,
    )

    assert db.execute.call_count == 2

    first_call_args = db.execute.call_args_list[0][0]
    assert first_call_args[4] == 0

    second_call_args = db.execute.call_args_list[1][0]
    assert second_call_args[4] == 2048
    assert second_call_args[5] is None


@pytest.mark.asyncio
async def test_rewrap_encryption_envelope_success(monkeypatch):
    """Test successful rewrapping of encryption envelope."""
    db = AsyncMock()

    src_bucket_id = str(uuid.uuid4())
    dest_bucket_id = str(uuid.uuid4())
    src_obj_id = str(uuid.uuid4())
    dest_obj_id = str(uuid.uuid4())

    source_bucket = {"bucket_id": src_bucket_id}
    dest_bucket = {"bucket_id": dest_bucket_id}

    src_obj_row = {
        "object_id": src_obj_id,
        "object_version": 1,
        "kek_id": "kek-src-111",
        "wrapped_dek": b"wrapped_source_dek_bytes",
    }

    mock_get_bucket_kek_bytes = AsyncMock(return_value=b"source_kek_32_bytes_12345678901")
    mock_get_or_create_active_bucket_kek = AsyncMock(return_value=("kek-dest-222", b"dest_kek_32_bytes_12345678901234"))
    mock_unwrap_dek = MagicMock(return_value=b"unwrapped_dek_32_bytes_12345678")
    mock_wrap_dek = MagicMock(return_value=b"wrapped_dest_dek_bytes")

    monkeypatch.setattr(
        "hippius_s3.services.kek_service.get_bucket_kek_bytes",
        mock_get_bucket_kek_bytes,
    )
    monkeypatch.setattr(
        "hippius_s3.services.kek_service.get_or_create_active_bucket_kek",
        mock_get_or_create_active_bucket_kek,
    )
    monkeypatch.setattr(
        "hippius_s3.services.envelope_service.unwrap_dek",
        mock_unwrap_dek,
    )
    monkeypatch.setattr(
        "hippius_s3.services.envelope_service.wrap_dek",
        mock_wrap_dek,
    )

    dest_kek_id, dest_wrapped_dek = await rewrap_encryption_envelope(
        db=db,
        source_bucket=source_bucket,
        dest_bucket=dest_bucket,
        src_obj_row=src_obj_row,
        object_id=dest_obj_id,
        dest_object_version=1,
    )

    assert dest_kek_id == "kek-dest-222"
    assert dest_wrapped_dek == b"wrapped_dest_dek_bytes"

    mock_get_bucket_kek_bytes.assert_called_once()
    mock_get_or_create_active_bucket_kek.assert_called_once()
    mock_unwrap_dek.assert_called_once()
    mock_wrap_dek.assert_called_once()


@pytest.mark.asyncio
async def test_rewrap_encryption_envelope_missing_source_kek():
    """Test error when source object missing kek_id."""
    db = AsyncMock()

    src_bucket_id = str(uuid.uuid4())
    dest_bucket_id = str(uuid.uuid4())
    src_obj_id = str(uuid.uuid4())
    dest_obj_id = str(uuid.uuid4())

    source_bucket = {"bucket_id": src_bucket_id}
    dest_bucket = {"bucket_id": dest_bucket_id}

    src_obj_row = {
        "object_id": src_obj_id,
        "object_version": 1,
        "kek_id": None,
        "wrapped_dek": b"wrapped_dek",
    }

    with pytest.raises(S3Error) as exc_info:
        await rewrap_encryption_envelope(
            db=db,
            source_bucket=source_bucket,
            dest_bucket=dest_bucket,
            src_obj_row=src_obj_row,
            object_id=dest_obj_id,
            dest_object_version=1,
        )

    assert exc_info.value.code == "InternalError"
    assert "Missing v5 envelope metadata" in exc_info.value.message


@pytest.mark.asyncio
async def test_rewrap_encryption_envelope_missing_source_wrapped_dek():
    """Test error when source object missing wrapped_dek."""
    db = AsyncMock()

    src_bucket_id = str(uuid.uuid4())
    dest_bucket_id = str(uuid.uuid4())
    src_obj_id = str(uuid.uuid4())
    dest_obj_id = str(uuid.uuid4())

    source_bucket = {"bucket_id": src_bucket_id}
    dest_bucket = {"bucket_id": dest_bucket_id}

    src_obj_row = {
        "object_id": src_obj_id,
        "object_version": 1,
        "kek_id": "kek-src-111",
        "wrapped_dek": None,
    }

    with pytest.raises(S3Error) as exc_info:
        await rewrap_encryption_envelope(
            db=db,
            source_bucket=source_bucket,
            dest_bucket=dest_bucket,
            src_obj_row=src_obj_row,
            object_id=dest_obj_id,
            dest_object_version=1,
        )

    assert exc_info.value.code == "InternalError"
    assert "Missing v5 envelope metadata" in exc_info.value.message
