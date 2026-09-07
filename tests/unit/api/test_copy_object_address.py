import uuid
from datetime import datetime
from datetime import timezone
from types import SimpleNamespace
from typing import Any

import pytest

from hippius_s3.api.s3 import copy_helpers
from hippius_s3.api.s3.copy_helpers import handle_streaming_copy
from hippius_s3.config import get_config
from hippius_s3.writer.types import PutResult
from tests.unit._fake_pool import make_fake_pool


DEST_OBJECT_ID = str(uuid.uuid4())
DEST_OBJECT_VERSION = 7
DEST_UPLOAD_ID = str(uuid.uuid4())


def _fake_request() -> Any:
    return SimpleNamespace(
        state=SimpleNamespace(
            account=SimpleNamespace(main_account="acct-main", id="sub-1"),
            main_account_id="acct-main",
            ray_id="ray-1",
        ),
        app=SimpleNamespace(state=SimpleNamespace(fs_store=SimpleNamespace(), obj_cache=SimpleNamespace())),
    )


def _src_obj_row(src_object_id: str) -> dict[str, Any]:
    return {
        "object_id": src_object_id,
        "bucket_id": str(uuid.uuid4()),
        "object_key": "src-key",
        "object_version": 3,
        "storage_version": 5,
        "metadata": {},
        "multipart": False,
        "content_type": "text/plain",
        "enc_chunk_size_bytes": None,
        "enc_suite_id": None,
        "encryption_version": None,
        "kek_id": None,
        "wrapped_dek": None,
    }


def _patch_copy_pipeline(monkeypatch: pytest.MonkeyPatch) -> None:
    async def fake_stream_object(*_a: Any, **_kw: Any) -> Any:
        async def _iter() -> Any:
            yield b"payload"

        return _iter()

    async def fake_put(_self: Any, **_kw: Any) -> PutResult:
        return PutResult(
            object_id=DEST_OBJECT_ID,
            etag="dest-etag",
            size_bytes=7,
            upload_id=DEST_UPLOAD_ID,
            object_version=DEST_OBJECT_VERSION,
        )

    monkeypatch.setattr(copy_helpers, "stream_object", fake_stream_object)
    monkeypatch.setattr(copy_helpers.ObjectWriter, "put_simple_stream_full", fake_put)


async def _run_copy(pool: Any) -> Any:
    src_object_id = str(uuid.uuid4())
    return await handle_streaming_copy(
        pool=pool,
        redis_client=SimpleNamespace(),
        request=_fake_request(),
        source_bucket={"bucket_id": str(uuid.uuid4()), "bucket_name": "src-bkt", "is_public": False},
        dest_bucket={"bucket_id": str(uuid.uuid4()), "bucket_name": "dst-bkt"},
        source_object={"object_key": "src-key", "content_type": "text/plain"},
        src_obj_row=_src_obj_row(src_object_id),
        object_id=DEST_OBJECT_ID,
        object_key="dst-key",
        copy_created_at=datetime.now(timezone.utc),
        config=get_config(),
    )


@pytest.mark.asyncio
async def test_streaming_copy_persists_dest_address_then_completes_upload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Without the address the Rust drain never enqueues the destination (C1)."""
    _patch_copy_pipeline(monkeypatch)

    captured: dict[str, Any] = {}

    async def fake_set_address(_db: Any, *, object_id: str, object_version: int, address: str) -> None:
        captured["object_id"] = object_id
        captured["object_version"] = object_version
        captured["address"] = address
        captured["completed_before_address"] = _completed_queries(pool)

    monkeypatch.setattr(copy_helpers, "set_object_version_address", fake_set_address)

    pool = make_fake_pool()
    response = await _run_copy(pool)

    assert response.status_code == 200
    assert captured["object_id"] == DEST_OBJECT_ID
    assert captured["object_version"] == DEST_OBJECT_VERSION
    assert captured["address"] == "acct-main"

    # is_completed must flip only after the address landed, so a failed address write leaves the
    # row eligible for the DELETE-cascade cleanup.
    assert captured["completed_before_address"] == []
    completed = _completed_queries(pool)
    assert len(completed) == 1
    assert completed[0]["args"] == (DEST_UPLOAD_ID,)


@pytest.mark.asyncio
async def test_streaming_copy_unserves_dest_and_raises_when_address_write_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _patch_copy_pipeline(monkeypatch)

    async def failing_set_address(*_a: Any, **_kw: Any) -> None:
        raise RuntimeError("address write failed")

    monkeypatch.setattr(copy_helpers, "set_object_version_address", failing_set_address)

    pool = make_fake_pool()
    with pytest.raises(RuntimeError, match="address write failed"):
        await _run_copy(pool)

    unserve = [e for e in pool.events if e.get("method") == "execute" and "size_bytes = 0" in (e.get("query") or "")]
    assert len(unserve) == 1
    assert unserve[0]["args"] == (DEST_OBJECT_ID, DEST_OBJECT_VERSION)
    assert _completed_queries(pool) == []


def _completed_queries(pool: Any) -> list[dict[str, Any]]:
    return [e for e in pool.events if e.get("method") == "execute" and "is_completed = TRUE" in (e.get("query") or "")]
