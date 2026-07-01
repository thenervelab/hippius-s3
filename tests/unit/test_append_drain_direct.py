"""Regression guard for the S4 append drain-direct fix.

Before: `handle_append` called `writer_enqueue_upload` at append time, so an appended
part was produced by BOTH the api and (on replication) the Rust drain — a double-produce
that only the idempotent uploader hid, and which stranded legacy (`address IS NULL`)
versions the drain would defer forever.

After: append persists the main-account address via `set_object_version_address` (exactly
like PUT/MPU) and enqueues nothing — the drain becomes the sole upload producer.

These tests pin that wiring: address is persisted for the appended part's version, and no
upload is enqueued from the append path.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.api.s3.extensions import append as append_mod


OBJ = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
ADDRESS = "5FmainAccountAddressSS58"


def _request(pool: Any) -> MagicMock:
    req = MagicMock()
    req.headers = {"x-amz-meta-append-if-version": "3"}
    req.app.state.postgres_pool = pool
    req.app.state.fs_store = MagicMock()
    req.state.account.main_account = ADDRESS
    req.state.seed_phrase = "seed words"
    req.state.ray_id = "ray-123"
    return req


def _append_result() -> dict:
    return {
        "object_id": OBJ,
        "part_number": 4,
        "etag": "d41d8cd98f00b204e9800998ecf8427e",
        "object_version": 1,
        "new_append_version": 4,
        "size_bytes": 7,
        "upload_id": "11111111-2222-3333-4444-555555555555",
    }


async def _noop_body():  # pragma: no cover - not consumed by the mocked writer
    yield b""


@pytest.mark.asyncio
async def test_append_persists_address_and_does_not_enqueue():
    """The happy path: address is written for (object_id, cov); nothing is enqueued."""
    pool = MagicMock()
    redis = MagicMock()
    redis.get = AsyncMock(return_value=None)
    redis.setex = AsyncMock()

    writer = MagicMock()
    writer.append_stream = AsyncMock(return_value=_append_result())

    with (
        patch.object(append_mod, "ObjectWriter", return_value=writer),
        patch.object(append_mod, "set_object_version_address", AsyncMock()) as set_addr,
    ):
        resp = await append_mod.handle_append(
            _request(pool),
            MagicMock(),
            redis,
            bucket={"bucket_id": "b1"},
            bucket_id="b1",
            bucket_name="bucket",
            object_key="log/a.txt",
            body_iter=_noop_body(),
        )

    assert resp.status_code == 200
    # Address persisted for the appended part's version — the drain's enqueue signal.
    set_addr.assert_awaited_once()
    _, kwargs = set_addr.await_args
    assert kwargs["object_id"] == OBJ
    assert kwargs["object_version"] == 1
    assert kwargs["address"] == ADDRESS
    # The pool (not a txn conn) is passed positionally, mirroring PUT/MPU.
    assert set_addr.await_args.args[0] is pool


@pytest.mark.asyncio
async def test_append_has_no_enqueue_symbol():
    """The append module must not import/use any upload-enqueue helper — the drain is
    the sole producer. Guards against a future re-introduction of the double-produce."""
    assert not hasattr(append_mod, "writer_enqueue_upload")
    assert not hasattr(append_mod, "enqueue_upload")
    assert hasattr(append_mod, "set_object_version_address")


@pytest.mark.asyncio
async def test_append_address_failure_bubbles_up():
    """If persisting the address fails, the error must bubble (no silent swallow) — a
    part with no address is never enqueued by the drain, so this must surface, not hide."""
    pool = MagicMock()
    redis = MagicMock()
    redis.get = AsyncMock(return_value=None)

    writer = MagicMock()
    writer.append_stream = AsyncMock(return_value=_append_result())

    with (
        patch.object(append_mod, "ObjectWriter", return_value=writer),
        patch.object(append_mod, "set_object_version_address", AsyncMock(side_effect=RuntimeError("db down"))),
        pytest.raises(RuntimeError, match="db down"),
    ):
        await append_mod.handle_append(
            _request(pool),
            MagicMock(),
            redis,
            bucket={"bucket_id": "b1"},
            bucket_id="b1",
            bucket_name="bucket",
            object_key="log/a.txt",
            body_iter=_noop_body(),
        )
