"""Unit tests for the drain-gated upload-promoter (s3-2.1 PR-7d).

The promoter turns a `cephor_replicated` signal (a part landed on ceph) into a
per-part backend UploadChainRequest, rebuilt by object_id now that the data is
readable. These cover the request-building + the notification parsing; the LISTEN /
sweep wiring is exercised on staging.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from hippius_s3.workers import upload_promoter as up


OBJ = "11111111-1111-1111-1111-111111111111"


def _db_pool(*, row, upload_id):
    conn = MagicMock()
    conn.fetchrow = AsyncMock(return_value=row)
    conn.fetchval = AsyncMock(return_value=upload_id)
    pool = MagicMock()
    pool.acquire = MagicMock(return_value=MagicMock(__aenter__=AsyncMock(return_value=conn), __aexit__=AsyncMock()))
    return pool


@pytest.mark.asyncio
async def test_promote_part_builds_single_part_request_and_fans_out() -> None:
    pool = _db_pool(
        row={"bucket_name": "buck", "object_key": "key/obj", "address": "5MainAcct"},
        upload_id="upl-123",
    )
    config = SimpleNamespace(upload_backends=["arion", "ovh"])

    with patch.object(up, "enqueue_upload_to_backends", new=AsyncMock()) as enqueue:
        ok = await up.promote_part(pool, config, object_id=OBJ, object_version=2, part_number=7)

    assert ok is True
    enqueue.assert_awaited_once()
    req = enqueue.await_args.args[0]
    assert req.object_id == OBJ
    assert req.object_version == 2
    assert req.address == "5MainAcct"
    assert req.bucket_name == "buck"
    assert req.object_key == "key/obj"
    assert req.upload_id == "upl-123"
    assert req.upload_backends == ["arion", "ovh"]
    assert [c.id for c in req.chunks] == [7], "promotes exactly the one replicated part"


@pytest.mark.asyncio
async def test_promote_part_skips_when_address_missing() -> None:
    # A version row with no address (e.g. written before the promoter path was on) can't
    # be promoted — skip and let the sweep retry, don't enqueue a broken request.
    pool = _db_pool(row={"bucket_name": "b", "object_key": "k", "address": None}, upload_id=None)
    config = SimpleNamespace(upload_backends=["arion"])

    with patch.object(up, "enqueue_upload_to_backends", new=AsyncMock()) as enqueue:
        ok = await up.promote_part(pool, config, object_id=OBJ, object_version=1, part_number=1)

    assert ok is False
    enqueue.assert_not_called()


@pytest.mark.asyncio
async def test_promote_part_simple_object_has_no_upload_id() -> None:
    pool = _db_pool(
        row={"bucket_name": "b", "object_key": "k", "address": "5Acct"},
        upload_id=None,
    )
    config = SimpleNamespace(upload_backends=["arion"])

    with patch.object(up, "enqueue_upload_to_backends", new=AsyncMock()) as enqueue:
        await up.promote_part(pool, config, object_id=OBJ, object_version=1, part_number=1)

    assert enqueue.await_args.args[0].upload_id is None


def test_parse_notification_valid() -> None:
    assert up._parse_notification(f"{OBJ}:3:12") == (OBJ, 3, 12)


@pytest.mark.parametrize("bad", ["", "not-a-payload", f"{OBJ}:3", f"{OBJ}:x:1", f"{OBJ}:1:y"])
def test_parse_notification_rejects_malformed(bad: str) -> None:
    assert up._parse_notification(bad) is None
