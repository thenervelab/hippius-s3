from __future__ import annotations

from datetime import datetime
from datetime import timezone
from unittest.mock import AsyncMock

import asyncpg
import pytest

from hippius_s3.api.s3.copy_helpers import handle_same_bucket_copy
from hippius_s3.api.s3.object_names import drop_s3_name


SRC_ID = "11111111-1111-1111-1111-111111111111"
OTHER_ID = "22222222-2222-2222-2222-222222222222"


@pytest.mark.asyncio
async def test_same_bucket_copy_inserts_alias_when_dest_free() -> None:
    db = AsyncMock()
    db.fetchrow = AsyncMock(side_effect=[None, {"object_id": SRC_ID}])
    src = {"object_id": SRC_ID, "md5_hash": "abc"}
    now = datetime.now(timezone.utc)

    resp = await handle_same_bucket_copy(
        db,
        dest_bucket_id="bucket",
        dest_key="dst",
        src_obj_row=src,
        copy_created_at=now,
    )

    assert resp is not None
    assert resp.status_code == 200
    assert db.fetchrow.await_count == 2


@pytest.mark.asyncio
async def test_same_bucket_copy_streams_when_insert_loses_race() -> None:
    db = AsyncMock()
    db.fetchrow = AsyncMock(side_effect=[None, None])
    src = {"object_id": SRC_ID, "md5_hash": "abc"}
    now = datetime.now(timezone.utc)

    resp = await handle_same_bucket_copy(
        db,
        dest_bucket_id="bucket",
        dest_key="dst",
        src_obj_row=src,
        copy_created_at=now,
    )

    assert resp is None


@pytest.mark.asyncio
async def test_same_bucket_copy_is_noop_when_dest_is_same_object() -> None:
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value={"object_id": SRC_ID})
    src = {"object_id": SRC_ID, "md5_hash": "abc"}
    now = datetime.now(timezone.utc)

    resp = await handle_same_bucket_copy(
        db,
        dest_bucket_id="bucket",
        dest_key="dst",
        src_obj_row=src,
        copy_created_at=now,
    )

    assert resp is not None
    assert resp.status_code == 200
    assert db.fetchrow.await_count == 1


@pytest.mark.asyncio
async def test_same_bucket_copy_falls_back_when_dest_is_other_primary() -> None:
    db = AsyncMock()
    db.fetchrow = AsyncMock(return_value={"object_id": OTHER_ID})
    src = {"object_id": SRC_ID, "md5_hash": "abc"}
    now = datetime.now(timezone.utc)

    resp = await handle_same_bucket_copy(
        db,
        dest_bucket_id="bucket",
        dest_key="dst",
        src_obj_row=src,
        copy_created_at=now,
    )

    assert resp is None


@pytest.mark.asyncio
async def test_drop_s3_name_alias() -> None:
    db = AsyncMock()
    db.fetchrow = AsyncMock(side_effect=[None, {"object_id": SRC_ID}])
    assert await drop_s3_name(db, "b", "k") == "alias"


@pytest.mark.asyncio
async def test_drop_s3_name_promoted() -> None:
    db = AsyncMock()
    db.fetchrow = AsyncMock(side_effect=[{"object_id": SRC_ID}, {"object_id": SRC_ID}])
    assert await drop_s3_name(db, "b", "k") == "promoted"


@pytest.mark.asyncio
async def test_drop_s3_name_last() -> None:
    db = AsyncMock()
    db.fetchrow = AsyncMock(side_effect=[{"object_id": SRC_ID}, {"object_id": None}])
    assert await drop_s3_name(db, "b", "k") == "last"


@pytest.mark.asyncio
async def test_drop_s3_name_propagates_unique_violation() -> None:
    db = AsyncMock()
    db.fetchrow = AsyncMock(side_effect=asyncpg.UniqueViolationError())
    with pytest.raises(asyncpg.UniqueViolationError):
        await drop_s3_name(db, "b", "k")
