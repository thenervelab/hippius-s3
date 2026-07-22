"""MPU-1: parts with an existing DEK envelope resolve it without the FOR UPDATE row lock.

Every UploadPart called _ensure_and_get_v5_dek, which took `SELECT ... FROM object_versions ... FOR
UPDATE` on the shared version row — serializing all concurrent parts through the DEK section. Parts
2..N (envelope already populated by the first part) should read the DEK non-locking; only the
create/rotate case needs the lock, and the first-part race stays idempotent via the in-lock recheck.
"""

from __future__ import annotations

import uuid
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest

from hippius_s3.writer.object_writer import ObjectWriter


class _ACtx:
    def __init__(self, val: Any) -> None:
        self._val = val

    async def __aenter__(self) -> Any:
        return self._val

    async def __aexit__(self, *_a: Any) -> bool:
        return False


class _FakeConn:
    def __init__(self, locked_row: Any) -> None:
        self._locked_row = locked_row

    async def fetchrow(self, *_a: Any, **_k: Any) -> Any:
        return self._locked_row

    async def execute(self, *_a: Any, **_k: Any) -> None:
        return None

    def transaction(self) -> _ACtx:
        return _ACtx(None)


class _FakePool:
    def __init__(self, pre_row: Any, locked_row: Any) -> None:
        self._pre_row = pre_row
        self._locked_row = locked_row
        self.pre_fetchrow_calls = 0
        self.acquire_calls = 0

    async def fetchrow(self, *_a: Any, **_k: Any) -> Any:  # the non-locking pre-check
        self.pre_fetchrow_calls += 1
        return self._pre_row

    def acquire(self) -> _ACtx:  # the FOR UPDATE transaction path
        self.acquire_calls += 1
        return _ACtx(_FakeConn(self._locked_row))


@pytest.fixture
def crypto_stubs(monkeypatch: Any):
    monkeypatch.setattr("hippius_s3.services.kek_service.get_bucket_kek_bytes", AsyncMock(return_value=b"k" * 32))
    monkeypatch.setattr(
        "hippius_s3.services.kek_service.get_or_create_active_bucket_kek",
        AsyncMock(return_value=(uuid.uuid4(), b"k" * 32)),
    )
    monkeypatch.setattr("hippius_s3.services.envelope_service.unwrap_dek", MagicMock(return_value=b"d" * 32))
    monkeypatch.setattr("hippius_s3.services.envelope_service.wrap_dek", MagicMock(return_value=b"w" * 48))
    monkeypatch.setattr("hippius_s3.services.envelope_service.generate_dek", MagicMock(return_value=b"n" * 32))


def _writer(pool: Any) -> ObjectWriter:
    return ObjectWriter(pool=pool, redis_client=None, fs_store=SimpleNamespace())


async def _call(writer: ObjectWriter, rotate: bool) -> bytes:
    return await writer._ensure_and_get_v5_dek(
        bucket_id="b",
        object_id="o",
        object_version=1,
        chunk_size=4194304,
        suite_id="hip-enc/aes256gcm",
        rotate=rotate,
    )


@pytest.mark.asyncio
async def test_existing_envelope_read_is_non_locking(crypto_stubs: Any) -> None:
    populated = {"storage_version": 5, "kek_id": uuid.uuid4(), "wrapped_dek": b"w"}
    pool = _FakePool(pre_row=populated, locked_row=populated)

    dek = await _call(_writer(pool), rotate=False)

    assert dek == b"d" * 32
    assert pool.acquire_calls == 0, "an existing envelope must resolve without FOR UPDATE"
    assert pool.pre_fetchrow_calls == 1


@pytest.mark.asyncio
async def test_rotate_still_takes_the_lock(crypto_stubs: Any) -> None:
    populated = {"storage_version": 5, "kek_id": uuid.uuid4(), "wrapped_dek": b"w"}
    pool = _FakePool(pre_row=populated, locked_row=populated)

    await _call(_writer(pool), rotate=True)

    assert pool.acquire_calls == 1, "rotate must take the FOR UPDATE lock"


@pytest.mark.asyncio
async def test_null_envelope_falls_through_to_lock(crypto_stubs: Any) -> None:
    # First part racing: envelope not yet written. Pre-check sees NULLs → must escalate to the lock
    # so DEK creation is serialized and idempotent.
    null_env = {"storage_version": 5, "kek_id": None, "wrapped_dek": None}
    pool = _FakePool(pre_row=null_env, locked_row=null_env)

    await _call(_writer(pool), rotate=False)

    assert pool.acquire_calls == 1, "a NULL envelope must escalate to the FOR UPDATE create path"
