"""KM-1: the GET-path KEK unwrap (get_bucket_kek_bytes) must singleflight cold misses.

The A14 singleflight (_kek_unwrap_lock) was wired only into the PUT path, so N concurrent cold GETs
sharing a bucket's active KEK each hit KMS. Mirror it here: N concurrent cold reads for the same
(bucket, kek) collapse to one KMS unwrap; distinct keks still unwrap independently.
"""

from __future__ import annotations

import asyncio
import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from hippius_s3.services import kek_service


class _FakeConn:
    async def fetchrow(self, *_a: Any, **_k: Any) -> Any:
        return {"wrapped_kek_bytes": b"wrapped", "kms_key_id": "local"}

    async def execute(self, *_a: Any, **_k: Any) -> None:
        return None


class _FakePool:
    def acquire(self) -> Any:
        class _Ctx:
            async def __aenter__(self_: Any) -> _FakeConn:
                return _FakeConn()

            async def __aexit__(self_: Any, *_a: Any) -> bool:
                return False

        return _Ctx()


@pytest.fixture
def kek_env(monkeypatch: Any):
    cfg = SimpleNamespace(kek_cache_ttl_seconds=3600, encryption_database_url="postgres://x")
    monkeypatch.setattr(kek_service, "get_config", lambda: cfg)

    async def fake_get_pool(_dsn: str) -> _FakePool:
        return _FakePool()

    async def fake_ensure(_c: Any) -> None:
        return None

    monkeypatch.setattr(kek_service, "_get_pool", fake_get_pool)
    monkeypatch.setattr(kek_service, "_maybe_ensure_tables", fake_ensure)
    kek_service._KEK_CACHE.clear()
    kek_service._kek_unwrap_locks.clear()
    yield
    kek_service._KEK_CACHE.clear()
    kek_service._kek_unwrap_locks.clear()


@pytest.mark.asyncio
async def test_concurrent_cold_reads_collapse_to_one_unwrap(monkeypatch: Any, kek_env: Any) -> None:
    calls = {"n": 0}

    async def counting_unwrap(_w: bytes, _kid: str, _k: uuid.UUID) -> bytes:
        calls["n"] += 1
        await asyncio.sleep(0.05)  # hold the window open so all callers race the cold miss
        return b"\x22" * 32

    monkeypatch.setattr(kek_service, "_unwrap_kek", counting_unwrap)

    bucket_id = str(uuid.uuid4())
    kek_id = uuid.uuid4()
    results = await asyncio.gather(
        *[kek_service.get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=kek_id) for _ in range(8)]
    )

    assert calls["n"] == 1, "concurrent cold reads must collapse to a single KMS unwrap"
    assert all(r == b"\x22" * 32 for r in results)


@pytest.mark.asyncio
async def test_distinct_keks_do_not_collapse(monkeypatch: Any, kek_env: Any) -> None:
    calls = {"n": 0}

    async def counting_unwrap(_w: bytes, _kid: str, _k: uuid.UUID) -> bytes:
        calls["n"] += 1
        await asyncio.sleep(0.02)
        return b"\x33" * 32

    monkeypatch.setattr(kek_service, "_unwrap_kek", counting_unwrap)

    bucket_id = str(uuid.uuid4())
    k1, k2 = uuid.uuid4(), uuid.uuid4()
    await asyncio.gather(
        kek_service.get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=k1),
        kek_service.get_bucket_kek_bytes(bucket_id=bucket_id, kek_id=k2),
    )
    assert calls["n"] == 2, "distinct keks must each unwrap (no cross-key collapse)"
