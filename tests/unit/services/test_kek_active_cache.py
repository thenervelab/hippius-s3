import time
import uuid
from types import SimpleNamespace
from typing import Any

import pytest

from hippius_s3.services import kek_service


class _FakeConn:
    def __init__(self, row: Any) -> None:
        self.row = row
        self.fetchrow_calls = 0

    async def fetchrow(self, *_a: Any, **_k: Any) -> Any:
        self.fetchrow_calls += 1
        return self.row

    async def execute(self, *_a: Any, **_k: Any) -> None:
        return None


class _FakePool:
    def __init__(self, conn: _FakeConn) -> None:
        self.conn = conn
        self.acquire_count = 0

    def acquire(self) -> Any:
        outer = self

        class _Ctx:
            async def __aenter__(self_: Any) -> _FakeConn:
                outer.acquire_count += 1
                return outer.conn

            async def __aexit__(self_: Any, *_a: Any) -> bool:
                return False

        return _Ctx()


@pytest.fixture
def kek_env(monkeypatch: Any):
    """Patch get_config (TTL + dsn) and clear the module-level KEK caches around each test."""
    cfg = SimpleNamespace(kek_cache_ttl_seconds=3600, encryption_database_url="postgres://x")
    monkeypatch.setattr(kek_service, "get_config", lambda: cfg)
    kek_service._KEK_CACHE.clear()
    kek_service._ACTIVE_KEK_CACHE.clear()
    yield
    kek_service._KEK_CACHE.clear()
    kek_service._ACTIVE_KEK_CACHE.clear()


def _wire(monkeypatch: Any) -> tuple[_FakeConn, _FakePool, uuid.UUID]:
    kek_id = uuid.uuid4()
    row = {"kek_id": kek_id, "wrapped_kek_bytes": b"wrapped", "kms_key_id": "local"}
    conn = _FakeConn(row)
    pool = _FakePool(conn)

    async def fake_get_pool(_dsn: str) -> _FakePool:
        return pool

    async def fake_ensure(_c: Any) -> None:
        return None

    async def fake_unwrap(_wrapped: bytes, _key_id: str, _kid: uuid.UUID) -> bytes:
        return b"\x11" * 32

    monkeypatch.setattr(kek_service, "_get_pool", fake_get_pool)
    monkeypatch.setattr(kek_service, "_maybe_ensure_tables", fake_ensure)
    monkeypatch.setattr(kek_service, "_unwrap_kek", fake_unwrap)
    return conn, pool, kek_id


@pytest.mark.asyncio
async def test_second_lookup_skips_keystore(monkeypatch: Any, kek_env: Any) -> None:
    """The active kek_id + plaintext are cached on the first call, so a second call for the same
    bucket touches neither the keystore pool nor the DB."""
    conn, pool, kek_id = _wire(monkeypatch)
    bucket_id = str(uuid.uuid4())

    kid1, bytes1 = await kek_service.get_or_create_active_bucket_kek(bucket_id=bucket_id)
    assert kid1 == kek_id
    assert bytes1 == b"\x11" * 32
    assert conn.fetchrow_calls == 1
    assert pool.acquire_count == 1

    kid2, bytes2 = await kek_service.get_or_create_active_bucket_kek(bucket_id=bucket_id)
    assert (kid2, bytes2) == (kid1, bytes1)
    assert conn.fetchrow_calls == 1, "second lookup must not query the keystore"
    assert pool.acquire_count == 1, "second lookup must not acquire a keystore connection"


@pytest.mark.asyncio
async def test_requery_after_active_cache_eviction(monkeypatch: Any, kek_env: Any) -> None:
    """If the active-kek cache is evicted (TTL/rotation), the next call re-resolves from the DB."""
    conn, pool, _ = _wire(monkeypatch)
    bucket_id = str(uuid.uuid4())

    await kek_service.get_or_create_active_bucket_kek(bucket_id=bucket_id)
    assert conn.fetchrow_calls == 1

    kek_service._ACTIVE_KEK_CACHE.clear()

    await kek_service.get_or_create_active_bucket_kek(bucket_id=bucket_id)
    assert conn.fetchrow_calls == 2, "eviction must force a fresh active-kek lookup"


@pytest.mark.asyncio
async def test_active_kek_id_cache_slides_on_read(monkeypatch: Any) -> None:
    """KM-3: reading the active-kek-id refreshes its TTL (sliding), like the plaintext KEK cache, so
    a continuously-hot bucket doesn't pay a keystore SELECT every TTL to re-learn an unchanged id."""
    cfg = SimpleNamespace(kek_cache_ttl_seconds=100, encryption_database_url="postgres://x")
    monkeypatch.setattr(kek_service, "get_config", lambda: cfg)
    kek_service._ACTIVE_KEK_CACHE.clear()

    bucket_id = str(uuid.uuid4())
    kek_id = uuid.uuid4()
    # Entry about to expire (1s left) — a read must slide it to ~now+ttl.
    kek_service._ACTIVE_KEK_CACHE[bucket_id] = (kek_id, time.monotonic() + 1)
    aged_expiry = kek_service._ACTIVE_KEK_CACHE[bucket_id][1]

    got = await kek_service._get_cached_active_kek_id(bucket_id)
    slid_expiry = kek_service._ACTIVE_KEK_CACHE[bucket_id][1]

    assert got == kek_id
    assert slid_expiry > aged_expiry + 50, "read must slide the TTL forward to ~now+ttl"

    kek_service._ACTIVE_KEK_CACHE.clear()


@pytest.mark.asyncio
async def test_active_kek_id_expired_entry_not_resurrected(monkeypatch: Any) -> None:
    cfg = SimpleNamespace(kek_cache_ttl_seconds=100, encryption_database_url="postgres://x")
    monkeypatch.setattr(kek_service, "get_config", lambda: cfg)
    kek_service._ACTIVE_KEK_CACHE.clear()

    bucket_id = str(uuid.uuid4())
    kek_service._ACTIVE_KEK_CACHE[bucket_id] = (uuid.uuid4(), time.monotonic() - 1)  # already expired
    assert await kek_service._get_cached_active_kek_id(bucket_id) is None
    assert bucket_id not in kek_service._ACTIVE_KEK_CACHE
    kek_service._ACTIVE_KEK_CACHE.clear()
