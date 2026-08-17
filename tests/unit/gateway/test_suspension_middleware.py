"""Unit tests for account suspension enforcement (issue #421).

Covers the pure decision helper (read_only write-matrix), the Redis-cached lookup,
and the middleware wiring — including the two traps the placement exists for: bearer
auth (whose account_id gets clobbered to "anonymous" downstream) and master tokens
(which bypass ACL entirely, so suspension cannot live inside acl_middleware).
"""

from typing import Any

import pytest
from fastapi import FastAPI
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.middlewares.suspension import suspension_middleware
from gateway.services.suspension import MODE_FULL
from gateway.services.suspension import MODE_READ_ONLY
from gateway.services.suspension import get_account_suspension
from gateway.services.suspension import suspension_blocks
from gateway.services.suspension import suspension_cache_key


ACCOUNT = "5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty"


class FakeRedis:
    def __init__(self) -> None:
        self.store: dict[str, bytes] = {}
        self.setex_calls: list[tuple[str, int, Any]] = []

    async def get(self, key: str) -> bytes | None:
        return self.store.get(key)

    async def setex(self, key: str, ttl: int, value: Any) -> None:
        self.setex_calls.append((key, ttl, value))
        self.store[key] = value if isinstance(value, bytes) else str(value).encode("utf-8")

    async def delete(self, key: str) -> None:
        self.store.pop(key, None)


class FakePool:
    def __init__(self, mode: str | None = None) -> None:
        self.mode = mode
        self.fetchrow_calls: list[tuple[str, tuple[Any, ...]]] = []

    async def fetchrow(self, query: str, *args: Any) -> Any:
        self.fetchrow_calls.append((query, args))
        if self.mode is None:
            return None
        return {"mode": self.mode}

    async def execute(self, query: str, *args: Any) -> None:
        return None


class TestSuspensionBlocks:
    def test_full_blocks_everything(self) -> None:
        for method in ("GET", "HEAD", "PUT", "POST", "DELETE", "PATCH"):
            assert suspension_blocks(MODE_FULL, method=method, query_params={}, has_key=True)

    def test_read_only_allows_reads(self) -> None:
        assert not suspension_blocks(MODE_READ_ONLY, method="GET", query_params={}, has_key=True)
        assert not suspension_blocks(MODE_READ_ONLY, method="HEAD", query_params={}, has_key=True)
        assert not suspension_blocks(MODE_READ_ONLY, method="GET", query_params={"acl": ""}, has_key=False)
        assert not suspension_blocks(MODE_READ_ONLY, method="GET", query_params={"list-type": "2"}, has_key=False)

    def test_read_only_blocks_writes(self) -> None:
        assert suspension_blocks(MODE_READ_ONLY, method="PUT", query_params={}, has_key=True)
        assert suspension_blocks(MODE_READ_ONLY, method="DELETE", query_params={}, has_key=True)
        # Multi-delete and MPU are POST/GET+query shapes, not plain writes — the ACL
        # matrix classifies them as WRITE and read_only must too.
        assert suspension_blocks(MODE_READ_ONLY, method="POST", query_params={"delete": ""}, has_key=False)
        assert suspension_blocks(MODE_READ_ONLY, method="POST", query_params={"uploads": ""}, has_key=True)
        assert suspension_blocks(MODE_READ_ONLY, method="PUT", query_params={"acl": ""}, has_key=False)
        assert suspension_blocks(MODE_READ_ONLY, method="GET", query_params={"uploadId": "u1"}, has_key=True)

    def test_read_only_treats_unknown_methods_as_writes(self) -> None:
        assert suspension_blocks(MODE_READ_ONLY, method="PATCH", query_params={}, has_key=True)


class TestGetAccountSuspension:
    @pytest.mark.asyncio
    async def test_cache_miss_reads_db_and_caches_positive(self) -> None:
        redis = FakeRedis()
        pool = FakePool(mode=MODE_FULL)

        mode = await get_account_suspension(ACCOUNT, pool, redis)

        assert mode == MODE_FULL
        assert len(pool.fetchrow_calls) == 1
        assert redis.store[suspension_cache_key(ACCOUNT)] == b"full"

    @pytest.mark.asyncio
    async def test_cache_miss_caches_negative_marker(self) -> None:
        redis = FakeRedis()
        pool = FakePool(mode=None)

        mode = await get_account_suspension(ACCOUNT, pool, redis)

        assert mode is None
        assert redis.store[suspension_cache_key(ACCOUNT)] == b"__none__"

    @pytest.mark.asyncio
    async def test_cached_value_skips_db(self) -> None:
        redis = FakeRedis()
        redis.store[suspension_cache_key(ACCOUNT)] = b"read_only"
        pool = FakePool(mode=None)

        mode = await get_account_suspension(ACCOUNT, pool, redis)

        assert mode == MODE_READ_ONLY
        assert pool.fetchrow_calls == []

    @pytest.mark.asyncio
    async def test_negative_marker_skips_db(self) -> None:
        redis = FakeRedis()
        redis.store[suspension_cache_key(ACCOUNT)] = b"__none__"
        pool = FakePool(mode=MODE_FULL)

        mode = await get_account_suspension(ACCOUNT, pool, redis)

        assert mode is None
        assert pool.fetchrow_calls == []

    @pytest.mark.asyncio
    async def test_redis_error_falls_through_to_db(self) -> None:
        from redis.exceptions import RedisError

        class BrokenRedis(FakeRedis):
            async def get(self, key: str) -> bytes | None:
                raise RedisError("boom")

            async def setex(self, key: str, ttl: int, value: Any) -> None:
                raise RedisError("boom")

        pool = FakePool(mode=MODE_FULL)
        mode = await get_account_suspension(ACCOUNT, pool, BrokenRedis())

        assert mode == MODE_FULL


def _suspension_app(pool: FakePool, redis: FakeRedis, state: dict[str, Any]) -> FastAPI:
    app = FastAPI()
    app.state.postgres_pool = pool
    app.state.redis_client = redis

    # Shim standing in for auth_router: stamps whatever identity the test dictates.
    async def identity_shim(request: Any, call_next: Any) -> Any:
        for k, v in state.items():
            setattr(request.state, k, v)
        return await call_next(request)

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "healthy"}

    @app.get("/user/whatever")
    async def user_route() -> dict[str, str]:
        return {"ok": "user"}

    @app.get("/{bucket}/{key:path}")
    async def get_object(bucket: str, key: str) -> dict[str, str]:
        return {"bucket": bucket, "key": key}

    @app.put("/{bucket}/{key:path}")
    async def put_object(bucket: str, key: str) -> dict[str, str]:
        return {"bucket": bucket, "key": key}

    @app.get("/")
    async def list_buckets() -> dict[str, str]:
        return {"ok": "list"}

    # Registration order: suspension INNER to the shim (shim plays auth_router).
    app.middleware("http")(suspension_middleware)
    app.middleware("http")(identity_shim)
    return app


class TestSuspensionMiddleware:
    @pytest.mark.asyncio
    async def test_active_account_passes(self) -> None:
        app = _suspension_app(FakePool(mode=None), FakeRedis(), {"account_address": ACCOUNT})
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/bucket/key")
        assert response.status_code == 200

    @pytest.mark.asyncio
    async def test_full_suspension_blocks_reads_and_writes(self) -> None:
        app = _suspension_app(FakePool(mode=MODE_FULL), FakeRedis(), {"account_address": ACCOUNT})
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            get_response = await client.get("/bucket/key")
            put_response = await client.put("/bucket/key", content=b"data")
            list_response = await client.get("/")
        assert get_response.status_code == 403
        assert "AccessDenied" in get_response.text
        assert put_response.status_code == 403
        assert list_response.status_code == 403

    @pytest.mark.asyncio
    async def test_read_only_allows_reads_blocks_writes(self) -> None:
        app = _suspension_app(FakePool(mode=MODE_READ_ONLY), FakeRedis(), {"account_address": ACCOUNT})
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            get_response = await client.get("/bucket/key")
            list_response = await client.get("/")
            put_response = await client.put("/bucket/key", content=b"data")
        assert get_response.status_code == 200
        assert list_response.status_code == 200
        assert put_response.status_code == 403

    @pytest.mark.asyncio
    async def test_bearer_identity_uses_account_address(self) -> None:
        """Bearer requests still carry account_address even though account_id is later
        clobbered to 'anonymous' — the middleware must key on account_address."""
        app = _suspension_app(
            FakePool(mode=MODE_FULL),
            FakeRedis(),
            {"account_address": ACCOUNT, "account_id": "anonymous", "auth_method": "bearer_access_key"},
        )
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/bucket/key")
        assert response.status_code == 403

    @pytest.mark.asyncio
    async def test_anonymous_passes_primary_check(self) -> None:
        pool = FakePool(mode=MODE_FULL)
        app = _suspension_app(pool, FakeRedis(), {})
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            response = await client.get("/bucket/key")
        assert response.status_code == 200
        assert pool.fetchrow_calls == []

    @pytest.mark.asyncio
    async def test_health_and_user_paths_skip(self) -> None:
        pool = FakePool(mode=MODE_FULL)
        app = _suspension_app(pool, FakeRedis(), {"account_address": ACCOUNT})
        async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
            health_response = await client.get("/health")
            user_response = await client.get("/user/whatever")
        assert health_response.status_code == 200
        assert user_response.status_code == 200
        assert pool.fetchrow_calls == []
