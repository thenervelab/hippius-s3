"""Unit tests for cache_invalidation_middleware.

The middleware purges the gateway's redis-acl cache on a successful
DeleteBucket so anonymous reads against a soft-deleted public bucket
don't keep succeeding for the cache TTL (600s). It MUST:

- Fire only on `DELETE /<bucket>` (no key, no `?tagging`) that returned 204.
- Skip every other request shape (any method other than DELETE, any
  non-204 status, any sub-path / object key, any `?tagging` query).
- Survive a Redis outage without 500'ing the response — the soft-delete
  has already committed; a cache hiccup must not turn 204 into 500.

A regression to either filter silently re-opens the authz hole, so
each branch is covered explicitly.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from fastapi import FastAPI
from fastapi import Request
from fastapi import Response
from httpx import ASGITransport
from httpx import AsyncClient

from gateway.middlewares.cache_invalidation import _bucket_from_path
from gateway.middlewares.cache_invalidation import _is_successful_bucket_delete
from gateway.middlewares.cache_invalidation import cache_invalidation_middleware
from gateway.repositories.cached_acl_repository import CachedACLRepository


class TestBucketFromPath:
    def test_simple_bucket(self) -> None:
        assert _bucket_from_path("/my-bucket") == "my-bucket"

    def test_trailing_slash(self) -> None:
        assert _bucket_from_path("/my-bucket/") == "my-bucket"

    def test_root_returns_none(self) -> None:
        assert _bucket_from_path("/") is None

    def test_empty_returns_none(self) -> None:
        assert _bucket_from_path("") is None

    def test_object_path_returns_none(self) -> None:
        # DELETE /<bucket>/<key> is DeleteObject, not DeleteBucket — skip.
        assert _bucket_from_path("/my-bucket/file.txt") is None

    def test_nested_key_returns_none(self) -> None:
        assert _bucket_from_path("/my-bucket/folder/file.txt") is None


class TestIsSuccessfulBucketDelete:
    def _make_request(self, method: str, query: dict[str, str] | None = None) -> Any:
        request = MagicMock(spec=Request)
        request.method = method
        request.query_params = query or {}
        return request

    def _make_response(self, status_code: int) -> Response:
        return Response(status_code=status_code)

    def test_204_delete_passes(self) -> None:
        assert _is_successful_bucket_delete(self._make_request("DELETE"), self._make_response(204))

    def test_get_skipped(self) -> None:
        assert not _is_successful_bucket_delete(self._make_request("GET"), self._make_response(204))

    def test_put_skipped(self) -> None:
        assert not _is_successful_bucket_delete(self._make_request("PUT"), self._make_response(204))

    def test_404_skipped(self) -> None:
        # idempotent DeleteBucket on already-gone bucket → don't re-purge cache
        assert not _is_successful_bucket_delete(self._make_request("DELETE"), self._make_response(404))

    def test_409_skipped(self) -> None:
        # BucketNotEmpty — bucket still exists, do NOT purge
        assert not _is_successful_bucket_delete(self._make_request("DELETE"), self._make_response(409))

    def test_500_skipped(self) -> None:
        assert not _is_successful_bucket_delete(self._make_request("DELETE"), self._make_response(500))

    def test_tagging_query_skipped(self) -> None:
        # DELETE /<bucket>?tagging removes only tags; bucket survives.
        assert not _is_successful_bucket_delete(
            self._make_request("DELETE", {"tagging": ""}),
            self._make_response(204),
        )


def _make_app(acl_service: Any, response: Response) -> FastAPI:
    app = FastAPI()
    app.state.acl_service = acl_service

    @app.api_route("/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"])
    async def catch_all(path: str) -> Response:  # noqa: ARG001
        return response

    app.middleware("http")(cache_invalidation_middleware)
    return app


def _make_cached_acl_repo() -> Any:
    repo = MagicMock(spec=CachedACLRepository)
    repo.invalidate_bucket_acl = AsyncMock()
    repo.invalidate_all_bucket_objects = AsyncMock()
    return repo


def _make_acl_service(repo: Any) -> Any:
    service = MagicMock()
    service.acl_repo = repo
    return service


@pytest.mark.asyncio
async def test_fires_on_delete_bucket_204() -> None:
    repo = _make_cached_acl_repo()
    app = _make_app(_make_acl_service(repo), Response(status_code=204))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.delete("/alpha")

    assert resp.status_code == 204
    repo.invalidate_bucket_acl.assert_awaited_once_with("alpha")
    repo.invalidate_all_bucket_objects.assert_awaited_once_with("alpha")


@pytest.mark.asyncio
async def test_skips_on_tagging_query() -> None:
    repo = _make_cached_acl_repo()
    app = _make_app(_make_acl_service(repo), Response(status_code=204))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.delete("/alpha?tagging")

    assert resp.status_code == 204
    repo.invalidate_bucket_acl.assert_not_awaited()
    repo.invalidate_all_bucket_objects.assert_not_awaited()


@pytest.mark.asyncio
async def test_skips_on_object_path() -> None:
    repo = _make_cached_acl_repo()
    app = _make_app(_make_acl_service(repo), Response(status_code=204))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.delete("/alpha/key.txt")

    assert resp.status_code == 204
    repo.invalidate_bucket_acl.assert_not_awaited()


@pytest.mark.asyncio
async def test_skips_on_409() -> None:
    repo = _make_cached_acl_repo()
    app = _make_app(_make_acl_service(repo), Response(status_code=409))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.delete("/alpha")

    assert resp.status_code == 409
    repo.invalidate_bucket_acl.assert_not_awaited()


@pytest.mark.asyncio
async def test_skips_on_404_idempotent() -> None:
    """A second DeleteBucket on an already-gone bucket returns 404; the first
    call already invalidated the cache — don't re-purge."""
    repo = _make_cached_acl_repo()
    app = _make_app(_make_acl_service(repo), Response(status_code=404))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.delete("/alpha")

    assert resp.status_code == 404
    repo.invalidate_bucket_acl.assert_not_awaited()


@pytest.mark.asyncio
async def test_skips_on_get() -> None:
    repo = _make_cached_acl_repo()
    app = _make_app(_make_acl_service(repo), Response(status_code=200))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.get("/alpha")

    assert resp.status_code == 200
    repo.invalidate_bucket_acl.assert_not_awaited()


@pytest.mark.asyncio
async def test_swallows_redis_exception() -> None:
    """A redis-acl outage must NOT turn a successful soft-delete into a 500.
    The upstream API already committed; the client will see 204 either way."""
    repo = MagicMock(spec=CachedACLRepository)
    repo.invalidate_bucket_acl = AsyncMock(side_effect=ConnectionError("redis-acl down"))
    repo.invalidate_all_bucket_objects = AsyncMock()
    app = _make_app(_make_acl_service(repo), Response(status_code=204))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.delete("/alpha")

    assert resp.status_code == 204
    repo.invalidate_bucket_acl.assert_awaited_once_with("alpha")


@pytest.mark.asyncio
async def test_skips_when_acl_service_missing() -> None:
    """Defensive: if app.state.acl_service is somehow not set, don't crash."""
    app = FastAPI()

    @app.api_route("/{path:path}", methods=["DELETE"])
    async def catch_all(path: str) -> Response:  # noqa: ARG001
        return Response(status_code=204)

    app.middleware("http")(cache_invalidation_middleware)

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.delete("/alpha")

    assert resp.status_code == 204


@pytest.mark.asyncio
async def test_skips_when_acl_repo_is_uncached() -> None:
    """If ACLService is configured without Redis (acl_repo is the bare
    ACLRepository, not CachedACLRepository), invalidation is a no-op."""
    bare_repo = MagicMock()  # NOT a CachedACLRepository
    app = _make_app(_make_acl_service(bare_repo), Response(status_code=204))

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        resp = await client.delete("/alpha")

    assert resp.status_code == 204
    # Bare repo has no invalidate_* methods called
    assert not hasattr(bare_repo, "invalidate_bucket_acl") or not bare_repo.invalidate_bucket_acl.called
