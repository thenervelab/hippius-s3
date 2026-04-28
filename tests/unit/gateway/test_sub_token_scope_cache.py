"""Unit tests for the sub-token scope Redis cache wrapper.

Critical invariant: the cache must fail-closed on storage errors. If Redis or
Postgres fails, the function returns None so the ACL middleware default-denies
rather than 500ing.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock

import asyncpg
import pytest
from redis.exceptions import RedisError

from gateway.services.sub_token_scope_cache import _NEGATIVE_MARKER
from gateway.services.sub_token_scope_cache import get_cached_sub_token_scope
from hippius_s3.models.sub_token import BucketScope
from hippius_s3.models.sub_token import Permission
from hippius_s3.models.sub_token import SubTokenScope


def _scope() -> SubTokenScope:
    return SubTokenScope(
        access_key_id="hip_test_key",
        account_id="5FHneW46xGXgs5mUiveU4sbTyGBzmstUspZC92UhjJM694ty",
        permission=Permission.object_read,
        bucket_scope=BucketScope.specific,
        bucket_ids=("11111111-1111-1111-1111-111111111111",),
    )


def _redis() -> Any:
    client = AsyncMock()
    client.get = AsyncMock(return_value=None)
    client.setex = AsyncMock()
    client.delete = AsyncMock()
    return client


def _repo(get_return: SubTokenScope | None = None, get_raises: Exception | None = None) -> Any:
    repo = AsyncMock()
    if get_raises is not None:
        repo.get = AsyncMock(side_effect=get_raises)
    else:
        repo.get = AsyncMock(return_value=get_return)
    return repo


@pytest.mark.asyncio
async def test_returns_none_when_postgres_raises_postgres_error() -> None:
    """Fail-closed: a Postgres error must surface as None, not propagate to the caller."""
    redis = _redis()
    repo = _repo(get_raises=asyncpg.PostgresError("connection terminated"))

    result = await get_cached_sub_token_scope("hip_test_key", repo, redis)

    assert result is None


@pytest.mark.asyncio
async def test_returns_none_when_postgres_raises_oserror() -> None:
    """Fail-closed: a network OSError (e.g. connection refused) must surface as None."""
    redis = _redis()
    repo = _repo(get_raises=OSError("connection refused"))

    result = await get_cached_sub_token_scope("hip_test_key", repo, redis)

    assert result is None


@pytest.mark.asyncio
async def test_postgres_error_is_not_cached_negatively() -> None:
    """A Postgres error must NOT be cached as a negative hit — that would deny
    the user for 60s every time the DB hiccups. Negative cache is only for
    confirmed 'no row exists'."""
    redis = _redis()
    repo = _repo(get_raises=asyncpg.PostgresError("transient"))

    await get_cached_sub_token_scope("hip_test_key", repo, redis)

    redis.setex.assert_not_awaited()


@pytest.mark.asyncio
async def test_redis_get_failure_falls_through_to_postgres() -> None:
    """Redis read errors are recoverable: bypass the cache, hit Postgres."""
    redis = _redis()
    redis.get = AsyncMock(side_effect=RedisError("redis is down"))
    scope = _scope()
    repo = _repo(get_return=scope)

    result = await get_cached_sub_token_scope("hip_test_key", repo, redis)

    assert result == scope


@pytest.mark.asyncio
async def test_redis_setex_failure_does_not_fail_lookup() -> None:
    """Redis write errors are best-effort: return the DB value even if cache write fails."""
    redis = _redis()
    redis.setex = AsyncMock(side_effect=RedisError("redis is down"))
    scope = _scope()
    repo = _repo(get_return=scope)

    result = await get_cached_sub_token_scope("hip_test_key", repo, redis)

    assert result == scope


@pytest.mark.asyncio
async def test_negative_cache_marker_short_circuits_db() -> None:
    """A cached __none__ marker means 'confirmed no row' — skip Postgres."""
    redis = _redis()
    redis.get = AsyncMock(return_value=_NEGATIVE_MARKER)
    repo = _repo(get_return=_scope())

    result = await get_cached_sub_token_scope("hip_test_key", repo, redis)

    assert result is None
    repo.get.assert_not_awaited()
