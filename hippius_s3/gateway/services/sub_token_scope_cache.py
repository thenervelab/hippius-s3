from __future__ import annotations

import json
import logging
from enum import Enum

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import RedisError

from hippius_s3.models.sub_token import BucketScope
from hippius_s3.models.sub_token import Permission
from hippius_s3.models.sub_token import SubTokenScope
from hippius_s3.repositories.sub_token_scope_repository import SubTokenScopeRepository


logger = logging.getLogger(__name__)

SCOPE_CACHE_PREFIX = "hippius_subscope:"
SCOPE_CACHE_TTL_SECONDS = 60

# Sentinel stored in Redis to mean "no scope row in DB" — keeps repeated
# default-deny decisions off the Postgres hot path. redis_client returns bytes
# (no decode_responses), so we compare bytes directly.
_NEGATIVE_MARKER = b"__none__"


def scope_cache_key(access_key_id: str) -> str:
    return f"{SCOPE_CACHE_PREFIX}{access_key_id}"


class ScopeUnavailable(Enum):
    """The scope could not be read. Distinct from "no scope row", which is a real answer."""

    UNAVAILABLE = "unavailable"


SCOPE_UNAVAILABLE = ScopeUnavailable.UNAVAILABLE


async def get_cached_sub_token_scope(
    access_key_id: str,
    repo: SubTokenScopeRepository,
    redis_client: Redis,
) -> SubTokenScope | None:
    """Return the sub-token scope, caching both hits and misses for 60s.

    Fail-closed: if either Redis or Postgres errors, return None so the caller
    default-denies. The alternative — propagating the exception — surfaces as a
    500 to the S3 client and looks like a write/read outage rather than an auth
    decision; default-deny converts the storage error into the safer 403.

    None is only fail-closed where None means "deny". A caller for which a missing scope row
    means something permissive must use lookup_sub_token_scope and treat SCOPE_UNAVAILABLE as a
    denial of its own.
    """
    scope = await lookup_sub_token_scope(access_key_id, repo, redis_client)
    return None if scope is SCOPE_UNAVAILABLE else scope


async def lookup_sub_token_scope(
    access_key_id: str,
    repo: SubTokenScopeRepository,
    redis_client: Redis,
) -> SubTokenScope | None | ScopeUnavailable:
    """As get_cached_sub_token_scope, but a failed Postgres read returns SCOPE_UNAVAILABLE."""
    key = scope_cache_key(access_key_id)

    try:
        cached = await redis_client.get(key)
    except RedisError as exc:
        logger.warning(f"scope cache: redis GET failed, falling through to DB: {exc}")
        cached = None
    if cached is not None:
        if cached == _NEGATIVE_MARKER:
            return None
        payload = json.loads(cached)
        return SubTokenScope(
            access_key_id=payload["access_key_id"],
            account_id=payload["account_id"],
            permission=Permission(payload["permission"]),
            bucket_scope=BucketScope(payload["bucket_scope"]),
            bucket_ids=tuple(payload.get("bucket_ids", [])),
        )

    try:
        scope = await repo.get(access_key_id)
    except (asyncpg.PostgresError, OSError) as exc:
        logger.error(f"scope cache: postgres lookup failed for {access_key_id[:8]}***, default-denying: {exc}")
        return SCOPE_UNAVAILABLE

    try:
        if scope is None:
            await redis_client.setex(key, SCOPE_CACHE_TTL_SECONDS, _NEGATIVE_MARKER)
        else:
            payload = {
                "access_key_id": scope.access_key_id,
                "account_id": scope.account_id,
                "permission": scope.permission.value,
                "bucket_scope": scope.bucket_scope.value,
                "bucket_ids": list(scope.bucket_ids),
            }
            await redis_client.setex(key, SCOPE_CACHE_TTL_SECONDS, json.dumps(payload))
    except RedisError as exc:
        logger.warning(f"scope cache: redis SETEX failed (best-effort, continuing): {exc}")

    return scope
