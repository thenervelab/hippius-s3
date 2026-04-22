from __future__ import annotations

import json
import logging

from redis.asyncio import Redis

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


async def get_cached_sub_token_scope(
    access_key_id: str,
    repo: SubTokenScopeRepository,
    redis_client: Redis,
) -> SubTokenScope | None:
    """Return the sub-token scope, caching both hits and misses for 60s."""
    key = scope_cache_key(access_key_id)

    cached = await redis_client.get(key)
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

    scope = await repo.get(access_key_id)

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

    return scope
