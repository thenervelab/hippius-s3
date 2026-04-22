from __future__ import annotations

import json
import logging

from redis.asyncio import Redis

from hippius_s3.repositories.sub_token_scope_repository import SCOPE_CACHE_TTL_SECONDS
from hippius_s3.repositories.sub_token_scope_repository import SubTokenScope
from hippius_s3.repositories.sub_token_scope_repository import SubTokenScopeRepository
from hippius_s3.repositories.sub_token_scope_repository import scope_cache_key


logger = logging.getLogger(__name__)

# Sentinel used to represent "no scope row in DB" — kept in Redis so repeated
# default-deny decisions don't hammer Postgres for unscoped sub-tokens. The
# shared redis_client returns bytes (no decode_responses), so store/compare bytes.
_NEGATIVE_MARKER = b"__none__"


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
            permission=payload["permission"],
            bucket_scope=payload["bucket_scope"],
            bucket_ids=tuple(payload.get("bucket_ids", [])),
        )

    scope = await repo.get(access_key_id)

    if scope is None:
        await redis_client.setex(key, SCOPE_CACHE_TTL_SECONDS, _NEGATIVE_MARKER)
    else:
        payload = {
            "access_key_id": scope.access_key_id,
            "account_id": scope.account_id,
            "permission": scope.permission,
            "bucket_scope": scope.bucket_scope,
            "bucket_ids": list(scope.bucket_ids),
        }
        await redis_client.setex(key, SCOPE_CACHE_TTL_SECONDS, json.dumps(payload))

    return scope
