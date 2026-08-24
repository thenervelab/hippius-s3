from __future__ import annotations

import logging
from typing import Any

import asyncpg
from redis.asyncio import Redis
from redis.exceptions import RedisError


logger = logging.getLogger(__name__)

SUSPENSION_CACHE_PREFIX = "hippius_suspension:"
SUSPENSION_CACHE_TTL_SECONDS = 30

MODE_FULL = "full"
MODE_READ_ONLY = "read_only"

# Sentinel for "no suspension row" — keeps active accounts (the overwhelming majority)
# off the Postgres hot path. Bytes because the main redis client does not decode.
_NEGATIVE_MARKER = b"__none__"


def suspension_cache_key(account_id: str) -> str:
    return f"{SUSPENSION_CACHE_PREFIX}{account_id}"


async def get_account_suspension(
    account_id: str,
    db_pool: Any,
    redis_client: Redis,
) -> str | None:
    """Return the suspension mode ('full' | 'read_only') for an account, or None if active.

    Redis is a 30s cache in front of account_suspensions; the admin endpoints write
    through the same keys, so state changes take effect gateway-wide immediately.

    FAIL-OPEN on any backend error: this runs on the hot read path (every authenticated
    request, plus the bucket-owner check on every bucket request), and it is a BILLING
    control, not a security one. A suspended account slipping through for the seconds a
    DB/Redis blip lasts is far cheaper than 500ing all S3 traffic — and it means the
    `account_suspensions` table being briefly absent (rollout race, rollback) degrades
    enforcement rather than taking the gateway down. This is the deliberate OPPOSITE of
    sub_token_scope_cache, which fails CLOSED because scope IS a security control.
    """
    key = suspension_cache_key(account_id)

    try:
        cached = await redis_client.get(key)
    except RedisError as exc:
        logger.warning(f"suspension cache: redis GET failed, falling through to DB: {exc}")
        cached = None
    if cached is not None:
        if cached == _NEGATIVE_MARKER:
            return None
        return cached.decode("utf-8")

    try:
        row = await db_pool.fetchrow("SELECT mode FROM account_suspensions WHERE account_id = $1", account_id)
    except (asyncpg.PostgresError, OSError) as exc:
        logger.error(f"suspension lookup failed for {account_id}, failing OPEN (treated as active): {exc}")
        return None
    mode: str | None = row["mode"] if row else None

    try:
        await redis_client.setex(
            key,
            SUSPENSION_CACHE_TTL_SECONDS,
            mode.encode("utf-8") if mode else _NEGATIVE_MARKER,
        )
    except RedisError as exc:
        logger.warning(f"suspension cache: redis SETEX failed (best-effort, continuing): {exc}")

    return mode


def suspension_blocks(mode: str, *, method: str, query_params: dict, has_key: bool) -> bool:
    """Whether a suspension mode forbids this request.

    'full' blocks everything. 'read_only' blocks anything the ACL matrix classifies as
    WRITE/WRITE_ACP — which correctly catches POST ?delete, POST ?uploads, PUT ?acl etc.
    Methods outside the matrix (PATCH; PURGE probe traffic never reaches here) count as
    writes.
    """
    if mode == MODE_FULL:
        return True
    if method not in ("GET", "HEAD", "PUT", "POST", "DELETE"):
        return True
    # Lazy import: acl.py imports this module for the bucket-owner check, so a
    # top-level import here would be circular.
    from hippius_s3.gateway.middlewares.acl import get_required_permission
    from hippius_s3.models.acl import Permission

    required = get_required_permission(method=method, query_params=query_params, has_key=has_key)
    return required in (Permission.WRITE, Permission.WRITE_ACP)
