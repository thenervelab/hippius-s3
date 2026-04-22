from __future__ import annotations

import logging
from dataclasses import dataclass
from dataclasses import field

import asyncpg

from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)

PERMISSION_VALUES = ("admin_read_write", "admin_read", "object_read_write", "object_read")
BUCKET_SCOPE_VALUES = ("all", "specific")

# Redis cache key prefix shared by gateway (reads) and API (write-side invalidation).
SCOPE_CACHE_PREFIX = "hippius_subscope:"
SCOPE_CACHE_TTL_SECONDS = 60


def scope_cache_key(access_key_id: str) -> str:
    return f"{SCOPE_CACHE_PREFIX}{access_key_id}"


@dataclass(frozen=True)
class SubTokenScope:
    access_key_id: str
    account_id: str
    permission: str
    bucket_scope: str
    bucket_ids: tuple[str, ...] = field(default_factory=tuple)


class SubTokenScopeRepository:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool

    async def get(self, access_key_id: str) -> SubTokenScope | None:
        row = await self.db.fetchrow(get_query("get_sub_token_scope"), access_key_id)
        if row is None:
            return None
        return SubTokenScope(
            access_key_id=row["access_key_id"],
            account_id=row["account_id"],
            permission=row["permission"],
            bucket_scope=row["bucket_scope"],
            bucket_ids=tuple(str(b) for b in (row["bucket_ids"] or [])),
        )

    async def upsert(
        self,
        *,
        access_key_id: str,
        account_id: str,
        permission: str,
        bucket_scope: str,
        bucket_ids: list[str],
    ) -> SubTokenScope:
        if permission not in PERMISSION_VALUES:
            raise ValueError(f"Invalid permission: {permission}")
        if bucket_scope not in BUCKET_SCOPE_VALUES:
            raise ValueError(f"Invalid bucket_scope: {bucket_scope}")
        if bucket_scope == "specific" and not bucket_ids:
            raise ValueError("bucket_scope='specific' requires non-empty bucket_ids")
        row = await self.db.fetchrow(
            get_query("upsert_sub_token_scope"),
            access_key_id,
            account_id,
            permission,
            bucket_scope,
            bucket_ids,
        )
        if row is None:
            # asyncpg's INSERT ... RETURNING always yields a row; this guards against
            # a schema-level regression rather than a realistic runtime condition.
            raise RuntimeError("upsert_sub_token_scope did not return a row")
        logger.info(
            f"Upserted sub_token_scope: access_key={access_key_id[:8]}***, "
            f"permission={permission}, bucket_scope={bucket_scope}, buckets={len(bucket_ids)}"
        )
        return SubTokenScope(
            access_key_id=row["access_key_id"],
            account_id=row["account_id"],
            permission=row["permission"],
            bucket_scope=row["bucket_scope"],
            bucket_ids=tuple(str(b) for b in (row["bucket_ids"] or [])),
        )

    async def delete(self, access_key_id: str) -> bool:
        result = await self.db.execute(get_query("delete_sub_token_scope"), access_key_id)
        deleted = result == "DELETE 1"
        logger.info(f"Deleted sub_token_scope: access_key={access_key_id[:8]}*** deleted={deleted}")
        return deleted
