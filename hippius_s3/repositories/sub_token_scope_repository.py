from __future__ import annotations

import logging

import asyncpg

from hippius_s3.models.sub_token import BucketScope
from hippius_s3.models.sub_token import Permission
from hippius_s3.models.sub_token import SubTokenScope
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


class SubTokenScopeRepository:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool

    async def get(self, access_key_id: str) -> SubTokenScope | None:
        row = await self.db.fetchrow(get_query("get_sub_token_scope"), access_key_id)
        if row is None:
            return None
        return _row_to_scope(row)

    async def upsert(
        self,
        *,
        access_key_id: str,
        account_id: str,
        permission: Permission,
        bucket_scope: BucketScope,
        bucket_ids: list[str],
    ) -> SubTokenScope:
        if bucket_scope is BucketScope.specific and not bucket_ids:
            raise ValueError("bucket_scope='specific' requires non-empty bucket_ids")
        row = await self.db.fetchrow(
            get_query("upsert_sub_token_scope"),
            access_key_id,
            account_id,
            permission.value,
            bucket_scope.value,
            bucket_ids,
        )
        # asyncpg INSERT ... RETURNING always yields a row; no None guard needed.
        assert row is not None
        logger.info(
            f"Upserted sub_token_scope: access_key={access_key_id[:8]}***, "
            f"permission={permission.value}, bucket_scope={bucket_scope.value}, buckets={len(bucket_ids)}"
        )
        return _row_to_scope(row)

    async def delete(self, access_key_id: str) -> bool:
        result = await self.db.execute(get_query("delete_sub_token_scope"), access_key_id)
        deleted = result == "DELETE 1"
        logger.info(f"Deleted sub_token_scope: access_key={access_key_id[:8]}*** deleted={deleted}")
        return deleted


def _row_to_scope(row: asyncpg.Record) -> SubTokenScope:
    return SubTokenScope(
        access_key_id=row["access_key_id"],
        account_id=row["account_id"],
        permission=Permission(row["permission"]),
        bucket_scope=BucketScope(row["bucket_scope"]),
        bucket_ids=tuple(str(b) for b in (row["bucket_ids"] or [])),
    )
