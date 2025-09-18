from __future__ import annotations

from typing import Any

from hippius_s3.utils import get_query


class BucketRepository:
    def __init__(self, db: Any) -> None:
        self._db = db

    async def get_by_name(self, bucket_name: str) -> Any:
        return await self._db.fetchrow(get_query("get_bucket_by_name"), bucket_name)

    async def get_by_name_and_owner(self, bucket_name: str, main_account_id: str) -> Any:
        return await self._db.fetchrow(get_query("get_bucket_by_name_and_owner"), bucket_name, main_account_id)
