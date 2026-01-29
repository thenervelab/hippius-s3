from __future__ import annotations

from typing import Any
from typing import Optional

from hippius_s3.utils import get_query


class ObjectRepository:
    def __init__(self, db: Any) -> None:
        self._db = db

    async def get_for_download_with_permissions(
        self, bucket_name: str, object_key: str, main_account_id: Optional[str]
    ) -> Any:
        return await self._db.fetchrow(
            get_query("get_object_for_download_with_permissions"),
            bucket_name,
            object_key,
        )

    async def get_for_download_with_permissions_by_version(
        self, bucket_name: str, object_key: str, version: int, main_account_id: Optional[str]
    ) -> Any:
        return await self._db.fetchrow(
            get_query("get_object_for_download_with_permissions_by_version"),
            bucket_name,
            object_key,
            version,
        )

    async def get_by_path(self, bucket_id: str, object_key: str) -> Any:
        return await self._db.fetchrow(get_query("get_object_by_path"), bucket_id, object_key)

    async def upsert_with_cid(
        self,
        object_id: str,
        bucket_id: str,
        object_key: str,
        cid_id: str,
        size_bytes: int,
        content_type: str,
        created_at: Any,
        metadata_json: str,
        md5_hash: str,
        *,
        storage_version: int,
    ) -> Any:
        return await self._db.fetchrow(
            get_query("upsert_object_with_cid"),
            object_id,
            bucket_id,
            object_key,
            cid_id,
            size_bytes,
            content_type,
            created_at,
            metadata_json,
            md5_hash,
            storage_version,
        )
