from __future__ import annotations

from typing import Any
from typing import Optional

from hippius_s3.db_retry import retry_on_object_version_conflict
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

    async def get_head_by_path(self, bucket_name: str, object_key: str, main_account_id: Optional[str]) -> Any:
        """HD-4: light HEAD metadata by path — no download_chunks/mpu joins; carries the Arion hash."""
        return await self._db.fetchrow(
            get_query("get_object_head_by_path"),
            bucket_name,
            object_key,
        )

    async def get_by_path(self, bucket_id: str, object_key: str) -> Any:
        return await self._db.fetchrow(get_query("get_object_by_path"), bucket_id, object_key)

    async def get_serveable_by_path(self, bucket_id: str, object_key: str) -> Any:
        """`get_by_path`, but a key whose current version is a delete marker reads as absent.

        `get_by_path` deliberately resolves TO markers — filtering them inside its version-resolution
        subquery would fall through to the previous content version and serve deleted data. Callers
        that need to distinguish "deleted" from "never existed" (CopyObject, which owes a 404 vs a
        405) read the flag themselves; callers for which a marker is simply "not found" use this.
        """
        row = await self.get_by_path(bucket_id, object_key)
        return None if row is not None and row["is_delete_marker"] else row

    async def get_by_path_and_version(self, bucket_id: str, object_key: str, version: int) -> Any:
        return await self._db.fetchrow(get_query("get_object_by_path_and_version"), bucket_id, object_key, version)

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
        upload_backends: list[str] | None = None,
    ) -> Any:
        # Version allocation can collide with a concurrent create_migration_version on
        # object_versions_pkey; retry re-reads the committed MAX in a fresh autocommit statement.
        return await retry_on_object_version_conflict(
            lambda: self._db.fetchrow(
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
                upload_backends,
            )
        )
