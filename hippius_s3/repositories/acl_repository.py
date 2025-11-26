import json
import logging
from typing import Optional

import asyncpg

from hippius_s3.models.acl import ACL


logger = logging.getLogger(__name__)


class ACLRepository:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool

    async def _get_bucket_id(self, bucket_name: str) -> Optional[str]:
        query = "SELECT bucket_id FROM buckets WHERE bucket_name = $1"
        row = await self.db.fetchrow(query, bucket_name)
        return str(row["bucket_id"]) if row else None

    async def _get_object_id(self, bucket_name: str, object_key: str) -> Optional[str]:
        query = """
        SELECT o.object_id
        FROM objects o
        JOIN buckets b ON o.bucket_id = b.bucket_id
        WHERE b.bucket_name = $1 AND o.object_key = $2
        """
        row = await self.db.fetchrow(query, bucket_name, object_key)
        return str(row["object_id"]) if row else None

    async def get_bucket_acl_by_id(self, bucket_id: str) -> Optional[ACL]:
        query = """
        SELECT owner_id, acl_json
        FROM bucket_acls
        WHERE bucket_id = $1
        """
        row = await self.db.fetchrow(query, bucket_id)
        if not row:
            return None

        acl_data = row["acl_json"]
        if isinstance(acl_data, str):
            acl_data = json.loads(acl_data)
        return ACL.from_dict(acl_data)

    async def get_bucket_acl(self, bucket_name: str) -> Optional[ACL]:
        bucket_id = await self._get_bucket_id(bucket_name)
        if not bucket_id:
            return None
        return await self.get_bucket_acl_by_id(bucket_id)

    async def set_bucket_acl_by_id(self, bucket_id: str, owner_id: str, acl: ACL) -> None:
        query = """
        INSERT INTO bucket_acls (bucket_id, owner_id, acl_json)
        VALUES ($1, $2, $3::jsonb)
        ON CONFLICT (bucket_id)
        DO UPDATE SET
            owner_id = EXCLUDED.owner_id,
            acl_json = EXCLUDED.acl_json,
            updated_at = NOW()
        """
        acl_json = json.dumps(acl.to_dict())
        await self.db.execute(query, bucket_id, owner_id, acl_json)
        logger.info(f"Set ACL for bucket_id {bucket_id} (owner: {owner_id})")

    async def set_bucket_acl(self, bucket_name: str, owner_id: str, acl: ACL) -> None:
        bucket_id = await self._get_bucket_id(bucket_name)
        if not bucket_id:
            raise ValueError(f"Bucket not found: {bucket_name}")
        await self.set_bucket_acl_by_id(bucket_id, owner_id, acl)
        logger.info(f"Set ACL for bucket {bucket_name} (owner: {owner_id})")

    async def delete_bucket_acl_by_id(self, bucket_id: str) -> None:
        query = "DELETE FROM bucket_acls WHERE bucket_id = $1"
        await self.db.execute(query, bucket_id)
        logger.info(f"Deleted ACL for bucket_id {bucket_id}")

    async def delete_bucket_acl(self, bucket_name: str) -> None:
        bucket_id = await self._get_bucket_id(bucket_name)
        if not bucket_id:
            raise ValueError(f"Bucket not found: {bucket_name}")
        await self.delete_bucket_acl_by_id(bucket_id)
        logger.info(f"Deleted ACL for bucket {bucket_name}")

    async def get_object_acl_by_id(self, object_id: str) -> Optional[ACL]:
        query = """
        SELECT owner_id, acl_json
        FROM object_acls
        WHERE object_id = $1
        """
        row = await self.db.fetchrow(query, object_id)
        if not row:
            return None

        acl_data = row["acl_json"]
        if isinstance(acl_data, str):
            acl_data = json.loads(acl_data)
        return ACL.from_dict(acl_data)

    async def get_object_acl(self, bucket_name: str, object_key: str) -> Optional[ACL]:
        object_id = await self._get_object_id(bucket_name, object_key)
        if not object_id:
            return None
        return await self.get_object_acl_by_id(object_id)

    async def set_object_acl_by_id(self, object_id: str, owner_id: str, acl: ACL) -> None:
        query = """
        INSERT INTO object_acls (bucket_id, object_id, owner_id, acl_json)
        SELECT b.bucket_id, $1, $2, $3::jsonb
        FROM objects o
        JOIN buckets b ON o.bucket_id = b.bucket_id
        WHERE o.object_id = $1
        ON CONFLICT (bucket_id, object_id)
        DO UPDATE SET
            owner_id = EXCLUDED.owner_id,
            acl_json = EXCLUDED.acl_json,
            updated_at = NOW()
        """
        acl_json = json.dumps(acl.to_dict())
        await self.db.execute(query, object_id, owner_id, acl_json)
        logger.info(f"Set ACL for object_id {object_id} (owner: {owner_id})")

    async def set_object_acl(self, bucket_name: str, object_key: str, owner_id: str, acl: ACL) -> None:
        object_id = await self._get_object_id(bucket_name, object_key)
        if not object_id:
            raise ValueError(f"Object not found: {bucket_name}/{object_key}")
        await self.set_object_acl_by_id(object_id, owner_id, acl)
        logger.info(f"Set ACL for object {bucket_name}/{object_key} (owner: {owner_id})")

    async def delete_object_acl_by_id(self, object_id: str) -> None:
        query = "DELETE FROM object_acls WHERE object_id = $1"
        await self.db.execute(query, object_id)
        logger.info(f"Deleted ACL for object_id {object_id}")

    async def delete_object_acl(self, bucket_name: str, object_key: str) -> None:
        object_id = await self._get_object_id(bucket_name, object_key)
        if not object_id:
            raise ValueError(f"Object not found: {bucket_name}/{object_key}")
        await self.delete_object_acl_by_id(object_id)
        logger.info(f"Deleted ACL for object {bucket_name}/{object_key}")

    async def get_all_bucket_acls_for_owner(self, owner_id: str) -> list[tuple[str, ACL]]:
        query = """
        SELECT b.bucket_name, ba.acl_json
        FROM bucket_acls ba
        JOIN buckets b ON ba.bucket_id = b.bucket_id
        WHERE ba.owner_id = $1
        ORDER BY b.bucket_name
        """
        rows = await self.db.fetch(query, owner_id)
        result = []
        for row in rows:
            acl_data = row["acl_json"]
            if isinstance(acl_data, str):
                acl_data = json.loads(acl_data)
            acl = ACL.from_dict(acl_data)
            result.append((row["bucket_name"], acl))
        return result

    async def get_all_object_acls_for_bucket(self, bucket_name: str) -> list[tuple[str, ACL]]:
        query = """
        SELECT o.object_key, oa.acl_json
        FROM object_acls oa
        JOIN objects o ON oa.object_id = o.object_id
        JOIN buckets b ON oa.bucket_id = b.bucket_id
        WHERE b.bucket_name = $1
        ORDER BY o.object_key
        """
        rows = await self.db.fetch(query, bucket_name)
        result = []
        for row in rows:
            acl_data = row["acl_json"]
            if isinstance(acl_data, str):
                acl_data = json.loads(acl_data)
            acl = ACL.from_dict(acl_data)
            result.append((row["object_key"], acl))
        return result

    async def object_exists(self, bucket_name: str, object_key: str) -> bool:
        """Check if an object exists in the database."""
        query = """
        SELECT 1 FROM objects o
        JOIN buckets b ON o.bucket_id = b.bucket_id
        WHERE b.bucket_name = $1 AND o.object_key = $2
        LIMIT 1
        """
        row = await self.db.fetchrow(query, bucket_name, object_key)
        return row is not None
