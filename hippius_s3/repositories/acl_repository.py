import json
import logging
from typing import Optional

import asyncpg

from hippius_s3.models.acl import ACL


logger = logging.getLogger(__name__)


class ACLRepository:
    def __init__(self, db_pool: asyncpg.Pool):
        self.db = db_pool

    async def get_bucket_acl(self, bucket_name: str) -> Optional[ACL]:
        query = """
        SELECT owner_id, acl_json
        FROM bucket_acls
        WHERE bucket_name = $1
        """
        row = await self.db.fetchrow(query, bucket_name)
        if not row:
            return None

        acl_data = row["acl_json"]
        if isinstance(acl_data, str):
            acl_data = json.loads(acl_data)
        return ACL.from_dict(acl_data)

    async def set_bucket_acl(self, bucket_name: str, owner_id: str, acl: ACL) -> None:
        query = """
        INSERT INTO bucket_acls (bucket_name, owner_id, acl_json)
        VALUES ($1, $2, $3::jsonb)
        ON CONFLICT (bucket_name)
        DO UPDATE SET
            owner_id = EXCLUDED.owner_id,
            acl_json = EXCLUDED.acl_json,
            updated_at = NOW()
        """
        acl_json = json.dumps(acl.to_dict())
        await self.db.execute(query, bucket_name, owner_id, acl_json)
        logger.info(f"Set ACL for bucket {bucket_name} (owner: {owner_id})")

    async def delete_bucket_acl(self, bucket_name: str) -> None:
        query = "DELETE FROM bucket_acls WHERE bucket_name = $1"
        await self.db.execute(query, bucket_name)
        logger.info(f"Deleted ACL for bucket {bucket_name}")

    async def get_object_acl(self, bucket_name: str, object_key: str) -> Optional[ACL]:
        query = """
        SELECT owner_id, acl_json
        FROM object_acls
        WHERE bucket_name = $1 AND object_key = $2
        """
        row = await self.db.fetchrow(query, bucket_name, object_key)
        if not row:
            return None

        acl_data = row["acl_json"]
        if isinstance(acl_data, str):
            acl_data = json.loads(acl_data)
        return ACL.from_dict(acl_data)

    async def set_object_acl(self, bucket_name: str, object_key: str, owner_id: str, acl: ACL) -> None:
        query = """
        INSERT INTO object_acls (bucket_name, object_key, owner_id, acl_json)
        VALUES ($1, $2, $3, $4::jsonb)
        ON CONFLICT (bucket_name, object_key)
        DO UPDATE SET
            owner_id = EXCLUDED.owner_id,
            acl_json = EXCLUDED.acl_json,
            updated_at = NOW()
        """
        acl_json = json.dumps(acl.to_dict())
        await self.db.execute(query, bucket_name, object_key, owner_id, acl_json)
        logger.info(f"Set ACL for object {bucket_name}/{object_key} (owner: {owner_id})")

    async def delete_object_acl(self, bucket_name: str, object_key: str) -> None:
        query = "DELETE FROM object_acls WHERE bucket_name = $1 AND object_key = $2"
        await self.db.execute(query, bucket_name, object_key)
        logger.info(f"Deleted ACL for object {bucket_name}/{object_key}")

    async def get_all_bucket_acls_for_owner(self, owner_id: str) -> list[tuple[str, ACL]]:
        query = """
        SELECT bucket_name, acl_json
        FROM bucket_acls
        WHERE owner_id = $1
        ORDER BY bucket_name
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
        SELECT object_key, acl_json
        FROM object_acls
        WHERE bucket_name = $1
        ORDER BY object_key
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
