import json
import logging
from typing import Any
from typing import Optional

import redis.asyncio as redis
from redis.exceptions import RedisError

from hippius_s3.models.acl import ACL
from hippius_s3.repositories.acl_repository import ACLRepository


logger = logging.getLogger(__name__)


class CachedACLRepository:
    def __init__(self, acl_repo: ACLRepository, redis_client: redis.Redis, ttl_seconds: int):
        self.acl_repo = acl_repo
        self.redis = redis_client
        self.ttl = ttl_seconds
        logger.info(f"CachedACLRepository initialized with TTL={ttl_seconds}s")

    @property
    def db(self) -> Any:
        return self.acl_repo.db

    def _bucket_acl_key(self, bucket_name: str) -> str:
        return f"hippius_acl:bucket:{bucket_name}"

    def _object_acl_key(self, bucket_name: str, object_key: str) -> str:
        return f"hippius_acl:object:{bucket_name}:{object_key}"

    async def get_bucket_acl(self, bucket_name: str) -> Optional[ACL]:
        cache_key = self._bucket_acl_key(bucket_name)

        # Best-effort cache: a redis-acl outage falls through to the DB (the ACL source of truth)
        # instead of 500-ing every permission check. Mirrors ACLService._cache_get_bucket_meta.
        try:
            cached = await self.redis.get(cache_key)
        except RedisError as exc:
            logger.warning(f"ACL cache: redis GET failed for bucket {bucket_name}, falling through to DB: {exc}")
            cached = None
        if cached:
            logger.debug(f"ACL cache HIT for bucket {bucket_name}")
            acl_data = json.loads(cached)
            return ACL.from_dict(acl_data)

        logger.debug(f"ACL cache MISS for bucket {bucket_name}")
        acl = await self.acl_repo.get_bucket_acl(bucket_name)

        if acl:
            acl_json = json.dumps(acl.to_dict())
            try:
                await self.redis.setex(cache_key, self.ttl, acl_json)
                logger.debug(f"ACL cached for bucket {bucket_name} (TTL={self.ttl}s)")
            except RedisError as exc:
                logger.warning(
                    f"ACL cache: redis SETEX failed for bucket {bucket_name} (best-effort, continuing): {exc}"
                )

        return acl

    async def get_object_acl(self, bucket_name: str, object_key: str) -> Optional[ACL]:
        cache_key = self._object_acl_key(bucket_name, object_key)

        try:
            cached = await self.redis.get(cache_key)
        except RedisError as exc:
            logger.warning(
                f"ACL cache: redis GET failed for object {bucket_name}/{object_key}, falling through to DB: {exc}"
            )
            cached = None
        if cached:
            logger.debug(f"ACL cache HIT for object {bucket_name}/{object_key}")
            acl_data = json.loads(cached)
            return ACL.from_dict(acl_data)

        logger.debug(f"ACL cache MISS for object {bucket_name}/{object_key}")
        acl = await self.acl_repo.get_object_acl(bucket_name, object_key)

        if acl:
            acl_json = json.dumps(acl.to_dict())
            try:
                await self.redis.setex(cache_key, self.ttl, acl_json)
                logger.debug(f"ACL cached for object {bucket_name}/{object_key} (TTL={self.ttl}s)")
            except RedisError as exc:
                logger.warning(
                    f"ACL cache: redis SETEX failed for object {bucket_name}/{object_key} (best-effort, continuing): {exc}"
                )

        return acl

    async def set_bucket_acl(self, bucket_name: str, owner_id: str, acl: ACL) -> None:
        await self.acl_repo.set_bucket_acl(bucket_name, owner_id, acl)
        await self.invalidate_bucket_acl(bucket_name)

    async def set_object_acl(self, bucket_name: str, object_key: str, owner_id: str, acl: ACL) -> None:
        await self.acl_repo.set_object_acl(bucket_name, object_key, owner_id, acl)
        await self.invalidate_object_acl(bucket_name, object_key)

    async def delete_bucket_acl(self, bucket_name: str) -> None:
        await self.acl_repo.delete_bucket_acl(bucket_name)
        await self.invalidate_bucket_acl(bucket_name)

    async def delete_object_acl(self, bucket_name: str, object_key: str) -> None:
        await self.acl_repo.delete_object_acl(bucket_name, object_key)
        await self.invalidate_object_acl(bucket_name, object_key)

    async def invalidate_bucket_acl(self, bucket_name: str) -> None:
        cache_key = self._bucket_acl_key(bucket_name)
        # Best-effort: the DB write already committed; a redis-acl outage must not fail the mutation.
        # SECURITY: if this DELETE fails transiently while redis is otherwise serving reads, a REVOKED
        # grant stays cached (stale ALLOW) until the entry's TTL. Bounded and self-healing — and no worse
        # than raising: the DELETE already failed, so the stale entry persists either way; swallowing just
        # spares the caller a 500. A full outage is safe regardless — reads fall through to the DB.
        try:
            deleted = await self.redis.delete(cache_key)
        except RedisError as exc:
            logger.warning(f"ACL cache: redis DELETE failed for bucket {bucket_name} (best-effort, continuing): {exc}")
            return
        if deleted:
            logger.info(f"Invalidated ACL cache for bucket {bucket_name}")

    async def invalidate_object_acl(self, bucket_name: str, object_key: str) -> None:
        cache_key = self._object_acl_key(bucket_name, object_key)
        try:
            deleted = await self.redis.delete(cache_key)
        except RedisError as exc:
            logger.warning(
                f"ACL cache: redis DELETE failed for object {bucket_name}/{object_key} (best-effort, continuing): {exc}"
            )
            return
        if deleted:
            logger.info(f"Invalidated ACL cache for object {bucket_name}/{object_key}")

    async def invalidate_all_bucket_objects(self, bucket_name: str) -> None:
        pattern = f"hippius_acl:object:{bucket_name}:*"
        keys = []
        try:
            async for key in self.redis.scan_iter(match=pattern, count=100):
                keys.append(key)  # noqa: PERF401
            if keys:
                deleted = await self.redis.delete(*keys)
                logger.info(f"Invalidated {deleted} object ACL cache entries for bucket {bucket_name}")
        except RedisError as exc:
            logger.warning(
                f"ACL cache: redis scan/delete failed for bucket {bucket_name} (best-effort, continuing): {exc}"
            )

    async def object_exists(self, bucket_name: str, object_key: str) -> bool:
        return await self.acl_repo.object_exists(bucket_name, object_key)
