import json
import os

import pytest
import pytest_asyncio
import redis.asyncio as redis

from gateway.repositories.cached_acl_repository import CachedACLRepository
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission
from hippius_s3.repositories.acl_repository import ACLRepository


@pytest_asyncio.fixture
async def redis_client() -> redis.Redis:
    redis_url = os.getenv("REDIS_ACL_URL", "redis://localhost:6384/0")
    client = redis.from_url(redis_url, decode_responses=True)
    await client.flushdb()
    yield client
    await client.flushdb()
    await client.close()


@pytest_asyncio.fixture
async def cached_acl_repo(gateway_db_pool, redis_client) -> CachedACLRepository:
    base_repo = ACLRepository(gateway_db_pool)
    return CachedACLRepository(base_repo, redis_client, ttl_seconds=600)


@pytest.fixture
def sample_acl() -> ACL:
    owner = Owner(id="account123", display_name="Test User")
    grants = [
        Grant(
            grantee=Grantee(type=GranteeType.CANONICAL_USER, id="account123"),
            permission=Permission.FULL_CONTROL,
        )
    ]
    return ACL(owner=owner, grants=grants)


class TestACLCacheFlow:
    @pytest.mark.asyncio
    async def test_bucket_acl_caching_flow(
        self, cached_acl_repo: CachedACLRepository, redis_client: redis.Redis, sample_acl: ACL, gateway_db_pool
    ) -> None:
        bucket_name = "test-cache-bucket"

        await gateway_db_pool.execute(
            "INSERT INTO buckets (bucket_name, main_account_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            bucket_name,
            "account123",
        )

        cache_key = f"hippius_acl:bucket:{bucket_name}"
        cached_before = await redis_client.get(cache_key)
        assert cached_before is None

        await cached_acl_repo.set_bucket_acl(bucket_name, "account123", sample_acl)

        cached_after_write = await redis_client.get(cache_key)
        assert cached_after_write is None

        acl = await cached_acl_repo.get_bucket_acl(bucket_name)
        assert acl is not None
        assert acl.owner.id == "account123"

        cached_after_read = await redis_client.get(cache_key)
        assert cached_after_read is not None
        acl_data = json.loads(cached_after_read)
        assert acl_data["owner"]["id"] == "account123"

        acl_second_read = await cached_acl_repo.get_bucket_acl(bucket_name)
        assert acl_second_read is not None
        assert acl_second_read.owner.id == "account123"

        await cached_acl_repo.invalidate_bucket_acl(bucket_name)

        cached_after_invalidation = await redis_client.get(cache_key)
        assert cached_after_invalidation is None

    @pytest.mark.asyncio
    async def test_object_acl_caching_flow(
        self,
        cached_acl_repo: CachedACLRepository,
        redis_client: redis.Redis,
        sample_acl: ACL,
        gateway_db_pool,
    ) -> None:
        bucket_name = "test-cache-bucket"
        object_key = "test-object.txt"

        await gateway_db_pool.execute(
            "INSERT INTO buckets (bucket_name, main_account_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            bucket_name,
            "account123",
        )

        bucket_id = await gateway_db_pool.fetchval("SELECT bucket_id FROM buckets WHERE bucket_name = $1", bucket_name)

        await gateway_db_pool.execute(
            "INSERT INTO objects (bucket_id, object_key, size_bytes, etag) VALUES ($1, $2, 0, 'test') ON CONFLICT DO NOTHING",
            bucket_id,
            object_key,
        )

        cache_key = f"hippius_acl:object:{bucket_name}:{object_key}"
        cached_before = await redis_client.get(cache_key)
        assert cached_before is None

        await cached_acl_repo.set_object_acl(bucket_name, object_key, "account123", sample_acl)

        cached_after_write = await redis_client.get(cache_key)
        assert cached_after_write is None

        acl = await cached_acl_repo.get_object_acl(bucket_name, object_key)
        assert acl is not None
        assert acl.owner.id == "account123"

        cached_after_read = await redis_client.get(cache_key)
        assert cached_after_read is not None

        acl_second_read = await cached_acl_repo.get_object_acl(bucket_name, object_key)
        assert acl_second_read is not None
        assert acl_second_read.owner.id == "account123"

        await cached_acl_repo.invalidate_object_acl(bucket_name, object_key)

        cached_after_invalidation = await redis_client.get(cache_key)
        assert cached_after_invalidation is None

    @pytest.mark.asyncio
    async def test_invalidate_all_bucket_objects(
        self,
        cached_acl_repo: CachedACLRepository,
        redis_client: redis.Redis,
        sample_acl: ACL,
    ) -> None:
        bucket_name = "test-cache-bucket"
        object_key_1 = "obj1.txt"
        object_key_2 = "obj2.txt"

        await redis_client.set(f"hippius_acl:object:{bucket_name}:{object_key_1}", json.dumps(sample_acl.to_dict()))
        await redis_client.set(f"hippius_acl:object:{bucket_name}:{object_key_2}", json.dumps(sample_acl.to_dict()))

        keys_before = []
        async for key in redis_client.scan_iter(match=f"hippius_acl:object:{bucket_name}:*"):
            keys_before.append(key)
        assert len(keys_before) == 2

        await cached_acl_repo.invalidate_all_bucket_objects(bucket_name)

        keys_after = []
        async for key in redis_client.scan_iter(match=f"hippius_acl:object:{bucket_name}:*"):
            keys_after.append(key)
        assert len(keys_after) == 0

    @pytest.mark.asyncio
    async def test_cache_ttl_set_correctly(
        self, cached_acl_repo: CachedACLRepository, redis_client: redis.Redis, sample_acl: ACL, gateway_db_pool
    ) -> None:
        bucket_name = "test-cache-bucket"

        await gateway_db_pool.execute(
            "INSERT INTO buckets (bucket_name, main_account_id) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            bucket_name,
            "account123",
        )

        await cached_acl_repo.set_bucket_acl(bucket_name, "account123", sample_acl)

        await cached_acl_repo.get_bucket_acl(bucket_name)

        cache_key = f"hippius_acl:bucket:{bucket_name}"
        ttl = await redis_client.ttl(cache_key)

        assert ttl > 0
        assert ttl <= 600
