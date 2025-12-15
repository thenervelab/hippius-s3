import json
from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
import pytest_asyncio

from gateway.repositories.cached_acl_repository import CachedACLRepository
from hippius_s3.models.acl import ACL
from hippius_s3.models.acl import Grant
from hippius_s3.models.acl import Grantee
from hippius_s3.models.acl import GranteeType
from hippius_s3.models.acl import Owner
from hippius_s3.models.acl import Permission
from hippius_s3.repositories.acl_repository import ACLRepository


@pytest_asyncio.fixture
async def redis_client() -> Any:
    mock_redis = MagicMock()
    mock_redis.get = AsyncMock(return_value=None)
    mock_redis.set = AsyncMock()
    mock_redis.setex = AsyncMock()
    mock_redis.delete = AsyncMock(return_value=1)
    mock_redis.scan_iter = MagicMock()
    mock_redis.ttl = AsyncMock(return_value=600)
    yield mock_redis


@pytest_asyncio.fixture
async def cached_acl_repo(gateway_db_pool: Any, redis_client: Any) -> CachedACLRepository:
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
        self, cached_acl_repo: CachedACLRepository, redis_client: Any, sample_acl: ACL, gateway_db_pool: Any
    ) -> None:
        bucket_name = "test-cache-bucket"
        cache_key = f"hippius_acl:bucket:{bucket_name}"

        gateway_db_pool.fetchrow.return_value = {
            "bucket_id": "test-bucket-id",
            "owner_id": "account123",
            "acl_json": json.dumps(sample_acl.to_dict()),
        }
        gateway_db_pool.fetchval.return_value = "test-bucket-id"

        redis_client.get.return_value = None
        await cached_acl_repo.set_bucket_acl(bucket_name, "account123", sample_acl)
        gateway_db_pool.execute.assert_called()
        redis_client.delete.assert_called_with(cache_key)

        redis_client.reset_mock()
        redis_client.get.return_value = None
        gateway_db_pool.fetchrow.return_value = {"owner_id": "account123", "acl_json": json.dumps(sample_acl.to_dict())}
        acl = await cached_acl_repo.get_bucket_acl(bucket_name)
        assert acl is not None
        assert acl.owner.id == "account123"
        redis_client.setex.assert_called()
        setex_args = redis_client.setex.call_args[0]
        assert setex_args[0] == cache_key
        assert setex_args[1] == 600

        redis_client.get.return_value = json.dumps(sample_acl.to_dict())
        acl_second_read = await cached_acl_repo.get_bucket_acl(bucket_name)
        assert acl_second_read is not None
        assert acl_second_read.owner.id == "account123"

        redis_client.delete.reset_mock()
        await cached_acl_repo.invalidate_bucket_acl(bucket_name)
        redis_client.delete.assert_called_once_with(cache_key)

    @pytest.mark.asyncio
    async def test_object_acl_caching_flow(
        self,
        cached_acl_repo: CachedACLRepository,
        redis_client: Any,
        sample_acl: ACL,
        gateway_db_pool: Any,
    ) -> None:
        bucket_name = "test-cache-bucket"
        object_key = "test-object.txt"
        cache_key = f"hippius_acl:object:{bucket_name}:{object_key}"

        gateway_db_pool.fetchrow.return_value = {
            "bucket_id": "test-bucket-id",
            "object_id": "test-object-id",
            "owner_id": "account123",
            "acl_json": json.dumps(sample_acl.to_dict()),
        }
        gateway_db_pool.fetchval.return_value = "test-object-id"

        redis_client.get.return_value = None
        await cached_acl_repo.set_object_acl(bucket_name, object_key, "account123", sample_acl)
        gateway_db_pool.execute.assert_called()
        redis_client.delete.assert_called_with(cache_key)

        redis_client.reset_mock()
        redis_client.get.return_value = None
        gateway_db_pool.fetchrow.return_value = {"owner_id": "account123", "acl_json": json.dumps(sample_acl.to_dict())}
        acl = await cached_acl_repo.get_object_acl(bucket_name, object_key)
        assert acl is not None
        assert acl.owner.id == "account123"
        redis_client.setex.assert_called()

        redis_client.get.return_value = json.dumps(sample_acl.to_dict())
        acl_second_read = await cached_acl_repo.get_object_acl(bucket_name, object_key)
        assert acl_second_read is not None
        assert acl_second_read.owner.id == "account123"

        redis_client.delete.reset_mock()
        await cached_acl_repo.invalidate_object_acl(bucket_name, object_key)
        redis_client.delete.assert_called_once_with(cache_key)

    @pytest.mark.asyncio
    async def test_invalidate_all_bucket_objects(
        self,
        cached_acl_repo: CachedACLRepository,
        redis_client: Any,
        sample_acl: ACL,
    ) -> None:
        bucket_name = "test-cache-bucket"
        object_key_1 = "obj1.txt"
        object_key_2 = "obj2.txt"

        class AsyncIterator:
            def __init__(self, items: list[str]):
                self.items = items
                self.index = 0

            def __aiter__(self) -> "AsyncIterator":
                return self

            async def __anext__(self) -> str:
                if self.index >= len(self.items):
                    raise StopAsyncIteration
                item = self.items[self.index]
                self.index += 1
                return item

        redis_client.scan_iter.return_value = AsyncIterator(
            [f"hippius_acl:object:{bucket_name}:{object_key_1}", f"hippius_acl:object:{bucket_name}:{object_key_2}"]
        )
        redis_client.delete.return_value = 2

        await cached_acl_repo.invalidate_all_bucket_objects(bucket_name)

        redis_client.scan_iter.assert_called_once()
        redis_client.delete.assert_called_once()
        delete_args = redis_client.delete.call_args[0]
        assert len(delete_args) == 2

    @pytest.mark.asyncio
    async def test_cache_ttl_set_correctly(
        self, cached_acl_repo: CachedACLRepository, redis_client: Any, sample_acl: ACL, gateway_db_pool: Any
    ) -> None:
        bucket_name = "test-cache-bucket"
        cache_key = f"hippius_acl:bucket:{bucket_name}"

        gateway_db_pool.fetchrow.return_value = {"owner_id": "account123", "acl_json": json.dumps(sample_acl.to_dict())}
        redis_client.get.return_value = None

        await cached_acl_repo.get_bucket_acl(bucket_name)

        redis_client.setex.assert_called_once()
        setex_args = redis_client.setex.call_args[0]
        assert setex_args[0] == cache_key
        assert setex_args[1] == 600
