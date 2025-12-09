from datetime import datetime
from datetime import timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from hippius_s3.models.bucket import BucketDB
from hippius_s3.models.user import UserDB
from hippius_s3.repositories.bucket_repository import BucketRepository


@pytest_asyncio.fixture
async def test_engine() -> AsyncEngine:
    database_url = "postgresql+asyncpg://postgres:postgres@localhost:5432/hippius"
    engine = create_async_engine(database_url, echo=False)
    yield engine
    await engine.dispose()


@pytest_asyncio.fixture
async def db_session(test_engine: AsyncEngine) -> AsyncSession:
    async with AsyncSession(test_engine, expire_on_commit=False) as session:
        try:
            yield session
            await session.rollback()
        finally:
            await session.close()


@pytest_asyncio.fixture
async def bucket_repository(db_session: AsyncSession) -> BucketRepository:
    return BucketRepository(db_session)


@pytest_asyncio.fixture
async def test_user(db_session: AsyncSession) -> UserDB:
    user = UserDB(main_account_id="test_account_bucket_repo", created_at=datetime.now(timezone.utc))
    db_session.add(user)
    await db_session.flush()
    await db_session.refresh(user)
    return user


class TestBucketRepositoryCreate:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_create_bucket(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
        test_user: UserDB,
    ) -> None:
        bucket = BucketDB(
            bucket_id=uuid4(),
            bucket_name="test-create-bucket",
            created_at=datetime.now(timezone.utc),
            is_public=False,
            main_account_id=test_user.main_account_id,
            tags={},
        )

        created = await bucket_repository.create(bucket)
        await db_session.commit()

        assert created.bucket_id is not None
        assert created.bucket_name == "test-create-bucket"
        assert created.main_account_id == test_user.main_account_id
        assert created.is_public is False
        assert created.tags == {}

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_create_bucket_with_tags(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
        test_user: UserDB,
    ) -> None:
        bucket = BucketDB(
            bucket_id=uuid4(),
            bucket_name="test-create-bucket-tags",
            created_at=datetime.now(timezone.utc),
            is_public=False,
            main_account_id=test_user.main_account_id,
            tags={"env": "dev", "team": "backend"},
        )

        created = await bucket_repository.create(bucket)
        await db_session.commit()

        assert created.tags == {"env": "dev", "team": "backend"}


class TestBucketRepositoryGet:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_by_id(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
        test_user: UserDB,
    ) -> None:
        bucket = BucketDB(
            bucket_id=uuid4(),
            bucket_name="test-get-by-id",
            created_at=datetime.now(timezone.utc),
            is_public=False,
            main_account_id=test_user.main_account_id,
        )
        await bucket_repository.create(bucket)
        await db_session.commit()

        found = await bucket_repository.get_by_id(bucket.bucket_id)

        assert found is not None
        assert found.bucket_id == bucket.bucket_id
        assert found.bucket_name == "test-get-by-id"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_by_name(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
        test_user: UserDB,
    ) -> None:
        bucket = BucketDB(
            bucket_id=uuid4(),
            bucket_name="test-get-by-name",
            created_at=datetime.now(timezone.utc),
            is_public=False,
            main_account_id=test_user.main_account_id,
        )
        await bucket_repository.create(bucket)
        await db_session.commit()

        found = await bucket_repository.get_by_name("test-get-by-name")

        assert found is not None
        assert found.bucket_name == "test-get-by-name"
        assert found.main_account_id == test_user.main_account_id

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_by_name_and_owner(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
        test_user: UserDB,
    ) -> None:
        bucket = BucketDB(
            bucket_id=uuid4(),
            bucket_name="test-get-by-name-owner",
            created_at=datetime.now(timezone.utc),
            is_public=False,
            main_account_id=test_user.main_account_id,
        )
        await bucket_repository.create(bucket)
        await db_session.commit()

        found = await bucket_repository.get_by_name_and_owner("test-get-by-name-owner", test_user.main_account_id)

        assert found is not None
        assert found.bucket_name == "test-get-by-name-owner"
        assert found.main_account_id == test_user.main_account_id

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_by_name_and_owner_returns_none_for_wrong_owner(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
        test_user: UserDB,
    ) -> None:
        bucket = BucketDB(
            bucket_id=uuid4(),
            bucket_name="test-get-wrong-owner",
            created_at=datetime.now(timezone.utc),
            is_public=False,
            main_account_id=test_user.main_account_id,
        )
        await bucket_repository.create(bucket)
        await db_session.commit()

        found = await bucket_repository.get_by_name_and_owner("test-get-wrong-owner", "different_account")

        assert found is None


class TestBucketRepositoryList:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_list_by_owner(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
        test_user: UserDB,
    ) -> None:
        bucket1 = BucketDB(
            bucket_id=uuid4(),
            bucket_name="test-list-1",
            created_at=datetime.now(timezone.utc),
            is_public=False,
            main_account_id=test_user.main_account_id,
        )
        bucket2 = BucketDB(
            bucket_id=uuid4(),
            bucket_name="test-list-2",
            created_at=datetime.now(timezone.utc),
            is_public=False,
            main_account_id=test_user.main_account_id,
        )
        await bucket_repository.create(bucket1)
        await bucket_repository.create(bucket2)
        await db_session.commit()

        buckets = await bucket_repository.list_by_owner(test_user.main_account_id)

        assert len(buckets) >= 2
        bucket_names = [b.bucket_name for b in buckets]
        assert "test-list-1" in bucket_names
        assert "test-list-2" in bucket_names

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_list_by_owner_returns_empty_for_no_buckets(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
    ) -> None:
        buckets = await bucket_repository.list_by_owner("nonexistent_account")

        assert buckets == []


class TestBucketRepositoryTags:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_update_tags(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
        test_user: UserDB,
    ) -> None:
        bucket = BucketDB(
            bucket_id=uuid4(),
            bucket_name="test-update-tags",
            created_at=datetime.now(timezone.utc),
            is_public=False,
            main_account_id=test_user.main_account_id,
            tags={"env": "dev"},
        )
        await bucket_repository.create(bucket)
        await db_session.commit()

        updated = await bucket_repository.update_tags(bucket.bucket_id, {"env": "prod", "version": "v2"})
        await db_session.commit()

        assert updated is not None
        assert updated.tags == {"env": "prod", "version": "v2"}

        found = await bucket_repository.get_by_id(bucket.bucket_id)
        assert found is not None
        assert found.tags == {"env": "prod", "version": "v2"}

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_update_tags_to_empty(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
        test_user: UserDB,
    ) -> None:
        bucket = BucketDB(
            bucket_id=uuid4(),
            bucket_name="test-clear-tags",
            created_at=datetime.now(timezone.utc),
            is_public=False,
            main_account_id=test_user.main_account_id,
            tags={"env": "dev", "team": "backend"},
        )
        await bucket_repository.create(bucket)
        await db_session.commit()

        updated = await bucket_repository.update_tags(bucket.bucket_id, {})
        await db_session.commit()

        assert updated is not None
        assert updated.tags == {}


class TestBucketRepositoryDelete:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_delete_bucket(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
        test_user: UserDB,
    ) -> None:
        bucket = BucketDB(
            bucket_id=uuid4(),
            bucket_name="test-delete",
            created_at=datetime.now(timezone.utc),
            is_public=False,
            main_account_id=test_user.main_account_id,
        )
        await bucket_repository.create(bucket)
        await db_session.commit()

        deleted = await bucket_repository.delete(bucket.bucket_id)
        await db_session.commit()

        assert deleted is not None
        assert deleted.bucket_name == "test-delete"

        found = await bucket_repository.get_by_id(bucket.bucket_id)
        assert found is None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_delete_nonexistent_bucket(
        self,
        bucket_repository: BucketRepository,
        db_session: AsyncSession,
    ) -> None:
        fake_id = uuid4()
        deleted = await bucket_repository.delete(fake_id)

        assert deleted is None
