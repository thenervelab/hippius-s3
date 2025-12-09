from datetime import datetime
from datetime import timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from hippius_s3.models.bucket import BucketDB
from hippius_s3.models.enums import ObjectStatus
from hippius_s3.models.enums import VersionType
from hippius_s3.models.object import ObjectDB
from hippius_s3.models.object_version import ObjectVersionDB
from hippius_s3.models.user import UserDB
from hippius_s3.repositories.object_repository import ObjectRepository


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
async def object_repository(db_session: AsyncSession) -> ObjectRepository:
    return ObjectRepository(db_session)


@pytest_asyncio.fixture
async def test_user(db_session: AsyncSession) -> UserDB:
    user = UserDB(main_account_id="test_account_object_repo", created_at=datetime.now(timezone.utc))
    db_session.add(user)
    await db_session.flush()
    await db_session.refresh(user)
    return user


@pytest_asyncio.fixture
async def test_bucket(db_session: AsyncSession, test_user: UserDB) -> BucketDB:
    bucket = BucketDB(
        bucket_id=uuid4(),
        bucket_name="test-object-repo-bucket",
        created_at=datetime.now(timezone.utc),
        is_public=False,
        main_account_id=test_user.main_account_id,
        tags={},
    )
    db_session.add(bucket)
    await db_session.flush()
    await db_session.refresh(bucket)
    return bucket


class TestObjectRepositoryCreate:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_create_with_version(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/object.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=1024,
            content_type="text/plain",
            metadata={"custom-key": "custom-value"},
        )

        created_obj, created_version = await object_repository.create_with_version(obj, version)
        await db_session.commit()

        assert created_obj.object_id is not None
        assert created_obj.object_key == "test/object.txt"
        assert created_obj.current_object_version == 1

        assert created_version.object_id == created_obj.object_id
        assert created_version.object_version == 1
        assert created_version.size_bytes == 1024
        assert created_version.metadata == {"custom-key": "custom-value"}


class TestObjectRepositoryGet:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_by_id(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/get-by-id.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=512,
            content_type="text/plain",
        )
        await object_repository.create_with_version(obj, version)
        await db_session.commit()

        found = await object_repository.get_by_id(obj.object_id)

        assert found is not None
        assert found.object_id == obj.object_id
        assert found.object_key == "test/get-by-id.txt"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_by_path(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/get-by-path.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=256,
            content_type="text/plain",
        )
        await object_repository.create_with_version(obj, version)
        await db_session.commit()

        found = await object_repository.get_by_path(test_bucket.bucket_id, "test/get-by-path.txt")

        assert found is not None
        assert found.object_key == "test/get-by-path.txt"
        assert found.bucket_id == test_bucket.bucket_id

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_with_current_version(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/get-with-version.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=2048,
            content_type="application/json",
            metadata={"key": "value"},
        )
        await object_repository.create_with_version(obj, version)
        await db_session.commit()

        result = await object_repository.get_with_current_version(test_bucket.bucket_id, "test/get-with-version.txt")

        assert result is not None
        found_obj, found_version = result
        assert found_obj.object_key == "test/get-with-version.txt"
        assert found_version.object_version == 1
        assert found_version.size_bytes == 2048
        assert found_version.content_type == "application/json"
        assert found_version.metadata == {"key": "value"}


class TestObjectRepositoryVersions:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_create_version(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/multi-version.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version1 = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=100,
            content_type="text/plain",
        )
        await object_repository.create_with_version(obj, version1)
        await db_session.commit()

        version2 = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=2,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=200,
            content_type="text/plain",
        )
        created = await object_repository.create_version(version2)
        await db_session.commit()

        assert created.object_version == 2
        assert created.size_bytes == 200

        found = await object_repository.get_version(obj.object_id, 2)
        assert found is not None
        assert found.object_version == 2

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_current_version(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/current-version.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=512,
            content_type="text/plain",
        )
        await object_repository.create_with_version(obj, version)
        await db_session.commit()

        current = await object_repository.get_current_version(obj.object_id)

        assert current is not None
        assert current.object_version == 1
        assert current.size_bytes == 512

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_list_versions(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/list-versions.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version1 = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=100,
            content_type="text/plain",
        )
        await object_repository.create_with_version(obj, version1)

        version2 = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=2,
            version_type=VersionType.MIGRATION,
            storage_version=2,
            size_bytes=200,
            content_type="text/plain",
        )
        await object_repository.create_version(version2)
        await db_session.commit()

        versions = await object_repository.list_versions(obj.object_id)

        assert len(versions) == 2
        assert versions[0].object_version == 2
        assert versions[1].object_version == 1

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_next_version_number(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/next-version.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version1 = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=100,
            content_type="text/plain",
        )
        await object_repository.create_with_version(obj, version1)
        await db_session.commit()

        next_version = await object_repository.get_next_version_number(obj.object_id)

        assert next_version == 2


class TestObjectRepositoryCAS:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_swap_current_version_cas_success(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/cas-success.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version1 = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=100,
            content_type="text/plain",
        )
        version2 = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=2,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=200,
            content_type="text/plain",
        )
        await object_repository.create_with_version(obj, version1)
        await object_repository.create_version(version2)
        await db_session.commit()

        result = await object_repository.swap_current_version_cas(obj.object_id, 1, 2)
        await db_session.commit()

        assert result is not None
        assert result.current_object_version == 2

        found = await object_repository.get_by_id(obj.object_id)
        assert found is not None
        assert found.current_object_version == 2

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_swap_current_version_cas_failure(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/cas-failure.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version1 = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=100,
            content_type="text/plain",
        )
        version2 = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=2,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=200,
            content_type="text/plain",
        )
        await object_repository.create_with_version(obj, version1)
        await object_repository.create_version(version2)
        await db_session.commit()

        result = await object_repository.swap_current_version_cas(obj.object_id, 99, 2)
        await db_session.commit()

        assert result is None

        found = await object_repository.get_by_id(obj.object_id)
        assert found is not None
        assert found.current_object_version == 1


class TestObjectRepositoryMetadata:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_update_version_metadata(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/update-metadata.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=512,
            content_type="text/plain",
            metadata={"old": "value"},
        )
        await object_repository.create_with_version(obj, version)
        await db_session.commit()

        updated = await object_repository.update_version_metadata(obj.object_id, 1, {"new": "metadata"})
        await db_session.commit()

        assert updated is not None
        assert updated.metadata == {"new": "metadata"}
        assert updated.last_modified is not None

        found = await object_repository.get_version(obj.object_id, 1)
        assert found is not None
        assert found.metadata == {"new": "metadata"}


class TestObjectRepositoryDelete:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_delete_object(
        self,
        object_repository: ObjectRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/delete.txt",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=256,
            content_type="text/plain",
        )
        await object_repository.create_with_version(obj, version)
        await db_session.commit()

        deleted = await object_repository.delete(obj.object_id)
        await db_session.commit()

        assert deleted is not None
        assert deleted.object_id == obj.object_id

        found = await object_repository.get_by_id(obj.object_id)
        assert found is None

        found_version = await object_repository.get_version(obj.object_id, 1)
        assert found_version is None
