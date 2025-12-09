from datetime import datetime
from datetime import timezone
from uuid import uuid4

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from hippius_s3.models.bucket import BucketDB
from hippius_s3.models.enums import VersionType
from hippius_s3.models.multipart_upload import MultipartUploadDB
from hippius_s3.models.object import ObjectDB
from hippius_s3.models.object_version import ObjectVersionDB
from hippius_s3.models.user import UserDB
from hippius_s3.repositories.multipart_upload_repository import MultipartUploadRepository


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
async def multipart_upload_repository(db_session: AsyncSession) -> MultipartUploadRepository:
    return MultipartUploadRepository(db_session)


@pytest_asyncio.fixture
async def test_user(db_session: AsyncSession) -> UserDB:
    user = UserDB(main_account_id="test_account_multipart_repo", created_at=datetime.now(timezone.utc))
    db_session.add(user)
    await db_session.flush()
    await db_session.refresh(user)
    return user


@pytest_asyncio.fixture
async def test_bucket(db_session: AsyncSession, test_user: UserDB) -> BucketDB:
    bucket = BucketDB(
        bucket_id=uuid4(),
        bucket_name="test-multipart-repo-bucket",
        created_at=datetime.now(timezone.utc),
        is_public=False,
        main_account_id=test_user.main_account_id,
        tags={},
    )
    db_session.add(bucket)
    await db_session.flush()
    await db_session.refresh(bucket)
    return bucket


class TestMultipartUploadRepositoryCreate:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_create_multipart_upload(
        self,
        multipart_upload_repository: MultipartUploadRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        upload = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/upload.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=False,
            content_type="application/octet-stream",
            metadata={"key1": "value1"},
        )

        created = await multipart_upload_repository.create(upload)
        await db_session.commit()

        assert created.upload_id is not None
        assert created.bucket_id == test_bucket.bucket_id
        assert created.object_key == "test/upload.dat"
        assert created.is_completed is False
        assert created.metadata == {"key1": "value1"}


class TestMultipartUploadRepositoryGet:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_by_id(
        self,
        multipart_upload_repository: MultipartUploadRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        upload = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/get-by-id.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=False,
        )
        await multipart_upload_repository.create(upload)
        await db_session.commit()

        found = await multipart_upload_repository.get_by_id(upload.upload_id)

        assert found is not None
        assert found.upload_id == upload.upload_id
        assert found.object_key == "test/get-by-id.dat"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_by_bucket_and_key(
        self,
        multipart_upload_repository: MultipartUploadRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        upload = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/active-upload.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=False,
        )
        await multipart_upload_repository.create(upload)
        await db_session.commit()

        found = await multipart_upload_repository.get_by_bucket_and_key(test_bucket.bucket_id, "test/active-upload.dat")

        assert found is not None
        assert found.upload_id == upload.upload_id

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_by_bucket_and_key_ignores_completed(
        self,
        multipart_upload_repository: MultipartUploadRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        upload = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/completed-upload.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=True,
        )
        await multipart_upload_repository.create(upload)
        await db_session.commit()

        found = await multipart_upload_repository.get_by_bucket_and_key(
            test_bucket.bucket_id, "test/completed-upload.dat"
        )

        assert found is None


class TestMultipartUploadRepositoryList:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_list_by_bucket(
        self,
        multipart_upload_repository: MultipartUploadRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        upload1 = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/upload1.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=False,
        )
        upload2 = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/upload2.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=False,
        )
        await multipart_upload_repository.create(upload1)
        await multipart_upload_repository.create(upload2)
        await db_session.commit()

        uploads = await multipart_upload_repository.list_by_bucket(test_bucket.bucket_id)

        assert len(uploads) >= 2
        upload_keys = [u.object_key for u in uploads]
        assert "test/upload1.dat" in upload_keys
        assert "test/upload2.dat" in upload_keys

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_list_by_bucket_excludes_completed_by_default(
        self,
        multipart_upload_repository: MultipartUploadRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        active = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/active.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=False,
        )
        completed = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/completed.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=True,
        )
        await multipart_upload_repository.create(active)
        await multipart_upload_repository.create(completed)
        await db_session.commit()

        uploads = await multipart_upload_repository.list_by_bucket(test_bucket.bucket_id, include_completed=False)

        upload_keys = [u.object_key for u in uploads]
        assert "test/active.dat" in upload_keys
        assert "test/completed.dat" not in upload_keys

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_list_by_bucket_includes_completed_when_requested(
        self,
        multipart_upload_repository: MultipartUploadRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        active = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/active2.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=False,
        )
        completed = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/completed2.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=True,
        )
        await multipart_upload_repository.create(active)
        await multipart_upload_repository.create(completed)
        await db_session.commit()

        uploads = await multipart_upload_repository.list_by_bucket(test_bucket.bucket_id, include_completed=True)

        upload_keys = [u.object_key for u in uploads]
        assert "test/active2.dat" in upload_keys
        assert "test/completed2.dat" in upload_keys


class TestMultipartUploadRepositoryComplete:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_mark_completed(
        self,
        multipart_upload_repository: MultipartUploadRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        obj = ObjectDB(
            object_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/final-object.dat",
            created_at=datetime.now(timezone.utc),
            current_object_version=1,
        )
        version = ObjectVersionDB(
            object_id=obj.object_id,
            object_version=1,
            version_type=VersionType.USER,
            storage_version=1,
            size_bytes=10485760,
            content_type="application/octet-stream",
        )
        db_session.add(obj)
        db_session.add(version)

        upload = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/final-object.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=False,
        )
        await multipart_upload_repository.create(upload)
        await db_session.commit()

        updated = await multipart_upload_repository.mark_completed(upload.upload_id, obj.object_id)
        await db_session.commit()

        assert updated is not None
        assert updated.is_completed is True
        assert updated.object_id == obj.object_id

        found = await multipart_upload_repository.get_by_id(upload.upload_id)
        assert found is not None
        assert found.is_completed is True


class TestMultipartUploadRepositoryDelete:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_delete_multipart_upload(
        self,
        multipart_upload_repository: MultipartUploadRepository,
        db_session: AsyncSession,
        test_bucket: BucketDB,
    ) -> None:
        upload = MultipartUploadDB(
            upload_id=uuid4(),
            bucket_id=test_bucket.bucket_id,
            object_key="test/delete.dat",
            initiated_at=datetime.now(timezone.utc),
            is_completed=False,
        )
        await multipart_upload_repository.create(upload)
        await db_session.commit()

        deleted = await multipart_upload_repository.delete(upload.upload_id)
        await db_session.commit()

        assert deleted is not None
        assert deleted.upload_id == upload.upload_id

        found = await multipart_upload_repository.get_by_id(upload.upload_id)
        assert found is None
