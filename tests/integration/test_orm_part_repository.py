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
from hippius_s3.models.multipart_upload import MultipartUploadDB
from hippius_s3.models.object import ObjectDB
from hippius_s3.models.object_version import ObjectVersionDB
from hippius_s3.models.part import PartDB
from hippius_s3.models.part_chunk import PartChunkDB
from hippius_s3.models.user import UserDB
from hippius_s3.repositories.part_repository import PartRepository


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
async def part_repository(db_session: AsyncSession) -> PartRepository:
    return PartRepository(db_session)


@pytest_asyncio.fixture
async def test_user(db_session: AsyncSession) -> UserDB:
    user = UserDB(main_account_id="test_account_part_repo", created_at=datetime.now(timezone.utc))
    db_session.add(user)
    await db_session.flush()
    await db_session.refresh(user)
    return user


@pytest_asyncio.fixture
async def test_bucket(db_session: AsyncSession, test_user: UserDB) -> BucketDB:
    bucket = BucketDB(
        bucket_id=uuid4(),
        bucket_name="test-part-repo-bucket",
        created_at=datetime.now(timezone.utc),
        is_public=False,
        main_account_id=test_user.main_account_id,
        tags={},
    )
    db_session.add(bucket)
    await db_session.flush()
    await db_session.refresh(bucket)
    return bucket


@pytest_asyncio.fixture
async def test_object_with_version(db_session: AsyncSession, test_bucket: BucketDB) -> tuple[ObjectDB, ObjectVersionDB]:
    obj = ObjectDB(
        object_id=uuid4(),
        bucket_id=test_bucket.bucket_id,
        object_key="test/multipart.dat",
        created_at=datetime.now(timezone.utc),
        current_object_version=1,
    )
    version = ObjectVersionDB(
        object_id=obj.object_id,
        object_version=1,
        version_type=VersionType.USER,
        storage_version=1,
        size_bytes=0,
        content_type="application/octet-stream",
    )
    db_session.add(obj)
    db_session.add(version)
    await db_session.flush()
    await db_session.refresh(obj)
    await db_session.refresh(version)
    return obj, version


@pytest_asyncio.fixture
async def test_multipart_upload(db_session: AsyncSession, test_bucket: BucketDB) -> MultipartUploadDB:
    upload = MultipartUploadDB(
        upload_id=uuid4(),
        bucket_id=test_bucket.bucket_id,
        object_key="test/upload.dat",
        initiated_at=datetime.now(timezone.utc),
        is_completed=False,
        content_type="application/octet-stream",
    )
    db_session.add(upload)
    await db_session.flush()
    await db_session.refresh(upload)
    return upload


class TestPartRepositoryCreate:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_create_part(
        self,
        part_repository: PartRepository,
        db_session: AsyncSession,
        test_multipart_upload: MultipartUploadDB,
    ) -> None:
        part = PartDB(
            part_id=uuid4(),
            upload_id=test_multipart_upload.upload_id,
            part_number=1,
            size_bytes=1024,
            etag="test-etag-1",
            uploaded_at=datetime.now(timezone.utc),
        )

        created = await part_repository.create(part)
        await db_session.commit()

        assert created.part_id is not None
        assert created.upload_id == test_multipart_upload.upload_id
        assert created.part_number == 1
        assert created.size_bytes == 1024

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_create_part_with_object_version(
        self,
        part_repository: PartRepository,
        db_session: AsyncSession,
        test_object_with_version: tuple[ObjectDB, ObjectVersionDB],
    ) -> None:
        obj, version = test_object_with_version
        part = PartDB(
            part_id=uuid4(),
            object_id=obj.object_id,
            object_version=version.object_version,
            part_number=0,
            size_bytes=2048,
            etag="test-etag-0",
            uploaded_at=datetime.now(timezone.utc),
        )

        created = await part_repository.create(part)
        await db_session.commit()

        assert created.object_id == obj.object_id
        assert created.object_version == version.object_version
        assert created.part_number == 0


class TestPartRepositoryGet:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_by_id(
        self,
        part_repository: PartRepository,
        db_session: AsyncSession,
        test_multipart_upload: MultipartUploadDB,
    ) -> None:
        part = PartDB(
            part_id=uuid4(),
            upload_id=test_multipart_upload.upload_id,
            part_number=1,
            size_bytes=512,
            etag="get-by-id-etag",
            uploaded_at=datetime.now(timezone.utc),
        )
        await part_repository.create(part)
        await db_session.commit()

        found = await part_repository.get_by_id(part.part_id)

        assert found is not None
        assert found.part_id == part.part_id
        assert found.part_number == 1

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_list_by_upload(
        self,
        part_repository: PartRepository,
        db_session: AsyncSession,
        test_multipart_upload: MultipartUploadDB,
    ) -> None:
        part1 = PartDB(
            part_id=uuid4(),
            upload_id=test_multipart_upload.upload_id,
            part_number=1,
            size_bytes=1024,
            etag="etag-1",
            uploaded_at=datetime.now(timezone.utc),
        )
        part2 = PartDB(
            part_id=uuid4(),
            upload_id=test_multipart_upload.upload_id,
            part_number=2,
            size_bytes=2048,
            etag="etag-2",
            uploaded_at=datetime.now(timezone.utc),
        )
        await part_repository.create(part1)
        await part_repository.create(part2)
        await db_session.commit()

        parts = await part_repository.list_by_upload(test_multipart_upload.upload_id)

        assert len(parts) == 2
        assert parts[0].part_number == 1
        assert parts[1].part_number == 2

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_list_by_object_version(
        self,
        part_repository: PartRepository,
        db_session: AsyncSession,
        test_object_with_version: tuple[ObjectDB, ObjectVersionDB],
    ) -> None:
        obj, version = test_object_with_version
        part0 = PartDB(
            part_id=uuid4(),
            object_id=obj.object_id,
            object_version=version.object_version,
            part_number=0,
            size_bytes=1024,
            etag="etag-0",
            uploaded_at=datetime.now(timezone.utc),
        )
        part1 = PartDB(
            part_id=uuid4(),
            object_id=obj.object_id,
            object_version=version.object_version,
            part_number=1,
            size_bytes=2048,
            etag="etag-1",
            uploaded_at=datetime.now(timezone.utc),
        )
        await part_repository.create(part0)
        await part_repository.create(part1)
        await db_session.commit()

        parts = await part_repository.list_by_object_version(obj.object_id, version.object_version)

        assert len(parts) == 2
        assert parts[0].part_number == 0
        assert parts[1].part_number == 1

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_by_object_version_and_number(
        self,
        part_repository: PartRepository,
        db_session: AsyncSession,
        test_object_with_version: tuple[ObjectDB, ObjectVersionDB],
    ) -> None:
        obj, version = test_object_with_version
        part = PartDB(
            part_id=uuid4(),
            object_id=obj.object_id,
            object_version=version.object_version,
            part_number=3,
            size_bytes=4096,
            etag="etag-3",
            uploaded_at=datetime.now(timezone.utc),
        )
        await part_repository.create(part)
        await db_session.commit()

        found = await part_repository.get_by_object_version_and_number(obj.object_id, version.object_version, 3)

        assert found is not None
        assert found.part_number == 3
        assert found.size_bytes == 4096


class TestPartRepositoryChunks:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_create_chunk(
        self,
        part_repository: PartRepository,
        db_session: AsyncSession,
        test_multipart_upload: MultipartUploadDB,
    ) -> None:
        part = PartDB(
            part_id=uuid4(),
            upload_id=test_multipart_upload.upload_id,
            part_number=1,
            size_bytes=5242880,
            etag="chunked-etag",
            uploaded_at=datetime.now(timezone.utc),
        )
        await part_repository.create(part)

        chunk = PartChunkDB(
            part_id=part.part_id,
            chunk_index=0,
            cid="QmTestChunk0",
            cipher_size_bytes=262144,
            plain_size_bytes=262144,
        )
        created = await part_repository.create_chunk(chunk)
        await db_session.commit()

        assert created.id is not None
        assert created.part_id == part.part_id
        assert created.chunk_index == 0
        assert created.cid == "QmTestChunk0"

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_list_chunks(
        self,
        part_repository: PartRepository,
        db_session: AsyncSession,
        test_multipart_upload: MultipartUploadDB,
    ) -> None:
        part = PartDB(
            part_id=uuid4(),
            upload_id=test_multipart_upload.upload_id,
            part_number=1,
            size_bytes=5242880,
            etag="multi-chunk-etag",
            uploaded_at=datetime.now(timezone.utc),
        )
        await part_repository.create(part)

        chunk0 = PartChunkDB(
            part_id=part.part_id,
            chunk_index=0,
            cid="QmChunk0",
            cipher_size_bytes=262144,
        )
        chunk1 = PartChunkDB(
            part_id=part.part_id,
            chunk_index=1,
            cid="QmChunk1",
            cipher_size_bytes=262144,
        )
        await part_repository.create_chunk(chunk0)
        await part_repository.create_chunk(chunk1)
        await db_session.commit()

        chunks = await part_repository.list_chunks(part.part_id)

        assert len(chunks) == 2
        assert chunks[0].chunk_index == 0
        assert chunks[1].chunk_index == 1

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_chunk(
        self,
        part_repository: PartRepository,
        db_session: AsyncSession,
        test_multipart_upload: MultipartUploadDB,
    ) -> None:
        part = PartDB(
            part_id=uuid4(),
            upload_id=test_multipart_upload.upload_id,
            part_number=1,
            size_bytes=262144,
            etag="single-chunk-etag",
            uploaded_at=datetime.now(timezone.utc),
        )
        await part_repository.create(part)

        chunk = PartChunkDB(
            part_id=part.part_id,
            chunk_index=0,
            cid="QmSingleChunk",
            cipher_size_bytes=262144,
        )
        await part_repository.create_chunk(chunk)
        await db_session.commit()

        found = await part_repository.get_chunk(part.part_id, 0)

        assert found is not None
        assert found.cid == "QmSingleChunk"


class TestPartRepositoryDelete:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_delete_part(
        self,
        part_repository: PartRepository,
        db_session: AsyncSession,
        test_multipart_upload: MultipartUploadDB,
    ) -> None:
        part = PartDB(
            part_id=uuid4(),
            upload_id=test_multipart_upload.upload_id,
            part_number=1,
            size_bytes=1024,
            etag="delete-etag",
            uploaded_at=datetime.now(timezone.utc),
        )
        await part_repository.create(part)
        await db_session.commit()

        deleted = await part_repository.delete(part.part_id)
        await db_session.commit()

        assert deleted is not None
        assert deleted.part_id == part.part_id

        found = await part_repository.get_by_id(part.part_id)
        assert found is None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_delete_part_cascades_to_chunks(
        self,
        part_repository: PartRepository,
        db_session: AsyncSession,
        test_multipart_upload: MultipartUploadDB,
    ) -> None:
        part = PartDB(
            part_id=uuid4(),
            upload_id=test_multipart_upload.upload_id,
            part_number=1,
            size_bytes=524288,
            etag="cascade-etag",
            uploaded_at=datetime.now(timezone.utc),
        )
        await part_repository.create(part)

        chunk = PartChunkDB(
            part_id=part.part_id,
            chunk_index=0,
            cid="QmCascadeChunk",
            cipher_size_bytes=262144,
        )
        await part_repository.create_chunk(chunk)
        await db_session.commit()

        await part_repository.delete(part.part_id)
        await db_session.commit()

        chunks = await part_repository.list_chunks(part.part_id)
        assert len(chunks) == 0
