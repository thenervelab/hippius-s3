import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from hippius_s3.models.cid import CIDDB
from hippius_s3.repositories.cid_repository import CIDRepository


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
async def cid_repository(db_session: AsyncSession) -> CIDRepository:
    return CIDRepository(db_session)


class TestCIDRepositoryUpsert:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_upsert_creates_new_cid(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        cid_string = "QmTest123NewCID"

        cid_id = await cid_repository.upsert(cid_string)

        assert cid_id is not None

        cid_obj = await cid_repository.get(cid_id)
        assert cid_obj is not None
        assert cid_obj.cid == cid_string

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_upsert_returns_existing_cid(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        cid_string = "QmTest456ExistingCID"

        cid_id1 = await cid_repository.upsert(cid_string)
        await db_session.commit()

        cid_id2 = await cid_repository.upsert(cid_string)

        assert cid_id1 == cid_id2

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_upsert_is_idempotent(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        cid_string = "QmTest789IdempotentCID"

        cid_id1 = await cid_repository.upsert(cid_string)
        await db_session.commit()

        cid_id2 = await cid_repository.upsert(cid_string)
        cid_id3 = await cid_repository.upsert(cid_string)

        assert cid_id1 == cid_id2 == cid_id3

        cids = await cid_repository.list(limit=1000)
        matching_cids = [c for c in cids if c.cid == cid_string]
        assert len(matching_cids) == 1

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_upsert_handles_concurrent_inserts(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        import asyncio

        cid_string = "QmTestConcurrentCID"

        async def upsert_task() -> str:
            repo = CIDRepository(db_session)
            cid_id = await repo.upsert(cid_string)
            return str(cid_id)

        results = await asyncio.gather(*[upsert_task() for _ in range(10)])

        assert len(set(results)) == 1

        cids = await cid_repository.list(limit=1000)
        matching_cids = [c for c in cids if c.cid == cid_string]
        assert len(matching_cids) == 1


class TestCIDRepositoryBasicOperations:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_returns_cid(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        cid_string = "QmTestGetCID"
        cid_id = await cid_repository.upsert(cid_string)
        await db_session.commit()

        cid_obj = await cid_repository.get(cid_id)

        assert cid_obj is not None
        assert cid_obj.id == cid_id
        assert cid_obj.cid == cid_string
        assert cid_obj.created_at is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_returns_none_for_nonexistent(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        from uuid import uuid4

        fake_id = uuid4()
        cid_obj = await cid_repository.get(fake_id)

        assert cid_obj is None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_list_returns_cids(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        await cid_repository.upsert("QmTestList1")
        await cid_repository.upsert("QmTestList2")
        await db_session.commit()

        cids = await cid_repository.list(limit=100)

        assert len(cids) >= 2
        cid_strings = [c.cid for c in cids]
        assert "QmTestList1" in cid_strings
        assert "QmTestList2" in cid_strings

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_create_cid_directly(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        cid_obj = CIDDB(cid="QmTestDirectCreate")

        created = await cid_repository.create(cid_obj)
        await db_session.commit()

        assert created.id is not None
        assert created.cid == "QmTestDirectCreate"

        retrieved = await cid_repository.get(created.id)
        assert retrieved is not None
        assert retrieved.cid == created.cid
