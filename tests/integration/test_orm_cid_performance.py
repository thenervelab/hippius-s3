import time

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

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


class TestCIDRepositoryPerformance:
    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.skip(reason="Performance benchmark - run manually with database")
    async def test_upsert_new_cid_performance(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        iterations = 100
        cid_prefix = "QmPerfTestNew"

        start_time = time.perf_counter()
        for i in range(iterations):
            await cid_repository.upsert(f"{cid_prefix}{i}")
        await db_session.commit()
        end_time = time.perf_counter()

        elapsed_ms = (end_time - start_time) * 1000
        avg_ms = elapsed_ms / iterations

        print(f"\nNew CID Upsert Performance:")
        print(f"  Total iterations: {iterations}")
        print(f"  Total time: {elapsed_ms:.2f} ms")
        print(f"  Average per operation: {avg_ms:.3f} ms")
        print(f"  Operations per second: {1000 / avg_ms:.2f}")

        assert avg_ms < 5.0, f"upsert took {avg_ms:.3f}ms on average, expected < 5ms"

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.skip(reason="Performance benchmark - run manually with database")
    async def test_upsert_existing_cid_performance(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        iterations = 100
        cid_string = "QmPerfTestExisting"

        await cid_repository.upsert(cid_string)
        await db_session.commit()

        start_time = time.perf_counter()
        for _ in range(iterations):
            await cid_repository.upsert(cid_string)
        end_time = time.perf_counter()

        elapsed_ms = (end_time - start_time) * 1000
        avg_ms = elapsed_ms / iterations

        print(f"\nExisting CID Upsert Performance:")
        print(f"  Total iterations: {iterations}")
        print(f"  Total time: {elapsed_ms:.2f} ms")
        print(f"  Average per operation: {avg_ms:.3f} ms")
        print(f"  Operations per second: {1000 / avg_ms:.2f}")

        assert avg_ms < 3.0, f"upsert took {avg_ms:.3f}ms on average, expected < 3ms"

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.skip(reason="Performance benchmark - run manually with database")
    async def test_get_cid_performance(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        iterations = 100
        cid_string = "QmPerfTestGet"

        cid_id = await cid_repository.upsert(cid_string)
        await db_session.commit()

        start_time = time.perf_counter()
        for _ in range(iterations):
            await cid_repository.get(cid_id)
        end_time = time.perf_counter()

        elapsed_ms = (end_time - start_time) * 1000
        avg_ms = elapsed_ms / iterations

        print(f"\nGet CID Performance:")
        print(f"  Total iterations: {iterations}")
        print(f"  Total time: {elapsed_ms:.2f} ms")
        print(f"  Average per operation: {avg_ms:.3f} ms")
        print(f"  Operations per second: {1000 / avg_ms:.2f}")

        assert avg_ms < 2.0, f"get took {avg_ms:.3f}ms on average, expected < 2ms"

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.skip(reason="Performance benchmark - run manually with database")
    async def test_bulk_upsert_performance(
        self,
        cid_repository: CIDRepository,
        db_session: AsyncSession,
    ) -> None:
        iterations = 1000
        cid_prefix = "QmPerfTestBulk"

        start_time = time.perf_counter()
        for i in range(iterations):
            await cid_repository.upsert(f"{cid_prefix}{i}")
            if i % 100 == 0:
                await db_session.commit()
        await db_session.commit()
        end_time = time.perf_counter()

        elapsed_ms = (end_time - start_time) * 1000
        avg_ms = elapsed_ms / iterations

        print(f"\nBulk Upsert Performance:")
        print(f"  Total iterations: {iterations}")
        print(f"  Total time: {elapsed_ms:.2f} ms")
        print(f"  Average per operation: {avg_ms:.3f} ms")
        print(f"  Operations per second: {1000 / avg_ms:.2f}")
        print(f"  Throughput: {iterations / (elapsed_ms / 1000):.2f} ops/sec")

        assert avg_ms < 10.0, f"bulk upsert took {avg_ms:.3f}ms on average, expected < 10ms"
