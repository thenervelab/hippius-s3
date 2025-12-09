import time

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from hippius_s3.repositories.users import UserRepository


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
async def user_repository(db_session: AsyncSession) -> UserRepository:
    return UserRepository(db_session)


class TestUserRepositoryPerformance:
    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.skip(reason="Performance benchmark - run manually with database")
    async def test_ensure_by_main_account_performance(
        self,
        user_repository: UserRepository,
        db_session: AsyncSession,
    ) -> None:
        iterations = 100
        main_account_id = "perf-test-user"

        await user_repository.ensure_by_main_account(main_account_id)
        await db_session.commit()

        start_time = time.perf_counter()
        for _ in range(iterations):
            await user_repository.ensure_by_main_account(main_account_id)
        end_time = time.perf_counter()

        elapsed_ms = (end_time - start_time) * 1000
        avg_ms = elapsed_ms / iterations

        print(f"\nPerformance Results:")
        print(f"  Total iterations: {iterations}")
        print(f"  Total time: {elapsed_ms:.2f} ms")
        print(f"  Average per operation: {avg_ms:.3f} ms")
        print(f"  Operations per second: {1000 / avg_ms:.2f}")

        assert avg_ms < 10, f"ensure_by_main_account took {avg_ms:.3f}ms on average, expected < 10ms"

    @pytest.mark.asyncio
    @pytest.mark.integration
    @pytest.mark.skip(reason="Performance benchmark - run manually with database")
    async def test_get_performance(
        self,
        user_repository: UserRepository,
        db_session: AsyncSession,
    ) -> None:
        iterations = 100
        main_account_id = "perf-test-get-user"

        await user_repository.ensure_by_main_account(main_account_id)
        await db_session.commit()

        start_time = time.perf_counter()
        for _ in range(iterations):
            await user_repository.get(main_account_id)
        end_time = time.perf_counter()

        elapsed_ms = (end_time - start_time) * 1000
        avg_ms = elapsed_ms / iterations

        print(f"\nPerformance Results:")
        print(f"  Total iterations: {iterations}")
        print(f"  Total time: {elapsed_ms:.2f} ms")
        print(f"  Average per operation: {avg_ms:.3f} ms")
        print(f"  Operations per second: {1000 / avg_ms:.2f}")

        assert avg_ms < 5, f"get took {avg_ms:.3f}ms on average, expected < 5ms"
