import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from hippius_s3.orm import get_session_factory
from hippius_s3.orm import initialize_engine


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


class TestSessionManagement:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_session_can_execute_query(self, db_session: AsyncSession) -> None:
        result = await db_session.execute(text("SELECT 1 as test"))
        row = result.first()
        assert row is not None
        assert row[0] == 1

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_session_can_query_users_table(
        self,
        db_session: AsyncSession,
    ) -> None:
        result = await db_session.execute(
            text("SELECT COUNT(*) FROM users")
        )
        count = result.scalar()
        assert count is not None
        assert count >= 0

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_session_rollback_works(self, db_session: AsyncSession) -> None:
        await db_session.execute(
            text("INSERT INTO users (main_account_id) VALUES (:id)"),
            {"id": "test-rollback-user-123"},
        )
        await db_session.rollback()

        result = await db_session.execute(
            text("SELECT COUNT(*) FROM users WHERE main_account_id = :id"),
            {"id": "test-rollback-user-123"},
        )
        count = result.scalar()
        assert count == 0


class TestEngineInitialization:
    def test_initialize_engine_creates_engine(self) -> None:
        database_url = "postgresql+asyncpg://postgres:postgres@localhost:5432/hippius?sslmode=disable"
        engine = initialize_engine(database_url)

        assert engine is not None
        assert engine.url.drivername == "postgresql+asyncpg"

    def test_initialize_engine_handles_sslmode(self) -> None:
        database_url = "postgresql+asyncpg://postgres:postgres@localhost:5432/hippius?sslmode=disable"
        engine = initialize_engine(database_url)

        assert "sslmode" not in str(engine.url)

    def test_get_session_factory_after_init(self) -> None:
        database_url = "postgresql+asyncpg://postgres:postgres@localhost:5432/hippius"
        initialize_engine(database_url)

        factory = get_session_factory()
        assert factory is not None
