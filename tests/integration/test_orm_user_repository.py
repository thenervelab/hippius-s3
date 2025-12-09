import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

from hippius_s3.models.user import UserDB
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


class TestUserRepositoryEnsureByMainAccount:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_ensure_creates_new_user(
        self,
        user_repository: UserRepository,
        db_session: AsyncSession,
    ) -> None:
        main_account_id = "test-main-account-123"

        user = await user_repository.ensure_by_main_account(main_account_id)

        assert user.main_account_id == main_account_id
        assert user.created_at is not None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_ensure_returns_existing_user(
        self,
        user_repository: UserRepository,
        db_session: AsyncSession,
    ) -> None:
        main_account_id = "test-main-account-456"

        user1 = await user_repository.ensure_by_main_account(main_account_id)
        await db_session.commit()

        user2 = await user_repository.ensure_by_main_account(main_account_id)

        assert user1.main_account_id == user2.main_account_id
        assert user1.created_at == user2.created_at

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_ensure_is_idempotent(
        self,
        user_repository: UserRepository,
        db_session: AsyncSession,
    ) -> None:
        main_account_id = "test-main-account-789"

        user1 = await user_repository.ensure_by_main_account(main_account_id)
        await db_session.commit()

        user2 = await user_repository.ensure_by_main_account(main_account_id)
        user3 = await user_repository.ensure_by_main_account(main_account_id)

        assert user1.main_account_id == user2.main_account_id == user3.main_account_id


class TestUserRepositoryBaseOperations:
    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_returns_user(
        self,
        user_repository: UserRepository,
        db_session: AsyncSession,
    ) -> None:
        main_account_id = "test-get-user-123"
        await user_repository.ensure_by_main_account(main_account_id)
        await db_session.commit()

        user = await user_repository.get(main_account_id)

        assert user is not None
        assert user.main_account_id == main_account_id

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_get_returns_none_for_nonexistent(
        self,
        user_repository: UserRepository,
        db_session: AsyncSession,
    ) -> None:
        user = await user_repository.get("nonexistent-user-999")

        assert user is None

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_list_returns_users(
        self,
        user_repository: UserRepository,
        db_session: AsyncSession,
    ) -> None:
        await user_repository.ensure_by_main_account("test-list-user-1")
        await user_repository.ensure_by_main_account("test-list-user-2")
        await db_session.commit()

        users = await user_repository.list(limit=10)

        assert len(users) >= 2
        account_ids = [user.main_account_id for user in users]
        assert "test-list-user-1" in account_ids
        assert "test-list-user-2" in account_ids

    @pytest.mark.asyncio
    @pytest.mark.integration
    async def test_create_new_user(
        self,
        user_repository: UserRepository,
        db_session: AsyncSession,
    ) -> None:
        user = UserDB(main_account_id="test-create-user-123")

        created_user = await user_repository.create(user)
        await db_session.commit()

        assert created_user.main_account_id == "test-create-user-123"
        assert created_user.created_at is not None

        retrieved_user = await user_repository.get("test-create-user-123")
        assert retrieved_user is not None
        assert retrieved_user.main_account_id == created_user.main_account_id
