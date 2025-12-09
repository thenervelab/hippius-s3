from typing import Any
from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hippius_s3.orm.base_repository import BaseRepository


class MockModel:
    def __init__(self, id: int, name: str):
        self.id = id
        self.name = name


@pytest.fixture
def mock_session() -> AsyncSession:
    session = MagicMock(spec=AsyncSession)
    session.get = AsyncMock()
    session.execute = AsyncMock()
    session.add = MagicMock()
    session.delete = AsyncMock()
    session.flush = AsyncMock()
    session.refresh = AsyncMock()
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def repository(mock_session: AsyncSession) -> BaseRepository[MockModel]:
    return BaseRepository(mock_session, MockModel)


class TestBaseRepositoryGet:
    @pytest.mark.asyncio
    async def test_get_returns_model_when_found(
        self,
        repository: BaseRepository[MockModel],
        mock_session: AsyncSession,
    ) -> None:
        expected_model = MockModel(id=1, name="test")
        mock_session.get.return_value = expected_model

        result = await repository.get(1)

        assert result == expected_model
        mock_session.get.assert_called_once_with(MockModel, 1)

    @pytest.mark.asyncio
    async def test_get_returns_none_when_not_found(
        self,
        repository: BaseRepository[MockModel],
        mock_session: AsyncSession,
    ) -> None:
        mock_session.get.return_value = None

        result = await repository.get(999)

        assert result is None
        mock_session.get.assert_called_once_with(MockModel, 999)


class TestBaseRepositoryList:
    @pytest.mark.skip(reason="list() requires proper SQLAlchemy model, tested in integration tests")
    @pytest.mark.asyncio
    async def test_list_returns_models(
        self,
        repository: BaseRepository[MockModel],
        mock_session: AsyncSession,
    ) -> None:
        pass

    @pytest.mark.skip(reason="list() requires proper SQLAlchemy model, tested in integration tests")
    @pytest.mark.asyncio
    async def test_list_respects_limit_and_offset(
        self,
        repository: BaseRepository[MockModel],
        mock_session: AsyncSession,
    ) -> None:
        pass


class TestBaseRepositoryCreate:
    @pytest.mark.asyncio
    async def test_create_adds_and_flushes(
        self,
        repository: BaseRepository[MockModel],
        mock_session: AsyncSession,
    ) -> None:
        model = MockModel(id=1, name="new")

        result = await repository.create(model)

        assert result == model
        mock_session.add.assert_called_once_with(model)
        mock_session.flush.assert_called_once()
        mock_session.refresh.assert_called_once_with(model)


class TestBaseRepositoryUpdate:
    @pytest.mark.asyncio
    async def test_update_flushes_and_refreshes(
        self,
        repository: BaseRepository[MockModel],
        mock_session: AsyncSession,
    ) -> None:
        model = MockModel(id=1, name="updated")

        result = await repository.update(model)

        assert result == model
        mock_session.flush.assert_called_once()
        mock_session.refresh.assert_called_once_with(model)


class TestBaseRepositoryDelete:
    @pytest.mark.asyncio
    async def test_delete_removes_and_flushes(
        self,
        repository: BaseRepository[MockModel],
        mock_session: AsyncSession,
    ) -> None:
        model = MockModel(id=1, name="to_delete")

        await repository.delete(model)

        mock_session.delete.assert_called_once_with(model)
        mock_session.flush.assert_called_once()


class TestBaseRepositoryTransactions:
    @pytest.mark.asyncio
    async def test_commit_calls_session_commit(
        self,
        repository: BaseRepository[MockModel],
        mock_session: AsyncSession,
    ) -> None:
        await repository.commit()
        mock_session.commit.assert_called_once()

    @pytest.mark.asyncio
    async def test_rollback_calls_session_rollback(
        self,
        repository: BaseRepository[MockModel],
        mock_session: AsyncSession,
    ) -> None:
        await repository.rollback()
        mock_session.rollback.assert_called_once()
