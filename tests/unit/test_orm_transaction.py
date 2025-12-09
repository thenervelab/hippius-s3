from unittest.mock import AsyncMock
from unittest.mock import MagicMock

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from hippius_s3.orm.transaction import transactional


@pytest.fixture
def mock_session() -> AsyncSession:
    session = MagicMock(spec=AsyncSession)
    session.commit = AsyncMock()
    session.rollback = AsyncMock()
    return session


class TestTransactionalContextManager:
    @pytest.mark.asyncio
    async def test_commits_on_success(self, mock_session: AsyncSession) -> None:
        async with transactional(mock_session):
            pass

        mock_session.commit.assert_called_once()
        mock_session.rollback.assert_not_called()

    @pytest.mark.asyncio
    async def test_rollback_on_exception(self, mock_session: AsyncSession) -> None:
        with pytest.raises(ValueError):
            async with transactional(mock_session):
                raise ValueError("Test error")

        mock_session.rollback.assert_called_once()
        mock_session.commit.assert_not_called()

    @pytest.mark.asyncio
    async def test_yields_session(self, mock_session: AsyncSession) -> None:
        async with transactional(mock_session) as session:
            assert session is mock_session

    @pytest.mark.asyncio
    async def test_multiple_operations_in_transaction(
        self,
        mock_session: AsyncSession,
    ) -> None:
        operations_executed = []

        async with transactional(mock_session):
            operations_executed.append(1)
            operations_executed.append(2)
            operations_executed.append(3)

        assert operations_executed == [1, 2, 3]
        mock_session.commit.assert_called_once()
