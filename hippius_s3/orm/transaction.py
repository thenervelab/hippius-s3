from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession


@asynccontextmanager
async def transactional(session: AsyncSession) -> AsyncGenerator[AsyncSession, None]:
    try:
        yield session
        await session.commit()
    except Exception:
        await session.rollback()
        raise
