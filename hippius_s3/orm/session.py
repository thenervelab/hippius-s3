from typing import AsyncGenerator

from fastapi import Request
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine


_engine: AsyncEngine | None = None
_session_factory: async_sessionmaker[AsyncSession] | None = None


def initialize_engine(database_url: str) -> AsyncEngine:
    global _engine, _session_factory

    clean_url = database_url.replace("?sslmode=disable", "")

    _engine = create_async_engine(
        clean_url,
        echo=False,
        pool_size=20,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
        connect_args={"ssl": False} if "sslmode=disable" in database_url else {},
    )

    _session_factory = async_sessionmaker(
        _engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )

    return _engine


def get_engine() -> AsyncEngine:
    if _engine is None:
        raise RuntimeError("Database engine not initialized. Call initialize_engine() first.")
    return _engine


def get_session_factory() -> async_sessionmaker[AsyncSession]:
    if _session_factory is None:
        raise RuntimeError("Session factory not initialized. Call initialize_engine() first.")
    return _session_factory


async def get_async_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    session_factory = get_session_factory()
    async with session_factory() as session:
        try:
            yield session
        except Exception:
            await session.rollback()
            raise
        finally:
            await session.close()
