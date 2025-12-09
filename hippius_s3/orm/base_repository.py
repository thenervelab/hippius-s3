from typing import Any
from typing import Generic
from typing import List
from typing import Optional
from typing import Type
from typing import TypeVar

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession


T = TypeVar("T")


class BaseRepository(Generic[T]):
    def __init__(self, session: AsyncSession, model: Type[T]):
        self.session = session
        self.model = model

    async def get(self, id: Any) -> Optional[T]:
        return await self.session.get(self.model, id)

    async def list(self, limit: int = 100, offset: int = 0) -> List[T]:
        stmt = select(self.model).limit(limit).offset(offset)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def create(self, obj: T) -> T:
        self.session.add(obj)
        await self.session.flush()
        await self.session.refresh(obj)
        return obj

    async def update(self, obj: T) -> T:
        await self.session.flush()
        await self.session.refresh(obj)
        return obj

    async def delete(self, obj: T) -> None:
        await self.session.delete(obj)
        await self.session.flush()

    async def commit(self) -> None:
        await self.session.commit()

    async def rollback(self) -> None:
        await self.session.rollback()
