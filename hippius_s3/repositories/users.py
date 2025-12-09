from __future__ import annotations

from datetime import datetime
from datetime import timezone

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from hippius_s3.models.user import UserDB
from hippius_s3.orm.base_repository import BaseRepository


class UserRepository(BaseRepository[UserDB]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, UserDB)

    async def ensure_by_main_account(self, main_account_id: str) -> UserDB:
        stmt = select(UserDB).where(UserDB.main_account_id == main_account_id)
        result = await self.session.execute(stmt)
        user = result.scalar_one_or_none()

        if user is None:
            user = UserDB(
                main_account_id=main_account_id,
                created_at=datetime.now(timezone.utc),
            )
            return await self.create(user)

        return user
