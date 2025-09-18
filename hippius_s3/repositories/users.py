from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any

from hippius_s3.utils import get_query


class UserRepository:
    def __init__(self, db: Any) -> None:
        self._db = db

    async def ensure_by_main_account(self, main_account_id: str) -> Any:
        return await self._db.fetchrow(
            get_query("get_or_create_user_by_main_account"),
            main_account_id,
            datetime.now(timezone.utc),
        )
