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

    async def ensure_by_main_account_read_first(self, main_account_id: str) -> None:
        # HD-2: on the hot read path (HEAD) the row almost always exists, so read first and only fall
        # back to the INSERT-on-conflict write on a genuine miss — keeping the common case off the WAL.
        existing = await self._db.fetchval(get_query("get_user_id_by_main_account"), main_account_id)
        if existing is None:
            await self.ensure_by_main_account(main_account_id)
