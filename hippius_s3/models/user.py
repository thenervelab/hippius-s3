from __future__ import annotations

from datetime import datetime
from datetime import timezone

from sqlmodel import Field
from sqlmodel import SQLModel


class UserDB(SQLModel, table=True):
    __tablename__ = "users"

    main_account_id: str = Field(primary_key=True, index=True)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
