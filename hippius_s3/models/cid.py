from __future__ import annotations

from datetime import datetime
from datetime import timezone
from uuid import UUID
from uuid import uuid4

from sqlmodel import Field
from sqlmodel import SQLModel


class CIDDB(SQLModel, table=True):
    __tablename__ = "cids"

    id: UUID = Field(default_factory=uuid4, primary_key=True)
    cid: str = Field(unique=True, index=True, max_length=255)
    created_at: datetime = Field(
        default_factory=lambda: datetime.now(timezone.utc),
        nullable=False,
    )
