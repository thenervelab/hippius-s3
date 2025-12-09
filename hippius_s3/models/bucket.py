from __future__ import annotations

from datetime import datetime
from datetime import timezone
from uuid import UUID
from uuid import uuid4

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field
from sqlmodel import SQLModel


class BucketDB(SQLModel, table=True):
    __tablename__ = "buckets"

    bucket_id: UUID = Field(default_factory=uuid4, primary_key=True)
    bucket_name: str = Field(unique=True, index=True, max_length=255)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), nullable=False)
    is_public: bool = Field(default=False, nullable=False)
    main_account_id: str = Field(foreign_key="users.main_account_id", index=True, max_length=255)
    tags: dict[str, str] = Field(default_factory=dict, sa_column=Column(JSONB, nullable=False, server_default="{}"))

    class Config:
        arbitrary_types_allowed = True
