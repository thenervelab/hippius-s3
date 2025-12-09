from __future__ import annotations

from datetime import datetime
from datetime import timezone
from uuid import UUID
from uuid import uuid4

from sqlmodel import Field
from sqlmodel import SQLModel


class ObjectDB(SQLModel, table=True):
    """Object identity table - stores bucket+key path and pointer to current version."""

    __tablename__ = "objects"

    object_id: UUID = Field(default_factory=uuid4, primary_key=True)
    bucket_id: UUID = Field(foreign_key="buckets.bucket_id", index=True)
    object_key: str = Field(max_length=1024)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), nullable=False)
    current_object_version: int = Field(default=1, nullable=False)

    class Config:
        arbitrary_types_allowed = True
