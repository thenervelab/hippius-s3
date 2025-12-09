from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Optional
from uuid import UUID
from uuid import uuid4

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field
from sqlmodel import SQLModel


class MultipartUploadDB(SQLModel, table=True):
    """Multipart upload table - tracks S3 multipart upload sessions."""

    __tablename__ = "multipart_uploads"

    upload_id: UUID = Field(default_factory=uuid4, primary_key=True)
    bucket_id: UUID = Field(foreign_key="buckets.bucket_id", index=True, nullable=False)
    object_id: Optional[UUID] = Field(default=None, foreign_key="objects.object_id", index=True)
    object_key: str = Field(max_length=1024, nullable=False)
    initiated_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), nullable=False)
    is_completed: bool = Field(default=False, nullable=False)
    content_type: Optional[str] = Field(default=None, max_length=255)
    metadata: dict[str, str] = Field(default_factory=dict, sa_column=Column(JSONB, nullable=True))

    class Config:
        arbitrary_types_allowed = True
