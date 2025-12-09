from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Optional
from uuid import UUID
from uuid import uuid4

from sqlmodel import Field
from sqlmodel import SQLModel


class PartDB(SQLModel, table=True):
    """Part table - stores individual parts for multipart uploads and chunked objects."""

    __tablename__ = "parts"

    part_id: UUID = Field(default_factory=uuid4, primary_key=True)
    upload_id: Optional[UUID] = Field(default=None, foreign_key="multipart_uploads.upload_id", index=True)
    object_id: Optional[UUID] = Field(default=None, foreign_key="objects.object_id", index=True)
    object_version: Optional[int] = Field(default=None)
    part_number: int = Field(nullable=False)
    ipfs_cid: Optional[str] = Field(default=None, max_length=255)
    cid_id: Optional[UUID] = Field(default=None, foreign_key="cids.id", index=True)
    size_bytes: int = Field(nullable=False)
    etag: str = Field(max_length=255, nullable=False)
    uploaded_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), nullable=False)
    chunk_size_bytes: Optional[int] = Field(default=None)

    class Config:
        arbitrary_types_allowed = True
