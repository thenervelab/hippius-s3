from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Optional
from uuid import UUID

from sqlalchemy import Column
from sqlalchemy.dialects.postgresql import JSONB
from sqlmodel import Field
from sqlmodel import SQLModel

from hippius_s3.models.enums import ObjectStatus
from hippius_s3.models.enums import VersionType


class ObjectVersionDB(SQLModel, table=True):
    """Object version table - stores actual object data for each version."""

    __tablename__ = "object_versions"

    object_id: UUID = Field(foreign_key="objects.object_id", primary_key=True)
    object_version: int = Field(primary_key=True)
    version_type: VersionType = Field(default=VersionType.USER, nullable=False)
    storage_version: int = Field(nullable=False)
    size_bytes: int = Field(nullable=False)
    content_type: str = Field(max_length=255, nullable=False)
    metadata: dict[str, str] = Field(default_factory=dict, sa_column=Column(JSONB, nullable=True))
    md5_hash: Optional[str] = Field(default=None, max_length=32)
    ipfs_cid: Optional[str] = Field(default=None, max_length=255)
    cid_id: Optional[UUID] = Field(default=None, foreign_key="cids.id", index=True)
    multipart: bool = Field(default=False, nullable=False)
    status: ObjectStatus = Field(default=ObjectStatus.PUBLISHING, nullable=False, index=True)
    append_version: int = Field(default=0, nullable=False)
    manifest_cid: Optional[str] = Field(default=None, max_length=255)
    manifest_built_for_version: Optional[int] = Field(default=None)
    manifest_built_at: Optional[datetime] = Field(default=None)
    last_append_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), nullable=False)
    last_modified: Optional[datetime] = Field(default=None)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), nullable=False)

    class Config:
        arbitrary_types_allowed = True
