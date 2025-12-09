from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Optional
from uuid import UUID

from sqlmodel import Field
from sqlmodel import SQLModel


class PartChunkDB(SQLModel, table=True):
    """Part chunk table - stores individual IPFS chunks for each part."""

    __tablename__ = "part_chunks"

    id: Optional[int] = Field(default=None, primary_key=True)
    part_id: UUID = Field(foreign_key="parts.part_id", index=True, nullable=False)
    chunk_index: int = Field(nullable=False)
    cid: Optional[str] = Field(default=None, max_length=255)
    cid_id: Optional[UUID] = Field(default=None, foreign_key="cids.id")
    cipher_size_bytes: int = Field(nullable=False)
    plain_size_bytes: Optional[int] = Field(default=None)
    checksum: Optional[bytes] = Field(default=None)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc), nullable=False)

    class Config:
        arbitrary_types_allowed = True
