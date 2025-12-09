from __future__ import annotations

from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from hippius_s3.models.part import PartDB
from hippius_s3.models.part_chunk import PartChunkDB


class PartRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, part: PartDB) -> PartDB:
        self.session.add(part)
        await self.session.flush()
        await self.session.refresh(part)
        return part

    async def get_by_id(self, part_id: UUID) -> Optional[PartDB]:
        stmt = select(PartDB).where(PartDB.part_id == part_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_by_upload(self, upload_id: UUID) -> list[PartDB]:
        """List all parts for a multipart upload session, ordered by part number."""
        stmt = select(PartDB).where(PartDB.upload_id == upload_id).order_by(PartDB.part_number.asc())
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def list_by_object_version(self, object_id: UUID, object_version: int) -> list[PartDB]:
        """List all parts for a specific object version, ordered by part number."""
        stmt = (
            select(PartDB)
            .where(PartDB.object_id == object_id, PartDB.object_version == object_version)
            .order_by(PartDB.part_number.asc())
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_object_version_and_number(
        self, object_id: UUID, object_version: int, part_number: int
    ) -> Optional[PartDB]:
        """Get a specific part by object version and part number."""
        stmt = select(PartDB).where(
            PartDB.object_id == object_id,
            PartDB.object_version == object_version,
            PartDB.part_number == part_number,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def delete(self, part_id: UUID) -> Optional[PartDB]:
        """Delete a part (CASCADE to chunks via DB FK)."""
        part = await self.get_by_id(part_id)
        if part is None:
            return None
        await self.session.delete(part)
        await self.session.flush()
        return part

    async def create_chunk(self, chunk: PartChunkDB) -> PartChunkDB:
        """Create a chunk for a part."""
        self.session.add(chunk)
        await self.session.flush()
        await self.session.refresh(chunk)
        return chunk

    async def list_chunks(self, part_id: UUID) -> list[PartChunkDB]:
        """List all chunks for a part, ordered by chunk index."""
        stmt = select(PartChunkDB).where(PartChunkDB.part_id == part_id).order_by(PartChunkDB.chunk_index.asc())
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_chunk(self, part_id: UUID, chunk_index: int) -> Optional[PartChunkDB]:
        """Get a specific chunk by part and index."""
        stmt = select(PartChunkDB).where(PartChunkDB.part_id == part_id, PartChunkDB.chunk_index == chunk_index)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()
