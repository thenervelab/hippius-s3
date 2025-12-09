from __future__ import annotations

from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from hippius_s3.models.multipart_upload import MultipartUploadDB


class MultipartUploadRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, upload: MultipartUploadDB) -> MultipartUploadDB:
        self.session.add(upload)
        await self.session.flush()
        await self.session.refresh(upload)
        return upload

    async def get_by_id(self, upload_id: UUID) -> Optional[MultipartUploadDB]:
        stmt = select(MultipartUploadDB).where(MultipartUploadDB.upload_id == upload_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_bucket_and_key(self, bucket_id: UUID, object_key: str) -> Optional[MultipartUploadDB]:
        """Get active multipart upload for a bucket+key (not completed)."""
        stmt = select(MultipartUploadDB).where(
            MultipartUploadDB.bucket_id == bucket_id,
            MultipartUploadDB.object_key == object_key,
            ~MultipartUploadDB.is_completed,
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_by_bucket(self, bucket_id: UUID, include_completed: bool = False) -> list[MultipartUploadDB]:
        """List all multipart uploads for a bucket."""
        stmt = select(MultipartUploadDB).where(MultipartUploadDB.bucket_id == bucket_id)
        if not include_completed:
            stmt = stmt.where(~MultipartUploadDB.is_completed)
        stmt = stmt.order_by(MultipartUploadDB.initiated_at.desc())
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def mark_completed(self, upload_id: UUID, object_id: UUID) -> Optional[MultipartUploadDB]:
        """Mark upload as completed and link to final object."""
        upload = await self.get_by_id(upload_id)
        if upload is None:
            return None
        upload.is_completed = True
        upload.object_id = object_id
        await self.session.flush()
        await self.session.refresh(upload)
        return upload

    async def delete(self, upload_id: UUID) -> Optional[MultipartUploadDB]:
        """Delete a multipart upload (CASCADE to parts via DB FK)."""
        upload = await self.get_by_id(upload_id)
        if upload is None:
            return None
        await self.session.delete(upload)
        await self.session.flush()
        return upload
