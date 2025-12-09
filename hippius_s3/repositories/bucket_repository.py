from __future__ import annotations

from typing import Optional
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from hippius_s3.models.bucket import BucketDB


class BucketRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(self, bucket: BucketDB) -> BucketDB:
        self.session.add(bucket)
        await self.session.flush()
        await self.session.refresh(bucket)
        return bucket

    async def get_by_id(self, bucket_id: UUID) -> Optional[BucketDB]:
        stmt = select(BucketDB).where(BucketDB.bucket_id == bucket_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_name(self, bucket_name: str) -> Optional[BucketDB]:
        stmt = select(BucketDB).where(BucketDB.bucket_name == bucket_name)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_name_and_owner(self, bucket_name: str, main_account_id: str) -> Optional[BucketDB]:
        stmt = select(BucketDB).where(BucketDB.bucket_name == bucket_name, BucketDB.main_account_id == main_account_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_by_owner(self, main_account_id: str) -> list[BucketDB]:
        stmt = select(BucketDB).where(BucketDB.main_account_id == main_account_id).order_by(BucketDB.created_at.desc())
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def update_tags(self, bucket_id: UUID, tags: dict[str, str]) -> Optional[BucketDB]:
        bucket = await self.get_by_id(bucket_id)
        if bucket is None:
            return None
        bucket.tags = tags
        await self.session.flush()
        await self.session.refresh(bucket)
        return bucket

    async def delete(self, bucket_id: UUID) -> Optional[BucketDB]:
        bucket = await self.get_by_id(bucket_id)
        if bucket is None:
            return None
        await self.session.delete(bucket)
        await self.session.flush()
        return bucket
