from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Optional
from typing import Tuple
from uuid import UUID

from sqlalchemy import select
from sqlalchemy import text
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from hippius_s3.models.object import ObjectDB
from hippius_s3.models.object_version import ObjectVersionDB


class ObjectRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_with_version(
        self,
        obj: ObjectDB,
        version: ObjectVersionDB,
    ) -> Tuple[ObjectDB, ObjectVersionDB]:
        """Create object and its first version atomically."""
        obj.current_object_version = version.object_version
        self.session.add(obj)
        self.session.add(version)
        await self.session.flush()
        await self.session.refresh(obj)
        await self.session.refresh(version)
        return obj, version

    async def get_by_id(self, object_id: UUID) -> Optional[ObjectDB]:
        stmt = select(ObjectDB).where(ObjectDB.object_id == object_id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_path(self, bucket_id: UUID, object_key: str) -> Optional[ObjectDB]:
        stmt = select(ObjectDB).where(ObjectDB.bucket_id == bucket_id, ObjectDB.object_key == object_key)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_with_current_version(
        self, bucket_id: UUID, object_key: str
    ) -> Optional[Tuple[ObjectDB, ObjectVersionDB]]:
        """Get object and its current version data with a JOIN."""
        stmt = (
            select(ObjectDB, ObjectVersionDB)
            .join(
                ObjectVersionDB,
                (ObjectDB.object_id == ObjectVersionDB.object_id)
                & (ObjectDB.current_object_version == ObjectVersionDB.object_version),
            )
            .where(ObjectDB.bucket_id == bucket_id, ObjectDB.object_key == object_key)
        )
        result = await self.session.execute(stmt)
        row = result.first()
        if row is None:
            return None
        return row[0], row[1]

    async def create_version(self, version: ObjectVersionDB) -> ObjectVersionDB:
        """Create a new version for an existing object."""
        self.session.add(version)
        await self.session.flush()
        await self.session.refresh(version)
        return version

    async def get_version(self, object_id: UUID, object_version: int) -> Optional[ObjectVersionDB]:
        """Get a specific version of an object."""
        stmt = select(ObjectVersionDB).where(
            ObjectVersionDB.object_id == object_id, ObjectVersionDB.object_version == object_version
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_current_version(self, object_id: UUID) -> Optional[ObjectVersionDB]:
        """Get the current version of an object."""
        obj = await self.get_by_id(object_id)
        if obj is None:
            return None
        return await self.get_version(object_id, obj.current_object_version)

    async def list_versions(self, object_id: UUID) -> list[ObjectVersionDB]:
        """List all versions of an object ordered by version number."""
        stmt = (
            select(ObjectVersionDB)
            .where(ObjectVersionDB.object_id == object_id)
            .order_by(ObjectVersionDB.object_version.desc())
        )
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def swap_current_version_cas(
        self, object_id: UUID, expected_old_version: int, new_version: int
    ) -> Optional[ObjectDB]:
        """Compare-and-swap to atomically update current_object_version.

        Returns updated ObjectDB if successful (old version matched), None if CAS failed.
        """
        stmt = (
            update(ObjectDB)
            .where(ObjectDB.object_id == object_id, ObjectDB.current_object_version == expected_old_version)
            .values(current_object_version=new_version)
            .returning(ObjectDB)
        )
        result = await self.session.execute(stmt)
        obj = result.scalar_one_or_none()
        if obj is not None:
            await self.session.flush()
        return obj

    async def delete(self, object_id: UUID) -> Optional[ObjectDB]:
        """Hard delete object (CASCADE to versions and parts via DB FK)."""
        obj = await self.get_by_id(object_id)
        if obj is None:
            return None
        await self.session.delete(obj)
        await self.session.flush()
        return obj

    async def get_next_version_number(self, object_id: UUID) -> int:
        """Get the next available version number for an object."""
        stmt = text(
            """
            SELECT COALESCE(MAX(object_version), 0) + 1
            FROM object_versions
            WHERE object_id = :object_id
            """
        )
        result = await self.session.execute(stmt, {"object_id": str(object_id)})
        return result.scalar_one()

    async def update_version_metadata(
        self, object_id: UUID, object_version: int, metadata: dict[str, str]
    ) -> Optional[ObjectVersionDB]:
        """Update metadata for a specific version."""
        version = await self.get_version(object_id, object_version)
        if version is None:
            return None
        version.metadata = metadata
        version.last_modified = datetime.now(timezone.utc)
        await self.session.flush()
        await self.session.refresh(version)
        return version
