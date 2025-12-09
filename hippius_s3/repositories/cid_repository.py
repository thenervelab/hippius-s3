from __future__ import annotations

from uuid import UUID

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from hippius_s3.models.cid import CIDDB
from hippius_s3.orm.base_repository import BaseRepository


class CIDRepository(BaseRepository[CIDDB]):
    def __init__(self, session: AsyncSession) -> None:
        super().__init__(session, CIDDB)

    async def upsert(self, cid: str) -> UUID:
        """Upsert CID and return UUID (atomic, single query).

        Uses PostgreSQL ON CONFLICT to handle concurrent inserts safely.
        If CID already exists, returns the existing ID.
        If CID is new, inserts and returns the new ID.
        """
        stmt = insert(CIDDB).values(cid=cid)
        stmt = stmt.on_conflict_do_update(index_elements=["cid"], set_={"cid": stmt.excluded.cid}).returning(CIDDB.id)

        result = await self.session.execute(stmt)
        return result.scalar_one()
