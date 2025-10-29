from __future__ import annotations

import contextlib
from dataclasses import dataclass
from types import TracebackType
from typing import Any
from typing import List
from typing import Mapping
from typing import Optional
from typing import Sequence
from typing import Type


class _ConnCtx:
    def __init__(self, db: Any):
        self._db = db
        self._own_acquire = hasattr(db, "acquire") and callable(db.acquire)
        self._conn: Any | None = None

    async def __aenter__(self) -> Any:
        if self._own_acquire:
            self._conn = await self._db.acquire()
            return self._conn
        return self._db

    async def __aexit__(
        self, exc_type: Optional[Type[BaseException]], exc: Optional[BaseException], tb: Optional[TracebackType]
    ) -> bool:
        if self._own_acquire and self._conn is not None:
            with contextlib.suppress(Exception):
                await self._db.release(self._conn)
        return False


def _acquire(db: Any) -> _ConnCtx:
    return _ConnCtx(db)


@dataclass(frozen=True)
class BlobRow:
    kind: str
    object_id: str
    object_version: int
    fs_path: str
    size_bytes: int
    # Optional foreign keys and indices
    part_id: Optional[str] = None
    policy_version: Optional[int] = None
    chunk_index: Optional[int] = None
    stripe_index: Optional[int] = None
    parity_index: Optional[int] = None


class BlobsRepository:
    """Plain-SQL repository for blobs, part_ec, and related counters.

    Accepts either an asyncpg Pool or Connection.
    """

    def __init__(self, db: Any) -> None:
        self.db = db

    # blobs ---------------------------------------------------------------
    async def insert_many(self, rows: Sequence[BlobRow]) -> List[str]:
        if not rows:
            return []
        sql = """
            INSERT INTO blobs (
              kind, object_id, object_version,
              part_id, policy_version,
              chunk_index, stripe_index, parity_index,
              fs_path, size_bytes
            ) VALUES (
              $1, $2, $3,
              $4, $5,
              $6, $7, $8,
              $9, $10
            )
            RETURNING id
            """
        values = [
            (
                r.kind,
                r.object_id,
                int(r.object_version),
                r.part_id,
                r.policy_version,
                r.chunk_index,
                r.stripe_index,
                r.parity_index,
                r.fs_path,
                int(r.size_bytes),
            )
            for r in rows
        ]
        ids: List[str] = []
        async with _acquire(self.db) as conn:
            for v in values:
                row = await conn.fetchrow(sql, *v)
                ids.append(str(row[0]))
        return ids

    async def claim_staged_by_id(self, blob_id: str) -> Mapping[str, Any] | None:
        sql = """
            UPDATE blobs
            SET status = 'uploading'
            WHERE id = $1 AND status = 'staged'
            RETURNING *
            """
        async with _acquire(self.db) as conn:
            row = await conn.fetchrow(sql, blob_id)
            return row if row else None
        return None

    async def mark_uploaded(self, blob_id: str, cid: str) -> None:
        sql = "UPDATE blobs SET cid=$2, status='uploaded' WHERE id=$1"
        async with _acquire(self.db) as conn:
            await conn.execute(sql, blob_id, cid)

    async def claim_uploaded_by_id(self, blob_id: str) -> Mapping[str, Any] | None:
        sql = """
            UPDATE blobs
            SET status = 'pinning'
            WHERE id = $1 AND status = 'uploaded'
            RETURNING *
            """
        async with _acquire(self.db) as conn:
            row = await conn.fetchrow(sql, blob_id)
            return row if row else None
        return None

    async def mark_pinned(self, blob_id: str) -> None:
        sql = "UPDATE blobs SET status='pinned' WHERE id=$1"
        async with _acquire(self.db) as conn:
            await conn.execute(sql, blob_id)

    async def mark_failed(self, blob_id: str, error: str) -> None:
        sql = "UPDATE blobs SET status='failed', last_error=$2, last_error_at=now() WHERE id=$1"
        async with _acquire(self.db) as conn:
            await conn.execute(sql, blob_id, error)

    async def mark_pinned_by_cids(self, cids: Sequence[str]) -> int:
        if not cids:
            return 0
        sql = "UPDATE blobs SET status='pinned' WHERE cid = ANY($1)"
        async with _acquire(self.db) as conn:
            res = await conn.execute(sql, list(cids))
            # asyncpg returns 'UPDATE <n>'
            try:
                return int(str(res).split(" ")[-1])
            except Exception:
                return 0
        return 0

    async def get_part_refs_for_cids(self, cids: Sequence[str]) -> list[tuple[str, int, str]]:
        if not cids:
            return []
        sql = "SELECT DISTINCT part_id, policy_version, kind FROM blobs WHERE cid = ANY($1) AND part_id IS NOT NULL"
        async with _acquire(self.db) as conn:
            rows = await conn.fetch(sql, list(cids))
            out: list[tuple[str, int, str]] = []
            for r in rows or []:
                pid = str(r[0])
                pv = int(r[1]) if r[1] is not None else 0
                kd = str(r[2])
                out.append((pid, pv, kd))
            return out
        return []

    async def pinned_count(self, part_id: str, policy_version: int, kind: str) -> int:
        sql = """
            SELECT COUNT(*)
            FROM blobs
            WHERE part_id=$1 AND policy_version=$2 AND kind=$3 AND status='pinned'
            """
        async with _acquire(self.db) as conn:
            row = await conn.fetchrow(sql, part_id, int(policy_version), kind)
            return int(row[0]) if row else 0
        return 0

    async def expected_counts(self, part_id: str, policy_version: int, kind: str) -> int:
        # For replica: expected = num_chunks * m (m stored in part_ec as replicas per chunk)
        # For parity: expected = stripes * m
        if kind == "replica":
            sql_num_chunks = "SELECT COUNT(*) FROM part_chunks WHERE part_id = $1"
            sql_m = "SELECT m FROM part_ec WHERE part_id=$1 AND policy_version=$2"
            async with _acquire(self.db) as conn:
                row_n = await conn.fetchrow(sql_num_chunks, part_id)
                row_m = await conn.fetchrow(sql_m, part_id, int(policy_version))
                n = int(row_n[0]) if row_n else 0
                m = int(row_m[0]) if row_m and row_m[0] is not None else 0
                return n * m
        # parity path
        sql = "SELECT stripes, m FROM part_ec WHERE part_id=$1 AND policy_version=$2"
        async with _acquire(self.db) as conn:
            row = await conn.fetchrow(sql, part_id, int(policy_version))
            if not row:
                return 0
            stripes = int(row[0])
            m = int(row[1])
            return stripes * m
        return 0

    # part_ec -------------------------------------------------------------
    async def upsert_part_ec(
        self,
        *,
        part_id: str,
        policy_version: int,
        scheme: str,
        k: int,
        m: int,
        shard_size_bytes: int,
        stripes: int,
        state: str,
    ) -> None:
        sql = """
            INSERT INTO part_ec (part_id, policy_version, scheme, k, m, shard_size_bytes, stripes, state)
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
            ON CONFLICT (part_id, policy_version)
            DO UPDATE SET scheme=EXCLUDED.scheme, k=EXCLUDED.k, m=EXCLUDED.m,
                          shard_size_bytes=EXCLUDED.shard_size_bytes, stripes=EXCLUDED.stripes,
                          state=EXCLUDED.state, updated_at=now()
            """
        async with _acquire(self.db) as conn:
            await conn.execute(
                sql,
                part_id,
                int(policy_version),
                scheme,
                int(k),
                int(m),
                int(shard_size_bytes),
                int(stripes),
                state,
            )

    async def maybe_mark_complete(self, part_id: str, policy_version: int, kind: str) -> bool:
        expected = await self.expected_counts(part_id, int(policy_version), kind)
        pinned = await self.pinned_count(part_id, int(policy_version), kind)
        if expected and pinned >= expected:
            sql = "UPDATE part_ec SET state='complete', updated_at=now() WHERE part_id=$1 AND policy_version=$2"
            async with _acquire(self.db) as conn:
                await conn.execute(sql, part_id, int(policy_version))
            return True
        return False
