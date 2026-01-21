from __future__ import annotations

import asyncio
import os
import time
import uuid
from typing import Optional

import asyncpg  # type: ignore[import-untyped]

from hippius_s3.config import get_config


KEK_SIZE_BYTES = 32

_POOL: asyncpg.Pool | None = None  # type: ignore[name-defined]
_POOL_DSN: str | None = None
_POOL_LOCK = asyncio.Lock()

_KEK_CACHE: dict[tuple[str, str], tuple[bytes, float]] = {}
_KEK_CACHE_LOCK = asyncio.Lock()


async def _get_pool(dsn: str) -> asyncpg.Pool:  # type: ignore[name-defined]
    global _POOL, _POOL_DSN
    if _POOL is not None and dsn == _POOL_DSN:
        return _POOL
    async with _POOL_LOCK:
        if _POOL is not None and dsn == _POOL_DSN:
            return _POOL
        cfg = get_config()
        _POOL = await asyncpg.create_pool(
            dsn,  # type: ignore[arg-type]
            min_size=int(cfg.kek_db_pool_min_size),
            max_size=int(cfg.kek_db_pool_max_size),
        )
        _POOL_DSN = dsn
        return _POOL


async def _get_cached_kek(bucket_id: str, kek_id: uuid.UUID) -> bytes | None:
    cfg = get_config()
    ttl = int(cfg.kek_cache_ttl_seconds)
    if ttl <= 0:
        return None
    key = (str(bucket_id), str(kek_id))
    now = time.monotonic()
    async with _KEK_CACHE_LOCK:
        entry = _KEK_CACHE.get(key)
        if not entry:
            return None
        kek_bytes, expires_at = entry
        if expires_at <= now:
            _KEK_CACHE.pop(key, None)
            return None
        return kek_bytes


async def _set_cached_kek(bucket_id: str, kek_id: uuid.UUID, kek_bytes: bytes) -> None:
    cfg = get_config()
    ttl = int(cfg.kek_cache_ttl_seconds)
    if ttl <= 0:
        return
    key = (str(bucket_id), str(kek_id))
    expires_at = time.monotonic() + ttl
    async with _KEK_CACHE_LOCK:
        _KEK_CACHE[key] = (bytes(kek_bytes), expires_at)


async def get_or_create_active_bucket_kek(
    *,
    bucket_id: str,
) -> tuple[uuid.UUID, bytes]:
    """Return (kek_id, kek_bytes) for the active KEK of a bucket, creating one if missing.

    KEKs live in the keystore DB (Config.encryption_database_url).
    """
    cfg = get_config()
    dsn: Optional[str] = getattr(cfg, "encryption_database_url", None)
    if not dsn:
        raise RuntimeError("kek_database_unavailable")

    async def _ensure_tables(conn: asyncpg.Connection) -> None:  # type: ignore[name-defined]
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bucket_keks (
                bucket_id UUID NOT NULL,
                kek_id UUID PRIMARY KEY,
                kek_bytes BYTEA NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            );
            CREATE INDEX IF NOT EXISTS idx_bucket_keks_bucket_status_created
              ON bucket_keks(bucket_id, status, created_at DESC);
            CREATE UNIQUE INDEX IF NOT EXISTS uniq_bucket_active_kek
              ON bucket_keks(bucket_id)
             WHERE status = 'active';
            """
        )

    pool = await _get_pool(dsn)
    async with pool.acquire() as conn:

        async def _fetch_active() -> asyncpg.Record | None:  # type: ignore[name-defined]
            return await conn.fetchrow(
                """
                SELECT kek_id, kek_bytes
                  FROM bucket_keks
                 WHERE bucket_id = $1
                   AND status = 'active'
                 ORDER BY created_at DESC
                 LIMIT 1
                """,
                uuid.UUID(str(bucket_id)),
            )

        try:
            row = await _fetch_active()
        except Exception as e:
            if isinstance(e, Exception) and getattr(e, "sqlstate", "") == "42P01":
                await _ensure_tables(conn)
                row = await _fetch_active()
            else:
                raise

        if row and row.get("kek_id") and row.get("kek_bytes") is not None:
            kek_id = uuid.UUID(str(row["kek_id"]))
            kek_bytes = bytes(row["kek_bytes"])
            await _set_cached_kek(bucket_id, kek_id, kek_bytes)
            return kek_id, kek_bytes

        kek_id = uuid.uuid4()
        kek_bytes = os.urandom(KEK_SIZE_BYTES)

        try:
            await conn.execute(
                """
                INSERT INTO bucket_keks (bucket_id, kek_id, kek_bytes, status)
                VALUES ($1, $2, $3, 'active')
                """,
                uuid.UUID(str(bucket_id)),
                kek_id,
                kek_bytes,
            )
        except Exception as e:
            sqlstate = getattr(e, "sqlstate", "")
            if isinstance(e, Exception) and sqlstate == "42P01":
                await _ensure_tables(conn)
                try:
                    await conn.execute(
                        """
                        INSERT INTO bucket_keks (bucket_id, kek_id, kek_bytes, status)
                        VALUES ($1, $2, $3, 'active')
                        """,
                        uuid.UUID(str(bucket_id)),
                        kek_id,
                        kek_bytes,
                    )
                except Exception as insert_err:
                    if getattr(insert_err, "sqlstate", "") == "23505":
                        row = await _fetch_active()
                        if row and row.get("kek_id") and row.get("kek_bytes") is not None:
                            kek_id = uuid.UUID(str(row["kek_id"]))
                            kek_bytes = bytes(row["kek_bytes"])
                            await _set_cached_kek(bucket_id, kek_id, kek_bytes)
                            return kek_id, kek_bytes
                    raise
            elif isinstance(e, Exception) and sqlstate == "23505":
                row = await _fetch_active()
                if row and row.get("kek_id") and row.get("kek_bytes") is not None:
                    kek_id = uuid.UUID(str(row["kek_id"]))
                    kek_bytes = bytes(row["kek_bytes"])
                    await _set_cached_kek(bucket_id, kek_id, kek_bytes)
                    return kek_id, kek_bytes
                raise
            else:
                raise

        await _set_cached_kek(bucket_id, kek_id, kek_bytes)
        return kek_id, kek_bytes


async def get_bucket_kek_bytes(*, bucket_id: str, kek_id: uuid.UUID) -> bytes:
    """Fetch a specific KEK by id for a bucket (used for decrypting old versions after rotation)."""
    cached = await _get_cached_kek(bucket_id, kek_id)
    if cached is not None:
        return cached

    cfg = get_config()
    dsn: Optional[str] = getattr(cfg, "encryption_database_url", None)
    if not dsn:
        raise RuntimeError("kek_database_unavailable")

    pool = await _get_pool(dsn)
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            """
            SELECT kek_bytes
              FROM bucket_keks
             WHERE bucket_id = $1
               AND kek_id = $2
             LIMIT 1
            """,
            uuid.UUID(str(bucket_id)),
            uuid.UUID(str(kek_id)),
        )
        if not row or row.get("kek_bytes") is None:
            raise RuntimeError("kek_not_found")
        kek_bytes = bytes(row["kek_bytes"])
        await _set_cached_kek(bucket_id, kek_id, kek_bytes)
        return kek_bytes


async def close_kek_pool() -> None:
    global _POOL, _POOL_DSN
    async with _POOL_LOCK:
        if _POOL is not None:
            await _POOL.close()
            _POOL = None
            _POOL_DSN = None
    async with _KEK_CACHE_LOCK:
        _KEK_CACHE.clear()
