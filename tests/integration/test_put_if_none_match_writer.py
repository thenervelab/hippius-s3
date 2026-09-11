"""The real PUT writer with If-None-Match: * against real Postgres.

test_conditional_write_sql.py pins each query; this drives ObjectWriter.put_simple_stream_full end to
end — real reserve/tail transactions, real row locks, real FS chunk store — with only the bucket KEK
lookup stubbed (it lives in a separate keystore/KMS).
"""

from __future__ import annotations

import asyncio
import hashlib
import os
import uuid
from typing import Any
from typing import AsyncGenerator
from typing import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.utils import get_query
from hippius_s3.writer.object_writer import ObjectWriter
from hippius_s3.writer.types import PreconditionFailed


pytestmark = pytest.mark.asyncio

_DB_URL = os.getenv("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius?sslmode=disable")


class _NoRedis:
    async def delete(self, *_a: Any, **_k: Any) -> int:
        return 0

    async def setex(self, *_a: Any, **_k: Any) -> None:
        return None

    async def set(self, *_a: Any, **_k: Any) -> None:
        return None


@pytest_asyncio.fixture
async def env(tmp_path: Any, monkeypatch: Any) -> AsyncGenerator[dict[str, Any], None]:
    try:
        pool = await asyncpg.create_pool(_DB_URL, min_size=1, max_size=20)
    except OSError as exc:  # only an unreachable server is a legitimate skip
        pytest.skip(f"postgres unavailable: {exc}")

    async def fake_kek(*, bucket_id: str) -> tuple[Any, bytes]:
        return uuid.uuid4(), b"\x01" * 32

    monkeypatch.setattr("hippius_s3.services.kek_service.get_or_create_active_bucket_kek", fake_kek)

    account = f"5INMW{uuid.uuid4().hex[:12]}"
    bucket_id = uuid.uuid4()
    bucket_name = f"inmw-{uuid.uuid4().hex[:10]}"
    await pool.execute("INSERT INTO users (main_account_id, created_at) VALUES ($1, now())", account)
    await pool.execute(
        "INSERT INTO buckets (bucket_id, bucket_name, main_account_id, created_at) VALUES ($1, $2, $3, now())",
        bucket_id,
        bucket_name,
        account,
    )
    writer = ObjectWriter(pool=pool, redis_client=_NoRedis(), fs_store=FileSystemPartsStore(str(tmp_path)))
    try:
        yield {"pool": pool, "writer": writer, "bucket_id": str(bucket_id), "bucket_name": bucket_name}
    finally:
        async with pool.acquire() as conn, conn.transaction():
            rows = await conn.fetch("SELECT object_id FROM objects WHERE bucket_id = $1", bucket_id)
            ids = [str(r["object_id"]) for r in rows]
            # Compared as text: fs_cache_inventory keys object_id as text, the other tables as uuid.
            await conn.execute("DELETE FROM fs_cache_inventory WHERE object_id::text = ANY($1::text[])", ids)
            await conn.execute("DELETE FROM parts WHERE object_id::text = ANY($1::text[])", ids)
            await conn.execute("DELETE FROM multipart_uploads WHERE bucket_id = $1", bucket_id)
            await conn.execute("DELETE FROM objects WHERE bucket_id = $1", bucket_id)
            await conn.execute("DELETE FROM object_versions WHERE object_id::text = ANY($1::text[])", ids)
            await conn.execute("DELETE FROM buckets WHERE bucket_id = $1", bucket_id)
            await conn.execute("DELETE FROM users WHERE main_account_id = $1", account)
        await pool.close()


async def _body(data: bytes, gate: asyncio.Event | None = None) -> AsyncIterator[bytes]:
    if gate is not None:
        await gate.wait()
    yield data


async def _put(
    env: dict[str, Any], key: str, data: bytes, *, create_only: bool, gate: asyncio.Event | None = None
) -> Any:
    return await env["writer"].put_simple_stream_full(
        bucket_id=env["bucket_id"],
        bucket_name=env["bucket_name"],
        object_id=str(uuid.uuid4()),
        object_key=key,
        account_address="acct",
        content_type="application/octet-stream",
        metadata={},
        body_iter=_body(data, gate),
        if_none_match=create_only,
    )


async def _served_md5(env: dict[str, Any], key: str) -> str | None:
    """What HEAD would report for the key right now."""
    row = await env["pool"].fetchrow(get_query("get_object_head_by_path"), env["bucket_name"], key)
    return None if row is None else str(row["md5_hash"])


async def test_second_create_only_put_is_refused_and_the_first_is_kept(env: dict[str, Any]) -> None:
    await _put(env, "audit.log", b"first", create_only=True)
    with pytest.raises(PreconditionFailed):
        await _put(env, "audit.log", b"second", create_only=True)
    assert await _served_md5(env, "audit.log") == hashlib.md5(b"first").hexdigest()


async def test_create_only_put_over_an_unconditional_object_is_refused(env: dict[str, Any]) -> None:
    await _put(env, "k", b"plain", create_only=False)
    with pytest.raises(PreconditionFailed):
        await _put(env, "k", b"new", create_only=True)
    assert await _served_md5(env, "k") == hashlib.md5(b"plain").hexdigest()


async def test_create_only_put_after_a_delete_succeeds(env: dict[str, Any]) -> None:
    await _put(env, "k", b"old", create_only=False)
    await env["pool"].execute(get_query("soft_delete_object"), uuid.UUID(env["bucket_id"]), "k")

    await _put(env, "k", b"new", create_only=True)

    assert await _served_md5(env, "k") == hashlib.md5(b"new").hexdigest()


async def test_unconditional_put_still_overwrites(env: dict[str, Any]) -> None:
    await _put(env, "k", b"one", create_only=True)
    await _put(env, "k", b"two", create_only=False)
    assert await _served_md5(env, "k") == hashlib.md5(b"two").hexdigest()


async def test_concurrent_create_only_writers_have_exactly_one_winner(env: dict[str, Any]) -> None:
    """All eight reserve before any of them streams (the gate), so none is refused at reserve and
    the outcome rests entirely on the finalize-time re-check under the exclusive row lock."""
    writers = 8
    gate = asyncio.Event()
    bodies = [f"writer-{i}".encode() * 100 for i in range(writers)]
    tasks = [asyncio.create_task(_put(env, "race", body, create_only=True, gate=gate)) for body in bodies]

    # Wait until every writer has reserved its version (all placeholders present), then release them.
    for _ in range(200):
        reserved = await env["pool"].fetchval(
            "SELECT count(*) FROM object_versions v JOIN objects o ON o.object_id = v.object_id "
            "WHERE o.bucket_id = $1 AND o.object_key = 'race'",
            uuid.UUID(env["bucket_id"]),
        )
        if reserved == writers:
            break
        await asyncio.sleep(0.05)
    assert reserved == writers, "every writer must pass the reserve-time check before any finalizes"
    gate.set()

    results = await asyncio.gather(*tasks, return_exceptions=True)

    winners = [i for i, r in enumerate(results) if not isinstance(r, BaseException)]
    losers = [r for r in results if isinstance(r, BaseException)]
    assert len(winners) == 1, f"expected exactly one winner, got {results}"
    assert all(isinstance(r, PreconditionFailed) for r in losers), losers
    assert await _served_md5(env, "race") == hashlib.md5(bodies[winners[0]]).hexdigest()
