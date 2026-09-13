"""The real PUT and append writers with Content-MD5 against real Postgres.

Drives ObjectWriter.put_simple_stream_full and ObjectWriter.append_stream end to end — real
reserve/tail transactions, real FS chunk store — with only the bucket KEK lookup stubbed (it lives in
a separate keystore/KMS), and checks what a HEAD would serve afterwards.
"""

from __future__ import annotations

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
from hippius_s3.writer.types import BadDigest


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
        pool = await asyncpg.create_pool(_DB_URL, min_size=1, max_size=5)
    except OSError as exc:  # only an unreachable server is a legitimate skip
        pytest.skip(f"postgres unavailable: {exc}")

    async def fake_kek(*, bucket_id: str) -> tuple[Any, bytes]:
        return uuid.uuid4(), b"\x01" * 32

    # The append path unwraps the DEK the PUT already wrapped, so it reads the KEK back BY ID
    # (get_bucket_kek_bytes) instead of creating one — same key bytes, or the unwrap fails.
    async def fake_kek_by_id(*, bucket_id: str, kek_id: Any) -> bytes:
        return b"\x01" * 32

    monkeypatch.setattr("hippius_s3.services.kek_service.get_or_create_active_bucket_kek", fake_kek)
    monkeypatch.setattr("hippius_s3.services.kek_service.get_bucket_kek_bytes", fake_kek_by_id)

    account = f"5MD5W{uuid.uuid4().hex[:12]}"
    bucket_id = uuid.uuid4()
    bucket_name = f"md5w-{uuid.uuid4().hex[:10]}"
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


async def _body(data: bytes) -> AsyncIterator[bytes]:
    yield data


async def _put(env: dict[str, Any], key: str, data: bytes, expected_md5: bytes | None = None) -> Any:
    return await env["writer"].put_simple_stream_full(
        bucket_id=env["bucket_id"],
        bucket_name=env["bucket_name"],
        object_id=str(uuid.uuid4()),
        object_key=key,
        account_address="acct",
        content_type="application/octet-stream",
        metadata={},
        body_iter=_body(data),
        expected_md5=expected_md5,
    )


async def _served(env: dict[str, Any], key: str) -> tuple[str, int] | None:
    """(md5, size) that HEAD would report for the key right now."""
    row = await env["pool"].fetchrow(get_query("get_object_head_by_path"), env["bucket_name"], key)
    return None if row is None else (str(row["md5_hash"]), int(row["size_bytes"]))


async def _append(
    env: dict[str, Any],
    key: str,
    data: bytes,
    expected_version: int,
    expected_md5: bytes | None = None,
) -> Any:
    return await env["writer"].append_stream(
        bucket_id=env["bucket_id"],
        bucket_name=env["bucket_name"],
        object_key=key,
        expected_version=expected_version,
        account_address="acct",
        body_iter=_body(data),
        expected_md5=expected_md5,
    )


async def _live_version(env: dict[str, Any], key: str) -> dict[str, int]:
    """Append version, size and part count of the version an append would extend."""
    row = await env["pool"].fetchrow(
        """
        SELECT v.append_version,
               v.size_bytes,
               (SELECT count(*) FROM parts p WHERE p.object_id = v.object_id AND p.object_version = v.object_version)
                   AS part_count
          FROM object_versions v
          JOIN objects o ON o.object_id = v.object_id AND o.current_object_version = v.object_version
         WHERE o.bucket_id = $1 AND o.object_key = $2
        """,
        uuid.UUID(env["bucket_id"]),
        key,
    )
    return {
        "append_version": int(row["append_version"]),
        "size_bytes": int(row["size_bytes"]),
        "part_count": int(row["part_count"]),
    }


async def test_matching_digest_is_stored(env: dict[str, Any]) -> None:
    await _put(env, "k", b"record", expected_md5=hashlib.md5(b"record").digest())
    assert await _served(env, "k") == (hashlib.md5(b"record").hexdigest(), len(b"record"))


async def test_bad_digest_on_a_new_key_leaves_nothing_served(env: dict[str, Any]) -> None:
    with pytest.raises(BadDigest):
        await _put(env, "k", b"actual", expected_md5=hashlib.md5(b"claimed").digest())
    served = await _served(env, "k")
    assert served is None or served[1] == 0 and served[0] == "", f"a rejected body became visible: {served}"


async def test_bad_digest_on_an_overwrite_keeps_the_previous_content(env: dict[str, Any]) -> None:
    await _put(env, "k", b"original")

    with pytest.raises(BadDigest):
        await _put(env, "k", b"replacement", expected_md5=hashlib.md5(b"something else").digest())

    assert await _served(env, "k") == (hashlib.md5(b"original").hexdigest(), len(b"original"))
    # The rejected attempt exists only as the inert placeholder a disconnected PUT leaves.
    rows = await env["pool"].fetch(
        "SELECT v.object_version, v.size_bytes, v.md5_hash FROM object_versions v JOIN objects o USING (object_id) "
        "WHERE o.bucket_id = $1 AND o.object_key = 'k' ORDER BY v.object_version",
        uuid.UUID(env["bucket_id"]),
    )
    assert [(r["size_bytes"], r["md5_hash"]) for r in rows] == [
        (len(b"original"), hashlib.md5(b"original").hexdigest()),
        (0, ""),
    ]


async def test_a_correct_retry_after_a_bad_digest_lands(env: dict[str, Any]) -> None:
    with pytest.raises(BadDigest):
        await _put(env, "k", b"payload", expected_md5=hashlib.md5(b"typo").digest())
    await _put(env, "k", b"payload", expected_md5=hashlib.md5(b"payload").digest())
    assert await _served(env, "k") == (hashlib.md5(b"payload").hexdigest(), len(b"payload"))


async def test_matching_digest_on_an_append_lands(env: dict[str, Any]) -> None:
    await _put(env, "k", b"base")
    before = await _live_version(env, "k")

    await _append(env, "k", b"-one", before["append_version"], expected_md5=hashlib.md5(b"-one").digest())

    after = await _live_version(env, "k")
    assert after["size_bytes"] == len(b"base-one")
    assert after["part_count"] == before["part_count"] + 1


async def test_bad_digest_on_an_append_leaves_the_live_version_untouched(env: dict[str, Any]) -> None:
    # Unlike a rejected PUT, which only ever abandons a version of its own, a rejected append is
    # refused against the version the key is CURRENTLY serving: a leaked reservation would land a
    # part row on live, servable data.
    await _put(env, "k", b"base")
    await _append(env, "k", b"-one", 0, expected_md5=hashlib.md5(b"-one").digest())
    before = await _live_version(env, "k")
    served = await _served(env, "k")

    with pytest.raises(BadDigest):
        await _append(env, "k", b"-two", before["append_version"], expected_md5=hashlib.md5(b"typo").digest())

    # No extra part row on the live version, CAS counter not moved, nothing added to the size.
    assert await _live_version(env, "k") == before
    assert await _served(env, "k") == served


async def test_a_correct_append_retry_after_a_bad_digest_lands(env: dict[str, Any]) -> None:
    await _put(env, "k", b"base")
    before = await _live_version(env, "k")

    with pytest.raises(BadDigest):
        await _append(env, "k", b"-delta", before["append_version"], expected_md5=hashlib.md5(b"typo").digest())

    # Same expected_version as the rejected attempt: the CAS counter must not have moved, and the
    # part number the rejection reserved must be free again.
    await _append(env, "k", b"-delta", before["append_version"], expected_md5=hashlib.md5(b"-delta").digest())

    after = await _live_version(env, "k")
    assert after["size_bytes"] == len(b"base-delta")
    assert after["part_count"] == before["part_count"] + 1
