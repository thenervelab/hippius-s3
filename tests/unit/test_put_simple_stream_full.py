import asyncio
import uuid
from typing import Any
from typing import AsyncIterator

import pytest

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.config import get_config
from hippius_s3.writer.object_writer import ObjectWriter
from tests.unit._fake_pool import make_fake_pool


class DummyRedis:
    async def delete(self, *_a: Any, **_k: Any) -> int:
        return 1

    async def setex(self, *_a: Any, **_k: Any) -> None:
        return None

    async def set(self, *_a: Any, **_k: Any) -> None:
        return None


async def _body(*pieces: bytes) -> AsyncIterator[bytes]:
    for p in pieces:
        yield p


def _events_on(pool: Any, conn_id: Any, method: str) -> list[dict]:
    return [e for e in pool.events if e.get("conn") == conn_id and e.get("method") == method]


@pytest.fixture
def patched_writer(tmp_path: Any, monkeypatch: Any):
    """ObjectWriter wired to a FakePool, with KEK/DB helpers patched.

    Returns (writer, pool, captured) where captured records the connection each
    DB helper received and whether it was inside a transaction at call time.
    """
    cfg = get_config()
    monkeypatch.setattr("hippius_s3.writer.object_writer.get_config", lambda: cfg)

    captured: dict[str, Any] = {}

    # KEK lookup must run BEFORE we acquire a main-pool connection (separate keystore pool / KMS).
    async def fake_kek(*, bucket_id: str) -> tuple[str, bytes]:
        captured["kek_acquire_count_at_call"] = pool.acquire_count
        return ("kek-1", b"\x00" * 32)

    monkeypatch.setattr(
        "hippius_s3.services.kek_service.get_or_create_active_bucket_kek",
        fake_kek,
    )

    async def fake_upsert(db: Any, **kw: Any) -> dict:
        captured["head_conn"] = db
        captured["head_in_txn"] = getattr(db, "in_transaction", None)
        return {"object_id": kw["object_id"], "current_object_version": 1}

    async def fake_ensure(db: Any, **kw: Any) -> str:
        captured["tail_conn_ensure"] = db
        captured["tail_ensure_in_txn"] = getattr(db, "in_transaction", None)
        return str(uuid.uuid4())

    async def fake_parts(db: Any, **kw: Any) -> None:
        captured["tail_conn_parts"] = db
        captured["tail_parts_in_txn"] = getattr(db, "in_transaction", None)
        return None

    monkeypatch.setattr("hippius_s3.writer.object_writer.upsert_object_basic", fake_upsert)
    monkeypatch.setattr("hippius_s3.writer.object_writer.ensure_upload_row", fake_ensure)
    monkeypatch.setattr("hippius_s3.writer.object_writer.upsert_part_placeholder", fake_parts)

    pool = make_fake_pool()
    fs_store = FileSystemPartsStore(str(tmp_path))
    writer = ObjectWriter(pool=pool, redis_client=DummyRedis(), fs_store=fs_store)
    return writer, pool, captured


async def _run(writer: ObjectWriter, *pieces: bytes) -> Any:
    return await writer.put_simple_stream_full(
        bucket_id=str(uuid.uuid4()),
        bucket_name="bkt",
        object_id=str(uuid.uuid4()),
        object_key="k/obj.json",
        account_address="acct",
        content_type="application/json",
        metadata={},
        body_iter=_body(*pieces),
    )


@pytest.mark.asyncio
async def test_two_acquires_total(patched_writer: Any) -> None:
    """Regression guard for the whole refactor: the writer issues exactly 2 main-pool
    acquires (head + tail), down from ~7 acquire/reset cycles."""
    writer, pool, _ = patched_writer
    res = await _run(writer, b"hello world")
    assert res.size_bytes == len(b"hello world")
    assert pool.acquire_count == 2


@pytest.mark.asyncio
async def test_kek_lookup_before_acquire(patched_writer: Any) -> None:
    writer, pool, captured = patched_writer
    await _run(writer, b"x" * 10)
    # KEK was resolved while zero main-pool connections had been acquired.
    assert captured["kek_acquire_count_at_call"] == 0


@pytest.mark.asyncio
async def test_head_scope_single_transaction(patched_writer: Any) -> None:
    """reserve (upsert) + envelope UPDATE run on the SAME connection, inside a transaction."""
    writer, pool, captured = patched_writer
    await _run(writer, b"data")

    head_conn = captured["head_conn"]
    assert captured["head_in_txn"] is True, "upsert ran outside a transaction"

    envelope_events = [e for e in _events_on(pool, head_conn.conn_id, "execute") if "wrapped_dek" in e["query"]]
    assert len(envelope_events) == 1, "envelope UPDATE not issued on the head connection"
    assert envelope_events[0]["in_txn"] is True


@pytest.mark.asyncio
async def test_tail_scope_single_transaction_with_is_completed(patched_writer: Any) -> None:
    """metadata + ensure_upload + part_placeholder + is_completed run on ONE tail connection,
    inside a transaction, and the tail connection differs from the head connection."""
    writer, pool, captured = patched_writer
    await _run(writer, b"payload")

    tail_conn = captured["tail_conn_ensure"]
    assert tail_conn is captured["tail_conn_parts"], "ensure_upload and part_placeholder used different conns"
    assert captured["tail_ensure_in_txn"] is True
    assert captured["tail_parts_in_txn"] is True
    assert tail_conn.conn_id != captured["head_conn"].conn_id

    tail_executes = _events_on(pool, tail_conn.conn_id, "execute")
    assert any("is_completed" in e["query"] and e["in_txn"] for e in tail_executes), "is_completed not in tail txn"
    # All tail writes are inside the transaction.
    assert all(e["in_txn"] for e in tail_executes)


@pytest.mark.asyncio
async def test_tail_commits_before_return(patched_writer: Any) -> None:
    """The tail transaction must commit (txn_exit) before put_simple_stream_full returns, so the
    endpoint's subsequent enqueue happens only after the metadata/part rows are durable."""
    writer, pool, captured = patched_writer
    await _run(writer, b"payload")
    tail_conn_id = captured["tail_conn_ensure"].conn_id
    txn_exits = [i for i, e in enumerate(pool.events) if e["method"] == "txn_exit" and e["conn"] == tail_conn_id]
    assert txn_exits, "tail transaction never committed"


@pytest.mark.asyncio
async def test_no_direct_pool_writes_for_object(patched_writer: Any) -> None:
    """All writer DB writes go through acquired connections, not direct pool.execute."""
    writer, pool, _ = patched_writer
    await _run(writer, b"payload")
    direct = [e for e in pool.events if e.get("conn") == "pool" and e["method"] in {"execute", "fetchrow", "fetchval"}]
    assert direct == []


@pytest.mark.asyncio
async def test_acquire_timeout_propagates(patched_writer: Any) -> None:
    """If the head acquire times out, the writer surfaces asyncio.TimeoutError (it must NOT be
    swallowed) so the global handler can map it to a 503 SlowDown."""
    writer, pool, _ = patched_writer

    def _boom(*, timeout: float | None = None) -> Any:
        raise asyncio.TimeoutError()

    pool.acquire = _boom  # type: ignore[assignment]

    with pytest.raises(asyncio.TimeoutError):
        await _run(writer, b"payload")
