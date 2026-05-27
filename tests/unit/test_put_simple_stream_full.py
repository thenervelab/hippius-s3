import asyncio
import collections
import uuid
from typing import Any
from typing import AsyncIterator

import pytest

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.config import get_config
from hippius_s3.db_pool import PoolAcquireTimeout
from hippius_s3.writer.object_writer import ObjectWriter
from tests.unit._fake_pool import make_fake_pool


class DummyRedis:
    async def delete(self, *_a: Any, **_k: Any) -> int:
        return 1

    async def setex(self, *_a: Any, **_k: Any) -> None:
        return None

    async def set(self, *_a: Any, **_k: Any) -> None:
        return None


class CountingFS(FileSystemPartsStore):
    """FileSystemPartsStore that records every set_chunk / set_meta call.

    Used to prove the upload path writes each chunk and each part's meta exactly
    once — the pre-2026-04-21 Redis mirror used to write them a second time.
    """

    def __init__(self, root: str) -> None:
        super().__init__(root)
        self.chunk_calls: list[tuple[int, int]] = []
        self.meta_calls: list[int] = []

    async def set_chunk(
        self, object_id: Any, object_version: int, part_number: int, chunk_index: int, data: bytes
    ) -> None:
        self.chunk_calls.append((int(part_number), int(chunk_index)))
        await super().set_chunk(object_id, object_version, part_number, chunk_index, data)

    async def set_meta(self, object_id: Any, object_version: int, part_number: int, **kwargs: Any) -> None:
        self.meta_calls.append(int(part_number))
        await super().set_meta(object_id, object_version, part_number, **kwargs)


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
async def test_tail_scope_single_transaction(patched_writer: Any) -> None:
    """metadata + ensure_upload + part_placeholder run on ONE tail connection, inside a
    transaction, on a connection distinct from the head."""
    writer, pool, captured = patched_writer
    await _run(writer, b"payload")

    tail_conn = captured["tail_conn_ensure"]
    assert tail_conn is captured["tail_conn_parts"], "ensure_upload and part_placeholder used different conns"
    assert captured["tail_ensure_in_txn"] is True
    assert captured["tail_parts_in_txn"] is True
    assert tail_conn.conn_id != captured["head_conn"].conn_id

    # All tail writes are inside the transaction.
    tail_executes = _events_on(pool, tail_conn.conn_id, "execute")
    assert all(e["in_txn"] for e in tail_executes)


@pytest.mark.asyncio
async def test_is_completed_not_set_by_writer(patched_writer: Any) -> None:
    """is_completed=TRUE must NOT be issued by the writer: the endpoint sets it only after a
    successful enqueue, so an enqueue failure leaves the row cleanable (not an orphan)."""
    writer, pool, _ = patched_writer
    await _run(writer, b"payload")
    assert not any("is_completed" in (e.get("query") or "") for e in pool.events), (
        "writer must not set is_completed; that belongs after enqueue in the endpoint"
    )


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
async def test_acquire_timeout_raises_pool_acquire_timeout(patched_writer: Any) -> None:
    """A head-acquire timeout surfaces as the dedicated PoolAcquireTimeout (translated by
    acquire_with_timeout), so it maps to 503 — and is distinguishable from a query command_timeout
    (a bare asyncio.TimeoutError raised by the body, which must NOT be relabeled)."""
    writer, pool, _ = patched_writer

    def _boom(*, timeout: float | None = None) -> Any:
        raise asyncio.TimeoutError()

    pool.acquire = _boom  # type: ignore[assignment]

    with pytest.raises(PoolAcquireTimeout):
        await _run(writer, b"payload")


@pytest.fixture
def counting_writer(tmp_path: Any, monkeypatch: Any):
    """Like patched_writer but with a CountingFS so we can assert no double writes."""
    cfg = get_config()
    monkeypatch.setattr("hippius_s3.writer.object_writer.get_config", lambda: cfg)

    async def fake_kek(*, bucket_id: str) -> tuple[str, bytes]:
        return ("kek-1", b"\x00" * 32)

    async def fake_upsert(db: Any, **kw: Any) -> dict:
        return {"object_id": kw["object_id"], "current_object_version": 1}

    async def fake_ensure(db: Any, **kw: Any) -> str:
        return str(uuid.uuid4())

    async def fake_parts(db: Any, **kw: Any) -> None:
        return None

    monkeypatch.setattr("hippius_s3.services.kek_service.get_or_create_active_bucket_kek", fake_kek)
    monkeypatch.setattr("hippius_s3.writer.object_writer.upsert_object_basic", fake_upsert)
    monkeypatch.setattr("hippius_s3.writer.object_writer.ensure_upload_row", fake_ensure)
    monkeypatch.setattr("hippius_s3.writer.object_writer.upsert_part_placeholder", fake_parts)

    pool = make_fake_pool()
    fs_store = CountingFS(str(tmp_path))
    writer = ObjectWriter(pool=pool, redis_client=DummyRedis(), fs_store=fs_store)
    return writer, fs_store


@pytest.mark.asyncio
async def test_each_chunk_written_to_fs_once(counting_writer: Any) -> None:
    """Regression for the double-FS-write: removing the obj_cache.set_chunks mirror means
    every chunk hits the FS store exactly once (the consumer's set_chunk is the sole write)."""
    writer, fs_store = counting_writer
    # Force >1 chunk so a per-chunk duplicate would be obvious.
    chunk_size = writer.config.object_chunk_size_bytes
    await _run(writer, b"a" * (chunk_size * 2 + 7))

    counts = collections.Counter(fs_store.chunk_calls)
    assert all(n == 1 for n in counts.values()), f"a chunk was written more than once: {counts}"
    # 3 distinct chunks (two full + remainder), each on part 1.
    assert sorted(fs_store.chunk_calls) == [(1, 0), (1, 1), (1, 2)]


@pytest.mark.asyncio
async def test_meta_written_to_fs_once(counting_writer: Any) -> None:
    """Regression for the double-meta-write: meta (the part-complete marker, with its
    file+dir fsync) is written exactly once per part, not twice."""
    writer, fs_store = counting_writer
    await _run(writer, b"hello world")
    assert fs_store.meta_calls == [1], f"meta written {len(fs_store.meta_calls)} times, expected once"
