from typing import Any

import pytest
import pytest_asyncio  # type: ignore[import-not-found]
import redis.asyncio as async_redis

from hippius_s3.dlq.pinner_dlq import PinnerDLQEntry
from hippius_s3.dlq.pinner_dlq import PinnerDLQManager


pytestmark = pytest.mark.unit


@pytest_asyncio.fixture
async def redis_client() -> Any:
    """Create a Redis client for testing."""
    client = async_redis.from_url("redis://localhost:6379/0")
    await client.delete("pinner:dlq")
    try:
        yield client
    finally:
        await client.delete("pinner:dlq")
        await client.close()


@pytest.mark.asyncio
async def test_pinner_dlq_entry_to_dict() -> None:
    """Test PinnerDLQEntry serialization."""
    entry = PinnerDLQEntry(
        cid="QmTest123",
        user="5User123",
        object_id="obj-123",
        object_version=1,
        reason="max_attempts",
        pin_attempts=3,
        last_pinned_at=1234567890.5,
        dlq_timestamp=1234567900.0,
    )

    data = entry.to_dict()

    assert data["cid"] == "QmTest123"
    assert data["user"] == "5User123"
    assert data["object_id"] == "obj-123"
    assert data["object_version"] == 1
    assert data["reason"] == "max_attempts"
    assert data["pin_attempts"] == 3
    assert data["last_pinned_at"] == 1234567890.5
    assert data["dlq_timestamp"] == 1234567900.0


@pytest.mark.asyncio
async def test_pinner_dlq_entry_from_dict() -> None:
    """Test PinnerDLQEntry deserialization."""
    data = {
        "cid": "QmTest456",
        "user": "5User456",
        "object_id": "obj-456",
        "object_version": 2,
        "reason": "validator_ignore",
        "pin_attempts": 5,
        "last_pinned_at": 9876543210.0,
        "dlq_timestamp": 9876543220.0,
    }

    entry = PinnerDLQEntry.from_dict(data)

    assert entry.cid == "QmTest456"
    assert entry.user == "5User456"
    assert entry.object_id == "obj-456"
    assert entry.object_version == 2
    assert entry.reason == "validator_ignore"
    assert entry.pin_attempts == 5
    assert entry.last_pinned_at == 9876543210.0
    assert entry.dlq_timestamp == 9876543220.0


@pytest.mark.asyncio
async def test_dlq_push_and_peek(redis_client: Any) -> None:
    """Test pushing and peeking DLQ entries."""
    dlq_manager = PinnerDLQManager(redis_client)

    entry1 = PinnerDLQEntry(
        cid="QmTest1",
        user="5User1",
        object_id="obj-1",
        object_version=1,
        reason="max_attempts",
        pin_attempts=3,
    )

    entry2 = PinnerDLQEntry(
        cid="QmTest2",
        user="5User2",
        object_id="obj-2",
        object_version=1,
        reason="max_attempts",
        pin_attempts=3,
    )

    await dlq_manager.push(entry1)
    await dlq_manager.push(entry2)

    entries = await dlq_manager.peek(limit=10)

    assert len(entries) == 2
    assert entries[0].cid == "QmTest2"
    assert entries[1].cid == "QmTest1"


@pytest.mark.asyncio
async def test_dlq_stats(redis_client: Any) -> None:
    """Test DLQ statistics."""
    dlq_manager = PinnerDLQManager(redis_client)

    stats = await dlq_manager.stats()
    assert stats["total_entries"] == 0

    entry = PinnerDLQEntry(
        cid="QmTestStats",
        user="5UserStats",
        object_id="obj-stats",
        object_version=1,
        reason="max_attempts",
        pin_attempts=3,
    )
    await dlq_manager.push(entry)

    stats = await dlq_manager.stats()
    assert stats["total_entries"] == 1
    assert stats["queue_key"] == "pinner:dlq"


@pytest.mark.asyncio
async def test_dlq_find_and_remove(redis_client: Any) -> None:
    """Test finding and removing a specific CID entry."""
    dlq_manager = PinnerDLQManager(redis_client)

    entry1 = PinnerDLQEntry(
        cid="QmFindMe",
        user="5UserFind",
        object_id="obj-find",
        object_version=1,
        reason="max_attempts",
        pin_attempts=3,
    )

    entry2 = PinnerDLQEntry(
        cid="QmKeepMe",
        user="5UserKeep",
        object_id="obj-keep",
        object_version=1,
        reason="max_attempts",
        pin_attempts=3,
    )

    await dlq_manager.push(entry1)
    await dlq_manager.push(entry2)

    removed = await dlq_manager.find_and_remove("QmFindMe")
    assert removed is not None
    assert removed.cid == "QmFindMe"

    entries = await dlq_manager.peek(limit=10)
    assert len(entries) == 1
    assert entries[0].cid == "QmKeepMe"


@pytest.mark.asyncio
async def test_dlq_purge_single(redis_client: Any) -> None:
    """Test purging a single entry."""
    dlq_manager = PinnerDLQManager(redis_client)

    entry1 = PinnerDLQEntry(
        cid="QmPurge1",
        user="5UserPurge",
        object_id="obj-purge",
        object_version=1,
        reason="max_attempts",
        pin_attempts=3,
    )

    entry2 = PinnerDLQEntry(
        cid="QmKeep1",
        user="5UserKeep",
        object_id="obj-keep",
        object_version=1,
        reason="max_attempts",
        pin_attempts=3,
    )

    await dlq_manager.push(entry1)
    await dlq_manager.push(entry2)

    count = await dlq_manager.purge(cid="QmPurge1")
    assert count == 1

    entries = await dlq_manager.peek(limit=10)
    assert len(entries) == 1
    assert entries[0].cid == "QmKeep1"


@pytest.mark.asyncio
async def test_dlq_purge_all(redis_client: Any) -> None:
    """Test purging all entries."""
    dlq_manager = PinnerDLQManager(redis_client)

    for i in range(5):
        entry = PinnerDLQEntry(
            cid=f"QmPurgeAll{i}",
            user=f"5UserPurge{i}",
            object_id=f"obj-{i}",
            object_version=1,
            reason="max_attempts",
            pin_attempts=3,
        )
        await dlq_manager.push(entry)

    count = await dlq_manager.purge()
    assert count == 5

    entries = await dlq_manager.peek(limit=10)
    assert len(entries) == 0


@pytest.mark.asyncio
async def test_dlq_export_all(redis_client: Any) -> None:
    """Test exporting all DLQ entries."""
    dlq_manager = PinnerDLQManager(redis_client)

    for i in range(3):
        entry = PinnerDLQEntry(
            cid=f"QmExport{i}",
            user=f"5UserExport{i}",
            object_id=f"obj-{i}",
            object_version=1,
            reason="max_attempts",
            pin_attempts=3,
        )
        await dlq_manager.push(entry)

    entries = await dlq_manager.export_all()
    assert len(entries) == 3
    assert all(isinstance(e, PinnerDLQEntry) for e in entries)
    cids = {e.cid for e in entries}
    assert "QmExport0" in cids
    assert "QmExport1" in cids
    assert "QmExport2" in cids
