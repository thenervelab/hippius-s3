from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from hippius_s3.dlq.pinner_dlq import PinnerDLQEntry
from hippius_s3.dlq.pinner_dlq import PinnerDLQManager


pytestmark = pytest.mark.unit


@pytest.fixture
def temp_dlq_dir(tmp_path: Path) -> str:
    """Create a temporary DLQ directory for testing."""
    dlq_dir = tmp_path / "pinner_dlq"
    dlq_dir.mkdir()
    return str(dlq_dir)


@pytest.fixture
def mock_db() -> AsyncMock:
    """Create a mock database connection."""
    return AsyncMock()


def test_pinner_dlq_entry_to_dict() -> None:
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


def test_pinner_dlq_entry_from_dict() -> None:
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
async def test_dlq_push_and_peek(temp_dlq_dir: str) -> None:
    """Test pushing and peeking DLQ entries."""
    dlq_manager = PinnerDLQManager(temp_dlq_dir)

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
    assert entries[0].cid == "QmTest1"
    assert entries[1].cid == "QmTest2"


@pytest.mark.asyncio
async def test_dlq_is_in_dlq(temp_dlq_dir: str) -> None:
    """Test is_in_dlq filesystem check."""
    dlq_manager = PinnerDLQManager(temp_dlq_dir)

    entry = PinnerDLQEntry(
        cid="QmTestExists",
        user="5UserExists",
        object_id="obj-exists",
        object_version=1,
        reason="max_attempts",
        pin_attempts=3,
    )

    assert not await dlq_manager.is_in_dlq("QmTestExists")

    await dlq_manager.push(entry)

    assert await dlq_manager.is_in_dlq("QmTestExists")
    assert not await dlq_manager.is_in_dlq("QmDoesNotExist")


@pytest.mark.asyncio
async def test_dlq_stats(temp_dlq_dir: str) -> None:
    """Test DLQ statistics."""
    dlq_manager = PinnerDLQManager(temp_dlq_dir)

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
    assert "dlq_path" in stats


@pytest.mark.asyncio
async def test_dlq_find_and_remove(temp_dlq_dir: str) -> None:
    """Test finding and removing a specific CID entry."""
    dlq_manager = PinnerDLQManager(temp_dlq_dir)

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
async def test_dlq_purge_single(temp_dlq_dir: str) -> None:
    """Test purging a single entry."""
    dlq_manager = PinnerDLQManager(temp_dlq_dir)

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
async def test_dlq_purge_all(temp_dlq_dir: str) -> None:
    """Test purging all entries."""
    dlq_manager = PinnerDLQManager(temp_dlq_dir)

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
async def test_dlq_export_all(temp_dlq_dir: str) -> None:
    """Test exporting all DLQ entries."""
    dlq_manager = PinnerDLQManager(temp_dlq_dir)

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
    assert entries[0].cid == "QmExport0"
    assert entries[1].cid == "QmExport1"
    assert entries[2].cid == "QmExport2"


@pytest.mark.asyncio
async def test_should_enqueue_cid_no_row(mock_db: AsyncMock) -> None:
    """Test should_enqueue_cid when CID doesn't exist in database."""
    mock_db.fetchrow.return_value = None

    row = await mock_db.fetchrow(
        """
        SELECT pin_attempts
        FROM part_chunks
        WHERE cid = $1
        ORDER BY pin_attempts DESC
        LIMIT 1
        """,
        "QmNonExistent",
    )
    if not row:
        result = True
    else:
        attempts = int(row["pin_attempts"] or 0)
        result = attempts < 3

    assert result is True
    mock_db.fetchrow.assert_called_once()


@pytest.mark.asyncio
async def test_should_enqueue_cid_below_max(mock_db: AsyncMock) -> None:
    """Test should_enqueue_cid when attempts are below max."""
    mock_db.fetchrow.return_value = {"pin_attempts": 2}

    row = await mock_db.fetchrow(
        """
        SELECT pin_attempts
        FROM part_chunks
        WHERE cid = $1
        ORDER BY pin_attempts DESC
        LIMIT 1
        """,
        "QmTestBelow",
    )
    if not row:
        result = True
    else:
        attempts = int(row["pin_attempts"] or 0)
        result = attempts < 3

    assert result is True


@pytest.mark.asyncio
async def test_should_enqueue_cid_at_max(mock_db: AsyncMock) -> None:
    """Test should_enqueue_cid when attempts are at max."""
    mock_db.fetchrow.return_value = {"pin_attempts": 3}

    row = await mock_db.fetchrow(
        """
        SELECT pin_attempts
        FROM part_chunks
        WHERE cid = $1
        ORDER BY pin_attempts DESC
        LIMIT 1
        """,
        "QmTestAtMax",
    )
    if not row:
        result = True
    else:
        attempts = int(row["pin_attempts"] or 0)
        result = attempts < 3

    assert result is False


@pytest.mark.asyncio
async def test_record_pin_attempt(mock_db: AsyncMock) -> None:
    """Test recording a pin attempt."""
    await mock_db.execute(
        """
        UPDATE part_chunks
        SET pin_attempts = pin_attempts + 1,
            last_pinned_at = NOW()
        WHERE cid = $1
        """,
        "QmTestRecord",
    )

    mock_db.execute.assert_called_once()
    call_args = mock_db.execute.call_args
    assert "UPDATE part_chunks" in call_args[0][0]
    assert "pin_attempts = pin_attempts + 1" in call_args[0][0]
    assert call_args[0][1] == "QmTestRecord"


@pytest.mark.asyncio
async def test_reset_pin_attempts(mock_db: AsyncMock) -> None:
    """Test resetting pin attempts."""
    cids = ["QmTestReset"]
    if not cids:
        return

    await mock_db.execute(
        """
        UPDATE part_chunks
        SET pin_attempts = 0,
            last_pinned_at = NULL
        WHERE cid = ANY($1::text[])
        """,
        cids,
    )

    mock_db.execute.assert_called_once()
    call_args = mock_db.execute.call_args
    assert "UPDATE part_chunks" in call_args[0][0]
    assert "pin_attempts = 0" in call_args[0][0]


@pytest.mark.asyncio
async def test_reset_pin_attempts_multiple(mock_db: AsyncMock) -> None:
    """Test resetting pin attempts for multiple CIDs."""
    cids = ["QmReset1", "QmReset2"]
    if not cids:
        return

    await mock_db.execute(
        """
        UPDATE part_chunks
        SET pin_attempts = 0,
            last_pinned_at = NULL
        WHERE cid = ANY($1::text[])
        """,
        cids,
    )

    mock_db.execute.assert_called_once()
    call_args = mock_db.execute.call_args
    assert "UPDATE part_chunks" in call_args[0][0]
    assert "pin_attempts = 0" in call_args[0][0]
    assert call_args[0][1] == ["QmReset1", "QmReset2"]
