import asyncio
import contextlib
import json
import logging
import time
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional


logger = logging.getLogger(__name__)


class PinnerDLQEntry:
    """Represents a CID entry in the pin checker DLQ."""

    def __init__(
        self,
        cid: str,
        user: str,
        object_id: str,
        object_version: int,
        reason: str,
        pin_attempts: int,
        last_pinned_at: Optional[float] = None,
        dlq_timestamp: Optional[float] = None,
    ):
        self.cid = cid
        self.user = user
        self.object_id = object_id
        self.object_version = object_version
        self.reason = reason
        self.pin_attempts = pin_attempts
        self.last_pinned_at = last_pinned_at
        self.dlq_timestamp = dlq_timestamp or time.time()

    def to_dict(self) -> Dict[str, Any]:
        return {
            "cid": self.cid,
            "user": self.user,
            "object_id": self.object_id,
            "object_version": self.object_version,
            "reason": self.reason,
            "pin_attempts": self.pin_attempts,
            "last_pinned_at": self.last_pinned_at,
            "dlq_timestamp": self.dlq_timestamp,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PinnerDLQEntry":
        return cls(
            cid=data["cid"],
            user=data["user"],
            object_id=data["object_id"],
            object_version=data["object_version"],
            reason=data.get("reason", "unknown"),
            pin_attempts=data.get("pin_attempts", 0),
            last_pinned_at=data.get("last_pinned_at"),
            dlq_timestamp=data.get("dlq_timestamp"),
        )


class PinnerDLQManager:
    """Filesystem-backed DLQ for pin checker CIDs."""

    def __init__(self, dlq_dir: str):
        self.dlq_dir = Path(dlq_dir)
        self.dlq_dir.mkdir(parents=True, exist_ok=True)

    def _cid_path(self, cid: str) -> Path:
        """Get filesystem path for a CID entry."""
        subdir = self.dlq_dir / "xx" if len(cid) < 2 else self.dlq_dir / cid[:2]
        subdir.mkdir(exist_ok=True)
        return subdir / f"{cid}.json"

    async def is_in_dlq(self, cid: str) -> bool:
        """Check if CID exists in DLQ - O(1) filesystem check."""
        path = self._cid_path(cid)
        return await asyncio.to_thread(path.exists)

    async def push(self, entry: PinnerDLQEntry) -> None:
        """Push a CID entry to the DLQ (filesystem)."""
        path = self._cid_path(entry.cid)

        def _write() -> None:
            with path.open("w") as f:
                json.dump(entry.to_dict(), f, indent=2, default=str)

        await asyncio.to_thread(_write)
        logger.warning(f"Pushed to pinner DLQ: cid={entry.cid} user={entry.user} attempts={entry.pin_attempts}")

    async def peek(self, limit: int = 10) -> List[PinnerDLQEntry]:
        """Peek at DLQ entries."""

        def _scan() -> List[PinnerDLQEntry]:
            entries = []
            for subdir in sorted(self.dlq_dir.iterdir()):
                if not subdir.is_dir():
                    continue
                for path in sorted(subdir.glob("*.json")):
                    try:
                        with path.open() as f:
                            data = json.load(f)
                            entries.append(PinnerDLQEntry.from_dict(data))
                        if len(entries) >= limit:
                            return entries
                    except Exception as e:
                        logger.warning(f"Invalid DLQ file {path}: {e}")
            return entries

        return await asyncio.to_thread(_scan)

    async def stats(self) -> Dict[str, Any]:
        """Get DLQ statistics."""

        def _count() -> int:
            count = 0
            for subdir in self.dlq_dir.iterdir():
                if subdir.is_dir():
                    count += len(list(subdir.glob("*.json")))
            return count

        count = await asyncio.to_thread(_count)
        return {
            "total_entries": count,
            "dlq_path": str(self.dlq_dir),
        }

    async def find_and_remove(self, cid: str) -> Optional[PinnerDLQEntry]:
        """Find and remove a specific CID entry."""
        path = self._cid_path(cid)

        def _remove() -> Optional[PinnerDLQEntry]:
            if not path.exists():
                return None
            try:
                with path.open() as f:
                    data = json.load(f)
                entry = PinnerDLQEntry.from_dict(data)
                path.unlink()
                return entry
            except Exception as e:
                logger.error(f"Failed to remove DLQ entry {cid}: {e}")
                return None

        return await asyncio.to_thread(_remove)

    async def purge(self, cid: Optional[str] = None) -> int:
        """Purge entries from DLQ."""
        if cid:
            entry = await self.find_and_remove(cid)
            return 1 if entry else 0

        def _purge_all() -> int:
            count = 0
            for subdir in self.dlq_dir.iterdir():
                if subdir.is_dir():
                    for path in subdir.glob("*.json"):
                        path.unlink()
                        count += 1
                    with contextlib.suppress(OSError):
                        subdir.rmdir()
            return count

        return await asyncio.to_thread(_purge_all)

    async def export_all(self) -> List[PinnerDLQEntry]:
        """Export all DLQ entries."""

        def _scan_all() -> List[PinnerDLQEntry]:
            entries = []
            for subdir in sorted(self.dlq_dir.iterdir()):
                if not subdir.is_dir():
                    continue
                for path in sorted(subdir.glob("*.json")):
                    try:
                        with path.open() as f:
                            data = json.load(f)
                            entries.append(PinnerDLQEntry.from_dict(data))
                    except Exception as e:
                        logger.warning(f"Invalid DLQ file during export {path}: {e}")
            return entries

        return await asyncio.to_thread(_scan_all)
