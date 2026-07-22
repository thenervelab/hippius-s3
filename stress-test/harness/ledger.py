"""The durability oracle: an in-memory + on-disk manifest of every acked object.

Records (bucket, key, plaintext-md5, size) at PUT time; the durability scenario re-GETs each
and asserts byte-identical. Keys on client-side PLAINTEXT md5 — NOT ETag (hippius objects are
envelope-encrypted and MPU ETags are not content hashes).
"""

from __future__ import annotations

import dataclasses
import json
import pathlib
import threading


@dataclasses.dataclass(frozen=True)
class Acked:
    bucket: str
    key: str
    md5: str
    size: int
    multipart: bool


class Ledger:
    def __init__(self) -> None:
        self._items: list[Acked] = []
        self._lock = threading.Lock()

    def record(self, bucket: str, key: str, md5: str, size: int, multipart: bool) -> None:
        with self._lock:
            self._items.append(Acked(bucket, key, md5, size, multipart))

    def all(self) -> list[Acked]:
        with self._lock:
            return list(self._items)

    def __len__(self) -> int:
        with self._lock:
            return len(self._items)

    def dump(self, path: pathlib.Path) -> None:
        path.write_text(json.dumps([dataclasses.asdict(a) for a in self.all()], indent=2))
