from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from typing import AsyncIterator


StreamLike = AsyncIterator[bytes]


@dataclass
class PutContext:
    db: Any
    redis: Any
    ipfs_service: Any
    account_address: str
    bucket_id: str
    bucket_name: str
    object_id: str
    object_key: str
    object_version: int


@dataclass
class PutResult:
    object_id: str
    etag: str
    size_bytes: int
    upload_id: str
    object_version: int


@dataclass
class PartResult:
    etag: str
    size_bytes: int
    part_number: int


@dataclass
class CompleteResult:
    etag: str
    size_bytes: int


# Typed domain errors for writer operations
class AppendPreconditionFailed(Exception):
    def __init__(self, current_version: int) -> None:
        super().__init__("Append precondition failed")
        self.current_version = int(current_version)


class ObjectNotFound(Exception):
    pass


class EmptyAppendError(Exception):
    pass


class BadDigest(Exception):
    """The MD5 of the body the writer received does not match the client's Content-MD5."""

    def __init__(self, *, expected: bytes, actual: bytes) -> None:
        super().__init__(f"Content-MD5 mismatch: expected {expected.hex()}, received {actual.hex()}")
        self.expected = expected
        self.actual = actual
