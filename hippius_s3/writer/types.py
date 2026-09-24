from __future__ import annotations

from dataclasses import dataclass


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


class UploadNoLongerOpen(Exception):
    """CompleteMultipartUpload found its upload aborted (or completed) under it; nothing was committed."""


class ObjectVersionLocked(Exception):
    """An append would change an Object-Locked version in place, which a lock forbids."""


class PreconditionFailed(Exception):
    """A conditional write (If-None-Match: *) found the key already existing."""


class BadDigest(Exception):
    """The MD5 of the body the writer received does not match the client's Content-MD5."""

    def __init__(self, *, expected: bytes, actual: bytes) -> None:
        super().__init__(f"Content-MD5 mismatch: expected {expected.hex()}, received {actual.hex()}")
        self.expected = expected
        self.actual = actual
