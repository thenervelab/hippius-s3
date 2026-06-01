from typing import Any

import pytest

from hippius_s3.writer.write_through_writer import WriteThroughPartsWriter


class RecordingFS:
    def __init__(self) -> None:
        self.chunk_calls: list[tuple[int, int]] = []
        self.meta_calls: list[int] = []

    async def set_chunk(self, object_id: Any, ov: int, pn: int, idx: int, data: bytes) -> None:
        self.chunk_calls.append((int(pn), int(idx)))

    async def set_meta(self, object_id: Any, ov: int, pn: int, **kwargs: Any) -> None:
        self.meta_calls.append(int(pn))


class ExplodingCache:
    """Stands in for the old Redis mirror — any write to it is a regression."""

    async def set_chunks(self, *_a: Any, **_k: Any) -> None:
        raise AssertionError("WriteThroughPartsWriter must not mirror chunks to the cache")

    async def set_meta(self, *_a: Any, **_k: Any) -> None:
        raise AssertionError("WriteThroughPartsWriter must not mirror meta to the cache")


OBJ = "11111111-2222-3333-4444-555555555555"


@pytest.mark.asyncio
async def test_write_chunks_only_touches_fs() -> None:
    fs = RecordingFS()
    writer = WriteThroughPartsWriter(fs, ExplodingCache(), ttl_seconds=60)
    await writer.write_chunks(OBJ, 1, 1, [b"a", b"b", b"c"])
    assert fs.chunk_calls == [(1, 0), (1, 1), (1, 2)]


@pytest.mark.asyncio
async def test_write_meta_only_touches_fs() -> None:
    fs = RecordingFS()
    writer = WriteThroughPartsWriter(fs, ExplodingCache(), ttl_seconds=60)
    await writer.write_meta(OBJ, 1, 1, chunk_size=4, num_chunks=3, plain_size=12)
    assert fs.meta_calls == [1]
