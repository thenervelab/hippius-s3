import uuid
from pathlib import Path
from typing import AsyncIterator

import pytest

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.config import get_config
from hippius_s3.writer.object_writer import ObjectWriter


@pytest.mark.asyncio
async def test_mpu_upload_part_stream_cleans_up_on_oversize(tmp_path, monkeypatch):
    cfg = get_config()
    original_chunk_size = cfg.object_chunk_size_bytes
    original_max_part = cfg.max_multipart_part_size
    original_ttl = cfg.cache_ttl_seconds

    cfg.object_chunk_size_bytes = 4
    cfg.max_multipart_part_size = 5
    cfg.cache_ttl_seconds = 60

    async def fake_key(*_args, **_kwargs) -> bytes:
        return b"\x00" * 32

    monkeypatch.setattr("hippius_s3.writer.object_writer.get_or_create_encryption_key_bytes", fake_key)
    monkeypatch.setattr("hippius_s3.writer.object_writer.get_config", lambda: cfg)

    object_id = str(uuid.uuid4())
    fs_store = FileSystemPartsStore(str(tmp_path))

    class DummyDB:
        async def fetchrow(self, *_args, **_kwargs):
            return {"bucket_id": "bucket", "storage_version": 2}

        async def fetchval(self, *_args, **_kwargs):
            return "part-id"

        async def execute(self, *_args, **_kwargs):
            return None

    async def body_iter() -> AsyncIterator[bytes]:
        yield b"abcd"
        yield b"ef"

    writer = ObjectWriter(db=DummyDB(), fs_store=fs_store)

    try:
        with pytest.raises(ValueError, match="part_size_exceeds_max"):
            await writer.mpu_upload_part_stream(
                upload_id="upload",
                object_id=object_id,
                object_version=1,
                bucket_name="bucket",
                account_address="acct",
                seed_phrase="seed",
                part_number=1,
                body_iter=body_iter(),
            )

        part_dir = Path(fs_store.part_path(object_id, 1, 1))
        assert not part_dir.exists()
    finally:
        cfg.object_chunk_size_bytes = original_chunk_size
        cfg.max_multipart_part_size = original_max_part
        cfg.cache_ttl_seconds = original_ttl
