"""Content-correctness pins for the two streaming write paths.

These exist to make the writer-pipeline refactor (MD5 off-loop + encrypt look-ahead)
safe: nothing else in the suite asserts that the ETag equals the body's real MD5, that
every stored chunk decrypts back to the original plaintext with the right AAD/index
binding, or that ``chunk_cipher_sizes`` lands in index order. They pin the CURRENT
behavior; the refactor must keep every one of them green.

Chunk size is shrunk to 8 KiB via the config object so chunk-boundary patterns run in
milliseconds instead of megabytes.
"""

import asyncio
import hashlib
import pathlib
import uuid
from typing import Any
from typing import AsyncIterator

import pytest

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.config import get_config
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.writer.object_writer import ObjectWriter
from tests.unit._fake_pool import make_fake_pool


CHUNK = 8 * 1024
DEK = b"\x07" * 32
SUITE = "hip-enc/aes256gcm"


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


def _chunk_file(fs_store: FileSystemPartsStore, object_id: str, part: int, idx: int) -> pathlib.Path:
    return pathlib.Path(fs_store.part_path(object_id, 1, part)) / f"chunk_{idx}.bin"


def _meta_file(fs_store: FileSystemPartsStore, object_id: str, part: int) -> pathlib.Path:
    return pathlib.Path(fs_store.part_path(object_id, 1, part)) / "meta.json"



@pytest.fixture
def rig(tmp_path: Any, monkeypatch: Any):
    """ObjectWriter with a known DEK, small chunks, and captured part-placeholder args."""
    cfg = get_config()
    monkeypatch.setattr(cfg, "object_chunk_size_bytes", CHUNK)
    monkeypatch.setattr("hippius_s3.writer.object_writer.get_config", lambda: cfg)
    monkeypatch.setattr("hippius_s3.services.envelope_service.generate_dek", lambda: DEK)

    async def fake_kek(*, bucket_id: str) -> tuple[str, bytes]:
        return ("kek-1", b"\x00" * 32)

    monkeypatch.setattr("hippius_s3.services.kek_service.get_or_create_active_bucket_kek", fake_kek)

    captured: dict[str, Any] = {}

    async def fake_upsert(db: Any, **kw: Any) -> dict:
        return {"object_id": kw["object_id"], "current_object_version": 1}

    async def fake_ensure(db: Any, **kw: Any) -> str:
        captured["ensure_called"] = True
        return str(uuid.uuid4())

    async def fake_parts(db: Any, **kw: Any) -> None:
        captured["placeholder"] = kw

    monkeypatch.setattr("hippius_s3.writer.object_writer.upsert_object_basic", fake_upsert)
    monkeypatch.setattr("hippius_s3.writer.object_writer.ensure_upload_row", fake_ensure)
    monkeypatch.setattr("hippius_s3.writer.object_writer.upsert_part_placeholder", fake_parts)

    fs_store = FileSystemPartsStore(str(tmp_path))
    writer = ObjectWriter(pool=make_fake_pool(), redis_client=DummyRedis(), fs_store=fs_store)
    return writer, fs_store, captured


async def _put(writer: ObjectWriter, bucket_id: str, *pieces: bytes) -> Any:
    return await writer.put_simple_stream_full(
        bucket_id=bucket_id,
        bucket_name="bkt",
        object_id=str(uuid.uuid4()),
        object_key="k/pin.bin",
        account_address="acct",
        content_type="application/octet-stream",
        metadata={},
        body_iter=_body(*pieces),
    )


def _reassemble(fs_store: FileSystemPartsStore, bucket_id: str, object_id: str, num_chunks: int) -> tuple[bytes, list[int]]:
    """Decrypt every stored chunk with the known DEK and the exact AAD binding."""
    adapter = CryptoService.get_adapter(SUITE)
    plain = b""
    ct_sizes: list[int] = []
    for i in range(num_chunks):
        ct = _chunk_file(fs_store, object_id, 1, i).read_bytes()
        ct_sizes.append(len(ct))
        plain += adapter.decrypt_chunk(
            ct, key=DEK, bucket_id=bucket_id, object_id=object_id, part_number=1, chunk_index=i, upload_id=""
        )
    return plain, ct_sizes


BODIES = {
    "sub_chunk": [b"x" * 100],
    "exact_one_chunk": [bytes(range(256)) * 32],  # 8192 == CHUNK
    "chunk_plus_one": [b"a" * (CHUNK + 1)],
    "multi_chunk_small_pieces": [b"pqrs" * 700 for _ in range(9)],  # 25200 B in 2800-B pieces
    "one_byte_pieces_over_boundary": [b"z"] * (CHUNK + 3),
    "empty_pieces_interleaved": [b"", b"m" * CHUNK, b"", b"n" * 10, b""],
}


@pytest.mark.asyncio
@pytest.mark.parametrize("name", sorted(BODIES))
async def test_etag_is_md5_and_chunks_decrypt_back(rig: Any, name: str) -> None:
    writer, fs_store, captured = rig
    pieces = BODIES[name]
    body = b"".join(pieces)
    bucket_id = str(uuid.uuid4())

    res = await _put(writer, bucket_id, *pieces)

    assert res.etag == hashlib.md5(body).hexdigest(), "ETag must be the MD5 of the whole body"
    assert res.size_bytes == len(body)

    expected_chunks = (len(body) + CHUNK - 1) // CHUNK
    plain, ct_sizes = _reassemble(fs_store, bucket_id, res.object_id, expected_chunks)
    assert plain == body, "stored chunks must decrypt back to the original plaintext, in index order"

    # chunk_cipher_sizes recorded for the DB must match the real ciphertexts, index-ordered.
    assert captured["placeholder"]["chunk_cipher_sizes"] == ct_sizes

    assert _meta_file(fs_store, res.object_id, 1).exists(), "meta.json is the part-complete signal and must exist after success"


@pytest.mark.asyncio
async def test_empty_body(rig: Any) -> None:
    writer, fs_store, captured = rig
    res = await _put(writer, str(uuid.uuid4()))
    assert res.etag == hashlib.md5(b"").hexdigest()
    assert res.size_bytes == 0
    assert captured["placeholder"]["chunk_cipher_sizes"] == []
    assert _meta_file(fs_store, res.object_id, 1).exists()


@pytest.mark.asyncio
async def test_consumer_error_propagates_and_nothing_finalizes(rig: Any, monkeypatch: Any) -> None:
    """fs_store failure mid-stream: the PUT must fail, write no meta, and never reach the DB tail."""
    writer, fs_store, captured = rig

    real_set_chunk = fs_store.set_chunk
    calls = {"n": 0}

    async def failing_set_chunk(*a: Any, **k: Any) -> None:
        calls["n"] += 1
        if calls["n"] >= 2:
            raise OSError("disk gone")
        await real_set_chunk(*a, **k)

    monkeypatch.setattr(fs_store, "set_chunk", failing_set_chunk)

    with pytest.raises(OSError, match="disk gone"):
        await _put(writer, str(uuid.uuid4()), b"q" * (CHUNK * 4))

    assert "ensure_called" not in captured, "the DB tail must not run after a consumer failure"
    assert "placeholder" not in captured


@pytest.mark.asyncio
async def test_body_iter_error_cancels_consumer_and_writes_no_meta(rig: Any) -> None:
    """A client disconnect mid-stream unwinds without meta, without the DB tail, and
    without leaking the consumer task (the finally-cancel path)."""
    writer, fs_store, captured = rig

    async def dying_body() -> AsyncIterator[bytes]:
        yield b"h" * CHUNK
        yield b"i" * CHUNK
        raise ConnectionResetError("client went away")

    tasks_before = len(asyncio.all_tasks())
    with pytest.raises(ConnectionResetError):
        await writer.put_simple_stream_full(
            bucket_id=str(uuid.uuid4()),
            bucket_name="bkt",
            object_id=str(uuid.uuid4()),
            object_key="k/dead.bin",
            account_address="acct",
            content_type="application/octet-stream",
            metadata={},
            body_iter=dying_body(),
        )
    await asyncio.sleep(0)  # let any leaked task surface
    assert len(asyncio.all_tasks()) <= tasks_before, "consumer task leaked past the failed PUT"
    assert "ensure_called" not in captured
    assert "placeholder" not in captured


# ---------------------------------------------------------------------------
# MPU streaming path — same producer, plus staging/publish and its own aborts.
# ---------------------------------------------------------------------------


@pytest.fixture
def mpu_rig(tmp_path: Any, monkeypatch: Any):
    cfg = get_config()
    monkeypatch.setattr(cfg, "object_chunk_size_bytes", CHUNK)
    monkeypatch.setattr("hippius_s3.writer.object_writer.get_config", lambda: cfg)

    captured: dict[str, Any] = {}

    async def fake_parts(db: Any, **kw: Any) -> None:
        captured["placeholder"] = kw

    monkeypatch.setattr("hippius_s3.writer.object_writer.upsert_part_placeholder", fake_parts)

    fs_store = FileSystemPartsStore(str(tmp_path))
    writer = ObjectWriter(pool=make_fake_pool(), redis_client=DummyRedis(), fs_store=fs_store)

    async def fixed_dek(**_k: Any) -> bytes:
        return DEK

    monkeypatch.setattr(writer, "_ensure_and_get_v5_dek", fixed_dek)
    return writer, fs_store, captured


async def _put_part(writer: ObjectWriter, bucket_id: str, object_id: str, *pieces: bytes, **kw: Any) -> Any:
    return await writer.mpu_upload_part_stream(
        upload_id="up-1",
        object_id=object_id,
        object_version=1,
        bucket_name="bkt",
        bucket_id=bucket_id,
        account_address="acct",
        part_number=3,
        body_iter=_body(*pieces),
        **kw,
    )


@pytest.mark.asyncio
async def test_mpu_part_etag_decrypt_and_size_order(mpu_rig: Any) -> None:
    writer, fs_store, captured = mpu_rig
    bucket_id, object_id = str(uuid.uuid4()), str(uuid.uuid4())
    body = b"K" * (CHUNK * 2 + 77)

    res = await _put_part(writer, bucket_id, object_id, body[: CHUNK + 5], body[CHUNK + 5 :])

    assert res.etag == hashlib.md5(body).hexdigest()
    assert res.size_bytes == len(body)

    adapter = CryptoService.get_adapter(SUITE)
    plain = b""
    ct_sizes = []
    for i in range(3):
        ct = _chunk_file(fs_store, object_id, 3, i).read_bytes()
        ct_sizes.append(len(ct))
        plain += adapter.decrypt_chunk(
            ct, key=DEK, bucket_id=bucket_id, object_id=object_id, part_number=3, chunk_index=i, upload_id="up-1"
        )
    assert plain == body, "published MPU chunks must decrypt back to the part body"
    assert captured["placeholder"]["chunk_cipher_sizes"] == ct_sizes
    assert _meta_file(fs_store, object_id, 3).exists()


@pytest.mark.asyncio
async def test_mpu_zero_length_part_raises_and_publishes_nothing(mpu_rig: Any) -> None:
    writer, fs_store, captured = mpu_rig
    object_id = str(uuid.uuid4())
    with pytest.raises(ValueError, match="Zero-length part"):
        await _put_part(writer, str(uuid.uuid4()), object_id)
    assert "placeholder" not in captured
    assert not _meta_file(fs_store, object_id, 3).exists()


@pytest.mark.asyncio
async def test_mpu_oversize_aborts_without_publish(mpu_rig: Any) -> None:
    writer, fs_store, captured = mpu_rig
    object_id = str(uuid.uuid4())
    with pytest.raises(ValueError, match="part_size_exceeds_max"):
        await _put_part(
            writer, str(uuid.uuid4()), object_id, b"a" * CHUNK, b"b" * CHUNK, b"c" * CHUNK, max_size_bytes=CHUNK * 2
        )
    assert "placeholder" not in captured
    assert not _meta_file(fs_store, object_id, 3).exists()


@pytest.mark.asyncio
async def test_mpu_stage_failure_propagates_without_publish(mpu_rig: Any, monkeypatch: Any) -> None:
    writer, fs_store, captured = mpu_rig
    object_id = str(uuid.uuid4())

    async def failing_stage(*a: Any, **k: Any) -> None:
        raise OSError("staging disk gone")

    monkeypatch.setattr(fs_store, "stage_chunk", failing_stage)
    with pytest.raises(OSError, match="staging disk gone"):
        await _put_part(writer, str(uuid.uuid4()), object_id, b"m" * (CHUNK * 3))
    assert "placeholder" not in captured
    assert not _meta_file(fs_store, object_id, 3).exists()


@pytest.mark.asyncio
async def test_mpu_empty_pieces_skipped(mpu_rig: Any) -> None:
    writer, fs_store, captured = mpu_rig
    bucket_id, object_id = str(uuid.uuid4()), str(uuid.uuid4())
    body = b"E" * (CHUNK + 9)
    res = await _put_part(writer, bucket_id, object_id, b"", body[:5], b"", body[5:], b"")
    assert res.etag == hashlib.md5(body).hexdigest()
    assert res.size_bytes == len(body)
