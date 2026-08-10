"""RD-2 / WU-1: offloading AES-GCM to the crypto pool must be byte-for-byte identical to the inline
path — no reordering, no altered ciphertext/plaintext. Parametrized over sizes (incl. empty and
exact/off-by-one chunk multiples) stands in for a property test since hypothesis isn't a dependency.
"""

from __future__ import annotations

import pytest
from cryptography.exceptions import InvalidTag

from hippius_s3.reader.decrypter import decrypt_chunk_if_needed
from hippius_s3.services.crypto_pool import run_crypto
from hippius_s3.services.crypto_service import CryptoService


SUITE = "hip-enc/aes256gcm"
KEY = bytes(range(32))
SIZES = [0, 1, 15, 16, 17, 255, 4096, 4 * 1024 * 1024 - 1, 4 * 1024 * 1024]


def _enc_kwargs(chunk_index: int) -> dict:
    return {
        "key": KEY,
        "bucket_id": "bkt-1",
        "object_id": "11111111-1111-1111-1111-111111111111",
        "part_number": 1,
        "chunk_index": chunk_index,
        "upload_id": "",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("size", SIZES)
async def test_offloaded_encrypt_matches_inline(size: int) -> None:
    adapter = CryptoService.get_adapter(SUITE)
    buf = bytes((i * 7 + 3) & 0xFF for i in range(size))

    offloaded = await run_crypto(adapter.encrypt_chunk, buf, **_enc_kwargs(0))

    # Byte-equality against an inline encrypt is NOT the assertion, even though it reads like the
    # obvious one. Nonces are random per call — deliberately, because deriving them from the
    # chunk's identity made an UploadPart retry reuse a nonce under the same DEK — so two
    # encryptions of one input differ, and a test demanding otherwise would be pinning the bug.
    #
    # What the offload has to preserve is that the ciphertext is the plaintext: same length, and
    # it round-trips. A thread-pool hop that corrupted or truncated the buffer fails both.
    assert len(offloaded) == size + adapter.overhead_per_chunk
    assert adapter.decrypt_chunk(offloaded, **_enc_kwargs(0)) == buf, (
        "offloaded encrypt must round-trip to the original plaintext"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("size", SIZES)
async def test_decrypt_if_needed_roundtrips_via_pool(size: int) -> None:
    adapter = CryptoService.get_adapter(SUITE)
    plaintext = bytes((i * 11 + 5) & 0xFF for i in range(size))
    chunk_index = 3

    ct = adapter.encrypt_chunk(plaintext, **_enc_kwargs(chunk_index))

    got = await decrypt_chunk_if_needed(
        ct,
        object_id="11111111-1111-1111-1111-111111111111",
        part_number=1,
        chunk_index=chunk_index,
        storage_version=5,
        key_bytes=KEY,
        suite_id=SUITE,
        bucket_id="bkt-1",
        upload_id="",
    )
    assert got == plaintext, "encrypt→offloaded-decrypt must be the identity"


@pytest.mark.asyncio
async def test_offloaded_decrypt_rejects_tampered_ciphertext() -> None:
    # An auth-tag failure must still raise (propagates to break the stream), not return garbage.
    adapter = CryptoService.get_adapter(SUITE)
    ct = bytearray(adapter.encrypt_chunk(b"hello world" * 100, **_enc_kwargs(0)))
    ct[-1] ^= 0x01  # flip a tag bit

    with pytest.raises(InvalidTag):
        await decrypt_chunk_if_needed(
            bytes(ct),
            object_id="11111111-1111-1111-1111-111111111111",
            part_number=1,
            chunk_index=0,
            storage_version=5,
            key_bytes=KEY,
            suite_id=SUITE,
            bucket_id="bkt-1",
            upload_id="",
        )
