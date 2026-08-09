"""AES-GCM nonces must never repeat under one key, whatever the caller does.

The chunk adapters used to derive the nonce deterministically from the chunk's identity
(bucket, object, part, chunk) under the DEK. Every one of those inputs is stable across an
`UploadPart` retry, and the DEK is per object-version which a retry also preserves — so a client
re-uploading a part with different bytes encrypted DIFFERENT PLAINTEXT UNDER THE SAME KEY AND
NONCE. That is the one thing AES-GCM must never do: the keystream repeats, so the XOR of the two
plaintexts leaks, and the GHASH subkey becomes recoverable, which turns the DEK into a tag-forgery
oracle.

The trigger is an ordinary client retry, not an attack. `UploadPart` has no guard against
re-uploading a part number, and re-uploading before `CompleteMultipartUpload` is legal S3.

These tests pin the property rather than the implementation: they say nonces must differ, not how.
"""

from __future__ import annotations

import pytest

from hippius_s3.services.crypto_service import AESGCMChunkedAdapter
from hippius_s3.services.crypto_service import AESGCMChunkedAdapterV2


KEY = b"\x11" * 32
CHUNK = {
    "bucket_id": "bucket-1",
    "object_id": "object-1",
    "part_number": 1,
    "chunk_index": 0,
    "upload_id": "upload-1",
}
# The adapters are instantiated DIRECTLY rather than resolved through `CryptoService.get_adapter`.
# Only the v2 suite is in the registry, and `get_adapter` falls back to the default for an
# unrecognised id — so parametrising over suite strings would silently exercise v2 twice and report
# coverage of the deprecated adapter it never touched.
ADAPTERS = [AESGCMChunkedAdapterV2(), AESGCMChunkedAdapter()]


def _nonce(blob: bytes, adapter: object) -> bytes:
    return blob[: adapter.NONCE_SIZE]  # type: ignore[attr-defined]


@pytest.mark.parametrize("adapter", ADAPTERS, ids=["v2-default", "v1-deprecated"])
def test_re_encrypting_the_same_chunk_never_reuses_a_nonce(adapter: AESGCMChunkedAdapter) -> None:
    """The MPU-retry shape: identical chunk identity, identical DEK, different bytes.

    This is the exact call the api makes on a re-`UploadPart`, so if the nonces match here they
    match in production.
    """

    first = adapter.encrypt_chunk(b"ATTEMPT-ONE-PLAINTEXT", key=KEY, **CHUNK)
    second = adapter.encrypt_chunk(b"attempt-two-plaintext", key=KEY, **CHUNK)

    assert _nonce(first, adapter) != _nonce(second, adapter), (
        "same key + same nonce + different plaintext is catastrophic AES-GCM misuse"
    )


@pytest.mark.parametrize("adapter", ADAPTERS, ids=["v2-default", "v1-deprecated"])
def test_the_keystream_does_not_repeat_across_two_encryptions_of_one_chunk(adapter: AESGCMChunkedAdapter) -> None:
    """The consequence, asserted directly rather than inferred from the nonce.

    Under a repeated nonce the ciphertext XOR equals the plaintext XOR, so knowing either
    plaintext yields the other. A nonce assertion alone would pass against an implementation that
    varied the nonce but reused the keystream some other way.
    """
    one, two = b"ATTEMPT-ONE-PLAINTEXT", b"attempt-two-plaintext"

    first = adapter.encrypt_chunk(one, key=KEY, **CHUNK)
    second = adapter.encrypt_chunk(two, key=KEY, **CHUNK)

    body_one = first[adapter.NONCE_SIZE : -adapter.TAG_SIZE]  # type: ignore[attr-defined]
    body_two = second[adapter.NONCE_SIZE : -adapter.TAG_SIZE]  # type: ignore[attr-defined]
    recovered = bytes(a ^ b ^ c for a, b, c in zip(body_one, body_two, one, strict=True))

    assert recovered != two, "one plaintext was recoverable from the other — the keystream repeated"


@pytest.mark.parametrize("adapter", ADAPTERS, ids=["v2-default", "v1-deprecated"])
def test_a_chunk_still_decrypts_after_the_nonce_stops_being_derived(adapter: AESGCMChunkedAdapter) -> None:
    """Round-trip guard. The nonce travels in the ciphertext, so decrypt never re-derives it.

    That is what makes this fix compatible in both directions: an object written before the change
    carries its derived nonce and still reads, and one written after carries a random nonce and
    reads through the identical path. No migration, no suite bump.
    """
    blob = adapter.encrypt_chunk(b"round-trip", key=KEY, **CHUNK)

    assert adapter.decrypt_chunk(blob, key=KEY, **CHUNK) == b"round-trip"


@pytest.mark.parametrize("adapter", ADAPTERS, ids=["v2-default", "v1-deprecated"])
def test_a_wrong_key_still_fails_to_authenticate(adapter: AESGCMChunkedAdapter) -> None:
    """Guards against 'fixing' nonce reuse by weakening authentication."""
    from cryptography.exceptions import InvalidTag

    blob = adapter.encrypt_chunk(b"round-trip", key=KEY, **CHUNK)

    with pytest.raises(InvalidTag):
        adapter.decrypt_chunk(blob, key=b"\x99" * 32, **CHUNK)
