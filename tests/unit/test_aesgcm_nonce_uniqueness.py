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

import random

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


@pytest.mark.parametrize("adapter", ADAPTERS, ids=["v2-default", "v1-deprecated"])
def test_no_two_encryptions_share_a_nonce_across_the_whole_input_space(adapter: AESGCMChunkedAdapter) -> None:
    """The property behind the two example tests above: uniqueness holds for ANY inputs.

    What this adds over them, measured rather than assumed: every example encrypts two DIFFERENT
    plaintexts, so a nonce derived from the plaintext satisfies all of them — verified, all four
    stay green under that mutation — while still colliding whenever a client re-uploads the same
    bytes, which is the commonest retry of all. Only a grid that repeats plaintexts sees it. The
    grid also varies key and chunk identity, so uniqueness is asserted across the whole space
    rather than at one point in it.

    Deliberately a seeded grid rather than `hypothesis`: that dependency arrives on the SSD
    read-tier branch, and declaring it here too would put both branches' `uv.lock` in conflict
    for no extra coverage. Worth converting to a real property test once the two have merged.
    """
    rng = random.Random(20260809)  # noqa: S311 - test-input generation, not cryptographic
    keys = [bytes([b]) * 32 for b in (0x11, 0x22, 0x33)]
    identities = [
        {"bucket_id": f"bucket-{b}", "object_id": f"object-{o}", "part_number": p, "chunk_index": c, "upload_id": "u"}
        for b in (1, 2)
        for o in (1, 2)
        for p in (1, 7)
        for c in (0, 5)
    ]

    seen: dict[bytes, str] = {}
    for key in keys:
        for identity in identities:
            for repeat in range(3):
                plaintext = bytes(rng.getrandbits(8) for _ in range(rng.choice((1, 16, 64))))
                nonce = _nonce(adapter.encrypt_chunk(plaintext, key=key, **identity), adapter)
                where = f"key={key[0]:#x} identity={identity} repeat={repeat}"

                assert len(nonce) == adapter.NONCE_SIZE, f"wrong nonce width at {where}"  # type: ignore[attr-defined]
                assert nonce not in seen, f"nonce reused: {where} collides with {seen[nonce]}"
                seen[nonce] = where
