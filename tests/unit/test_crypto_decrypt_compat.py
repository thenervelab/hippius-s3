"""Ciphertext written before the nonce rotation must still decrypt, forever.

Every object in production today was encrypted with a nonce derived as
HMAC(DEK, "hippius-aesgcm-nonce[-v2]:bucket=..:object=..[:upload=..]:part=..:chunk=..")[:12].
Rotating to a random nonce is claimed to need no migration and no suite bump, because the nonce
travels in the ciphertext prefix and `decrypt_chunk` reads it from there instead of re-deriving it.
A round-trip test cannot check that claim: it encrypts and decrypts with the same code, so it holds
whatever the framing is. These frozen vectors are the check — they were produced by code that no
longer exists in the tree, so they fail the moment the read path stops being able to read what
production already holds. That failure mode is unrecoverable data loss, not a broken build.

The case that matters most is a change applied SYMMETRICALLY to encrypt and decrypt — new AAD field,
different framing — because no round-trip test can see it and it would brick every stored object.
Verified: adding a field to v2's `_build_aad` on both sides leaves all of
test_aesgcm_nonce_uniqueness.py green and fails the v2 vector here. A decrypt that goes back to
deriving the nonce is caught too, but only by the v1 vector — v2's derivation would reproduce v2's
own golden nonce — together with the round-trip test in that sibling file. That is why both suites
keep a vector and not just the registered one.

HOW THEY WERE GENERATED — regenerate the same way if the inputs below ever need to change:
`_derive_nonce` and `_build_aad` were copied verbatim out of
`6152e3a3f22b694ad22745ba0dbe42c241e6c520:hippius_s3/services/crypto_service.py` (the last commit
that shipped the derived nonce) into a standalone script importing nothing from `hippius_s3`, and
run over the fixed inputs below. The transcription was cross-checked by loading that same historical
module directly and confirming it emits byte-identical blobs.
"""

from __future__ import annotations

from typing import TypedDict

import pytest
from cryptography.exceptions import InvalidTag

from hippius_s3.services.crypto_service import AESGCMChunkedAdapter
from hippius_s3.services.crypto_service import AESGCMChunkedAdapterV2


class ChunkIdentity(TypedDict):
    bucket_id: str
    object_id: str
    upload_id: str
    part_number: int
    chunk_index: int


# Not a secret and never was: 0x00..0x1f, so a leaked vector is obviously test-only.
KEY = bytes(range(32))
# The real chunk-AAD inputs. The ids are placeholders but the SHAPE is what production binds:
# `_build_aad` length-prefixes the bucket and object ids (plus the upload id, v1 only) and packs
# part and chunk as little-endian u32. A non-zero part and chunk index are deliberate — zeroes
# would hide a mixed-up struct field.
CHUNK: ChunkIdentity = {
    "bucket_id": "6f1e0a4e-0000-4000-8000-000000000001",
    "object_id": "6f1e0a4e-0000-4000-8000-000000000002",
    "upload_id": "6f1e0a4e-0000-4000-8000-000000000003",
    "part_number": 3,
    "chunk_index": 7,
}
PLAINTEXT = b"golden plaintext for pre-rotation decrypt compatibility"

# Mirrors test_aesgcm_nonce_uniqueness.py: the adapters are instantiated DIRECTLY rather than
# resolved through `CryptoService.get_adapter`, because only the v2 suite is in the registry and
# `get_adapter` falls back to the default for an unrecognised id — parametrising over suite strings
# would silently exercise v2 twice and report coverage of the deprecated adapter it never touched.
VECTORS = [
    (
        AESGCMChunkedAdapterV2(),
        "4b5b0551836f175e77d96af836c14eeef89175d5550ec66d0dc916cbba27f7f5e537718eed348ad2"
        "3857e519f6e8aa5b20f5dae9310486c5c9cbee0d762fa6a2f9abd42ab3277022c6074eb8a11c26dc"
        "c7b386",
    ),
    (
        AESGCMChunkedAdapter(),
        "7da9b0ccf1f173e7c2b4b0f0459ca858a8ddc9adcf827a3150265788781f6e74a871e75f41af3e4b"
        "7db1bed00804e3684de58f489ce1ca0e08cc4d0a3e36f04e06ab6d47d886c4f181a6982938a0fdfa"
        "a47497",
    ),
]
IDS = ["v2-default", "v1-deprecated"]


@pytest.mark.parametrize(("adapter", "blob_hex"), VECTORS, ids=IDS)
def test_a_derived_nonce_ciphertext_still_decrypts(adapter: AESGCMChunkedAdapter, blob_hex: str) -> None:
    """The compatibility claim, checked against bytes the current encrypt path cannot produce."""
    assert adapter.decrypt_chunk(bytes.fromhex(blob_hex), key=KEY, **CHUNK) == PLAINTEXT


@pytest.mark.parametrize(("adapter", "blob_hex"), VECTORS, ids=IDS)
def test_a_corrupted_golden_ciphertext_fails_to_authenticate(adapter: AESGCMChunkedAdapter, blob_hex: str) -> None:
    """Proves the vectors are live rather than decrypting by some accident of framing.

    Flipping a single body bit must fail the tag, so a vector that passes is genuinely being
    authenticated against this key and AAD.
    """
    blob = bytearray(bytes.fromhex(blob_hex))
    blob[adapter.NONCE_SIZE] ^= 0x01

    with pytest.raises(InvalidTag):
        adapter.decrypt_chunk(bytes(blob), key=KEY, **CHUNK)


def test_the_two_suites_vectors_are_not_interchangeable() -> None:
    """v1 binds the upload id into the AAD and v2 does not, so their vectors must not cross-decrypt.

    Without this, a refactor that collapsed the deprecated adapter into v2 would leave both golden
    tests above passing while silently making every v1-era object unreadable.
    """
    v2_adapter, v2_hex = VECTORS[0]
    v1_adapter, v1_hex = VECTORS[1]

    with pytest.raises(InvalidTag):
        v2_adapter.decrypt_chunk(bytes.fromhex(v1_hex), key=KEY, **CHUNK)
    with pytest.raises(InvalidTag):
        v1_adapter.decrypt_chunk(bytes.fromhex(v2_hex), key=KEY, **CHUNK)
