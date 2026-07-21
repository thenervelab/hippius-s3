"""Direct tests for the DEK envelope (wrap/unwrap under a KEK via AES-256-GCM).

These were previously untested (A4): a wrap/unwrap failure surfaced only as a raw exception far up
the read path. The mapping of that failure to a clean S3 error lives in
`tests/unit/test_read_path_crypto_error_response.py`; here we pin the crypto primitive itself.
"""

from __future__ import annotations

import os

import pytest
from cryptography.exceptions import InvalidTag

from hippius_s3.services.envelope_service import DEK_SIZE_BYTES
from hippius_s3.services.envelope_service import WRAP_NONCE_SIZE_BYTES
from hippius_s3.services.envelope_service import generate_dek
from hippius_s3.services.envelope_service import unwrap_dek
from hippius_s3.services.envelope_service import wrap_dek


_KEK = b"\x11" * 32
_AAD = b"hippius-dek:bkt:obj:1"


def test_generate_dek_is_32_random_bytes() -> None:
    a = generate_dek()
    b = generate_dek()
    assert len(a) == DEK_SIZE_BYTES == 32
    assert a != b  # random


def test_wrap_unwrap_round_trips() -> None:
    dek = generate_dek()
    wrapped = wrap_dek(kek=_KEK, dek=dek, aad=_AAD)
    # nonce(12) || ciphertext+tag(dek + 16)
    assert len(wrapped) == WRAP_NONCE_SIZE_BYTES + DEK_SIZE_BYTES + 16
    assert unwrap_dek(kek=_KEK, wrapped_dek=wrapped, aad=_AAD) == dek


def test_each_wrap_uses_a_fresh_nonce() -> None:
    dek = generate_dek()
    w1 = wrap_dek(kek=_KEK, dek=dek, aad=_AAD)
    w2 = wrap_dek(kek=_KEK, dek=dek, aad=_AAD)
    assert w1 != w2  # non-deterministic nonce
    assert w1[:WRAP_NONCE_SIZE_BYTES] != w2[:WRAP_NONCE_SIZE_BYTES]


def test_wrong_kek_fails_authentication() -> None:
    wrapped = wrap_dek(kek=_KEK, dek=generate_dek(), aad=_AAD)
    with pytest.raises(InvalidTag):
        unwrap_dek(kek=b"\x22" * 32, wrapped_dek=wrapped, aad=_AAD)


def test_wrong_aad_fails_authentication() -> None:
    """AAD binds the envelope to (bucket, object, version) — a different AAD must not authenticate."""
    wrapped = wrap_dek(kek=_KEK, dek=generate_dek(), aad=_AAD)
    with pytest.raises(InvalidTag):
        unwrap_dek(kek=_KEK, wrapped_dek=wrapped, aad=b"hippius-dek:bkt:obj:2")


def test_tampered_ciphertext_fails_authentication() -> None:
    wrapped = bytearray(wrap_dek(kek=_KEK, dek=generate_dek(), aad=_AAD))
    wrapped[-1] ^= 0xFF  # flip a tag byte
    with pytest.raises(InvalidTag):
        unwrap_dek(kek=_KEK, wrapped_dek=bytes(wrapped), aad=_AAD)


@pytest.mark.parametrize("short_len", [0, 1, WRAP_NONCE_SIZE_BYTES, WRAP_NONCE_SIZE_BYTES + 15])
def test_too_short_wrapped_dek_raises_valueerror(short_len: int) -> None:
    with pytest.raises(ValueError, match="wrapped_dek_too_short"):
        unwrap_dek(kek=_KEK, wrapped_dek=os.urandom(short_len), aad=_AAD)
