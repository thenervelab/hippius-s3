from __future__ import annotations

import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM


DEK_SIZE_BYTES = 32
WRAP_NONCE_SIZE_BYTES = 12


def generate_dek() -> bytes:
    """Generate a random 256-bit DEK."""
    return os.urandom(DEK_SIZE_BYTES)


def wrap_dek(*, kek: bytes, dek: bytes, aad: bytes) -> bytes:
    """Wrap DEK under KEK using AES-256-GCM.

    Format: nonce(12) || ciphertext_with_tag
    """
    nonce = os.urandom(WRAP_NONCE_SIZE_BYTES)
    ct = AESGCM(kek).encrypt(nonce, dek, aad)
    return nonce + ct


def unwrap_dek(*, kek: bytes, wrapped_dek: bytes, aad: bytes) -> bytes:
    """Unwrap DEK from nonce||ciphertext using AES-256-GCM."""
    if len(wrapped_dek) < WRAP_NONCE_SIZE_BYTES + 16:
        raise ValueError("wrapped_dek_too_short")
    nonce = wrapped_dek[:WRAP_NONCE_SIZE_BYTES]
    body = wrapped_dek[WRAP_NONCE_SIZE_BYTES:]
    return bytes(AESGCM(kek).decrypt(nonce, body, aad))
