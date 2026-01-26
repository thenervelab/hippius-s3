"""Unit tests for local KEK wrapper (development fallback)."""

import os

import pytest

from hippius_s3.services.local_kek_wrapper import LOCAL_WRAP_KEY_ID
from hippius_s3.services.local_kek_wrapper import unwrap_key_local
from hippius_s3.services.local_kek_wrapper import wrap_key_local


class TestLocalKEKWrapper:
    """Tests for local AES-GCM KEK wrapping."""

    def test_wrap_unwrap_roundtrip(self):
        """Wrapped KEK can be unwrapped with same secret."""
        plaintext_kek = os.urandom(32)
        secret = "test-secret-key-0123456789abcdef"

        wrapped = wrap_key_local(plaintext_kek, secret)
        unwrapped = unwrap_key_local(wrapped, secret)

        assert unwrapped == plaintext_kek

    def test_wrapped_is_different_from_plaintext(self):
        """Wrapped bytes should not equal plaintext."""
        plaintext_kek = os.urandom(32)
        secret = "test-secret"

        wrapped = wrap_key_local(plaintext_kek, secret)

        assert wrapped != plaintext_kek
        # Wrapped includes nonce (12 bytes) + ciphertext (32 bytes) + tag (16 bytes)
        assert len(wrapped) == 12 + 32 + 16

    def test_wrong_secret_fails_unwrap(self):
        """Unwrap with wrong secret raises ValueError."""
        plaintext_kek = os.urandom(32)
        secret = "correct-secret"
        wrong_secret = "wrong-secret"

        wrapped = wrap_key_local(plaintext_kek, secret)

        with pytest.raises(ValueError, match="authentication failed"):
            unwrap_key_local(wrapped, wrong_secret)

    def test_tampered_wrapped_fails_unwrap(self):
        """Tampered wrapped bytes raises ValueError."""
        plaintext_kek = os.urandom(32)
        secret = "test-secret"

        wrapped = wrap_key_local(plaintext_kek, secret)

        # Tamper with ciphertext portion
        tampered = bytearray(wrapped)
        tampered[20] ^= 0xFF  # Flip bits in ciphertext
        tampered = bytes(tampered)

        with pytest.raises(ValueError, match="authentication failed"):
            unwrap_key_local(tampered, secret)

    def test_truncated_wrapped_fails(self):
        """Truncated wrapped bytes raises ValueError."""
        plaintext_kek = os.urandom(32)
        secret = "test-secret"

        wrapped = wrap_key_local(plaintext_kek, secret)

        # Truncate to less than nonce + tag
        truncated = wrapped[:10]

        with pytest.raises(ValueError, match="too short"):
            unwrap_key_local(truncated, secret)

    def test_each_wrap_produces_unique_ciphertext(self):
        """Multiple wraps of same plaintext produce different ciphertext (nonce uniqueness)."""
        plaintext_kek = os.urandom(32)
        secret = "test-secret"

        wrapped1 = wrap_key_local(plaintext_kek, secret)
        wrapped2 = wrap_key_local(plaintext_kek, secret)

        # Should be different due to random nonce
        assert wrapped1 != wrapped2

        # Both should unwrap to same plaintext
        assert unwrap_key_local(wrapped1, secret) == plaintext_kek
        assert unwrap_key_local(wrapped2, secret) == plaintext_kek

    def test_local_wrap_key_id_constant(self):
        """LOCAL_WRAP_KEY_ID is the expected value."""
        assert LOCAL_WRAP_KEY_ID == "local"

    def test_different_key_sizes(self):
        """Can wrap/unwrap different key sizes."""
        secret = "test-secret"

        for size in [16, 24, 32, 64]:
            plaintext = os.urandom(size)
            wrapped = wrap_key_local(plaintext, secret)
            unwrapped = unwrap_key_local(wrapped, secret)
            assert unwrapped == plaintext

    def test_empty_secret_works(self):
        """Empty secret is allowed (though not recommended)."""
        plaintext_kek = os.urandom(32)
        secret = ""

        wrapped = wrap_key_local(plaintext_kek, secret)
        unwrapped = unwrap_key_local(wrapped, secret)

        assert unwrapped == plaintext_kek
