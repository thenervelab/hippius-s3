"""Crypto service with versioned adapters for chunked AEAD encryption/decryption.

Supports:
- Random per-chunk nonces (libsodium default, prevents nonce reuse)
- Streaming encrypt/decrypt for memory-efficient range operations
- Legacy decrypt for backward compatibility

Note: AAD binding helpers exist but are not currently used. SecretBox does not expose
native AAD support; future work could switch to a proper AEAD primitive (ChaCha20-Poly1305)
or implement AAD via explicit MAC computation.
"""

from __future__ import annotations

import hashlib
import hmac
import struct
from abc import ABC
from abc import abstractmethod
from typing import AsyncIterator
from typing import Callable
from typing import Dict
from typing import Iterator
from typing import List
from typing import Optional
from typing import Tuple

from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore[import-not-found]
from nacl.exceptions import CryptoError  # type: ignore[import-not-found]
from nacl.secret import SecretBox  # type: ignore[import-not-found]


class CryptoAdapter(ABC):
    """Base interface for encryption adapters."""

    @property
    @abstractmethod
    def overhead_per_chunk(self) -> int:
        """Bytes of overhead added per chunk (nonce + MAC)."""
        ...

    @abstractmethod
    def encrypt_chunk(
        self,
        plaintext: bytes,
        *,
        key: bytes,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        """Encrypt a single chunk with AEAD."""
        pass

    @abstractmethod
    def decrypt_chunk(
        self,
        ciphertext: bytes,
        *,
        key: bytes,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        """Decrypt a single chunk with AEAD."""
        pass

    @abstractmethod
    def decrypt_chunk_legacy(
        self,
        ciphertext: bytes,
        *,
        key: bytes,
        object_id: str,
        part_number: int,
        chunk_index: int,
    ) -> bytes:
        """Legacy decrypt path for backward compatibility (no AAD)."""
        pass


class SecretBoxChunkedAdapter(CryptoAdapter):
    """XSalsa20-Poly1305 adapter using PyNaCl SecretBox with random nonces.

    Suite ID: hip-enc/legacy
    - Nonce: 24 bytes random (libsodium default, prepended to ciphertext)
    - MAC: 16 bytes Poly1305
    - Total overhead: 40 bytes per chunk

    Random nonces prevent nonce reuse on object rewrites without additional state.
    HKDF/AAD helper methods remain for potential future use with proper AEAD.
    """

    @property
    def overhead_per_chunk(self) -> int:
        return int(SecretBox.NONCE_SIZE + SecretBox.MACBYTES)  # 24 + 16 = 40

    def _derive_nonce_hkdf(
        self,
        key: bytes,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        """Derive unique nonce via HKDF-like construction using HMAC-SHA256.

        Info string includes all stable identifiers to ensure nonce uniqueness.
        Uses simple HKDF-Expand-like construction: HMAC(key, info)[0:nonce_size]
        """
        info = (
            f"hippius-chunk:"
            f"bucket={bucket_id}:"
            f"object={object_id}:"
            f"upload={upload_id}:"
            f"part={part_number}:"
            f"chunk={chunk_index}"
        ).encode("utf-8")

        # Simple HKDF-Expand using HMAC-SHA256
        # PRK=key, Info=info, expand to nonce_size bytes
        return hmac.new(key, info, hashlib.sha256).digest()[: SecretBox.NONCE_SIZE]

    def _derive_nonce_legacy(
        self,
        key: bytes,
        object_id: str,
        part_number: int,
        chunk_index: int,
    ) -> bytes:
        """Legacy nonce derivation for backward compatibility."""
        msg = f"{object_id}:{int(part_number)}:{int(chunk_index)}".encode("utf-8")
        digest = hmac.new(key, msg, hashlib.sha256).digest()
        return digest[: SecretBox.NONCE_SIZE]

    def _build_aad(
        self,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        """Build AAD (Additional Authenticated Data) for this chunk.

        AAD is included in MAC computation but not encrypted.
        Pack as: bucket_id_len | bucket_id | object_id_len | object_id |
                 upload_id_len | upload_id | part_number (u32) | chunk_index (u32)
        """
        parts = []
        for s in [bucket_id, object_id, upload_id]:
            s_bytes = s.encode("utf-8")
            parts.append(struct.pack("<H", len(s_bytes)))
            parts.append(s_bytes)
        parts.append(struct.pack("<II", part_number, chunk_index))
        return b"".join(parts)

    def encrypt_chunk(
        self,
        plaintext: bytes,
        *,
        key: bytes,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        box = SecretBox(key)
        # Random nonce (prepended to ciphertext automatically by SecretBox)
        # Prevents nonce reuse on rewrites without any storage overhead
        ct = box.encrypt(plaintext)
        return bytes(ct)

    def decrypt_chunk(
        self,
        ciphertext: bytes,
        *,
        key: bytes,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        box = SecretBox(key)
        # SecretBox.decrypt reads nonce from first 24 bytes, verifies MAC
        pt = box.decrypt(ciphertext)
        return bytes(pt)

    def decrypt_chunk_legacy(
        self,
        ciphertext: bytes,
        *,
        key: bytes,
        object_id: str,
        part_number: int,
        chunk_index: int,
    ) -> bytes:
        """Legacy decrypt for data encrypted before HKDF nonces."""
        box = SecretBox(key)
        pt = box.decrypt(ciphertext)
        return bytes(pt)


class LegacySecretBoxAdapter(CryptoAdapter):
    """Legacy whole-part SecretBox adapter.

    Suite ID: hip-enc/legacy
    """

    @property
    def overhead_per_chunk(self) -> int:
        return int(SecretBox.NONCE_SIZE + SecretBox.MACBYTES)

    def encrypt_chunk(
        self,
        plaintext: bytes,
        *,
        key: bytes,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        box = SecretBox(key)
        ct = box.encrypt(plaintext)
        return bytes(ct)

    def decrypt_chunk(
        self,
        ciphertext: bytes,
        *,
        key: bytes,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        box = SecretBox(key)
        return bytes(box.decrypt(ciphertext))

    def decrypt_chunk_legacy(
        self,
        ciphertext: bytes,
        *,
        key: bytes,
        object_id: str,
        part_number: int,
        chunk_index: int,
    ) -> bytes:
        return self.decrypt_chunk(
            ciphertext,
            key=key,
            bucket_id="",
            object_id=object_id,
            part_number=part_number,
            chunk_index=chunk_index,
            upload_id="",
        )


class AESGCMChunkedAdapter(CryptoAdapter):
    """AES-256-GCM adapter with per-chunk nonces and AAD binding.

    Deprecated internal variant kept for compatibility.
    - Nonce: 12 bytes (prepended to ciphertext)
    - Tag: 16 bytes (appended by AESGCM)
    - Total overhead: 28 bytes per chunk

    Note: This variant binds upload_id into nonce/AAD. This is easy to mismatch across
    code paths (e.g. simple PUT vs append/MPU), so prefer hip-enc/aes256gcm for new writes.
    """

    NONCE_SIZE = 12
    TAG_SIZE = 16

    @property
    def overhead_per_chunk(self) -> int:
        return int(self.NONCE_SIZE + self.TAG_SIZE)

    def _build_aad(
        self,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        # Keep format stable and compact; bound to auth tag.
        parts = []
        for s in [bucket_id, object_id, upload_id]:
            s_bytes = s.encode("utf-8")
            parts.append(struct.pack("<H", len(s_bytes)))
            parts.append(s_bytes)
        parts.append(struct.pack("<II", int(part_number), int(chunk_index)))
        return b"".join(parts)

    def _derive_nonce(
        self,
        key: bytes,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        """Derive a deterministic nonce to avoid reliance on RNG quality at extreme scale."""
        info = (
            f"hippius-aesgcm-nonce:"
            f"bucket={bucket_id}:"
            f"object={object_id}:"
            f"upload={upload_id}:"
            f"part={int(part_number)}:"
            f"chunk={int(chunk_index)}"
        ).encode("utf-8")
        return hmac.new(key, info, hashlib.sha256).digest()[: self.NONCE_SIZE]

    def encrypt_chunk(
        self,
        plaintext: bytes,
        *,
        key: bytes,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        aad = self._build_aad(bucket_id, object_id, int(part_number), int(chunk_index), upload_id)
        nonce = self._derive_nonce(key, bucket_id, object_id, int(part_number), int(chunk_index), upload_id)
        ct = AESGCM(key).encrypt(nonce, plaintext, aad)
        return nonce + ct

    def decrypt_chunk(
        self,
        ciphertext: bytes,
        *,
        key: bytes,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        if len(ciphertext) < self.NONCE_SIZE + self.TAG_SIZE:
            raise CryptoError("ciphertext_too_short")
        nonce = ciphertext[: self.NONCE_SIZE]
        body = ciphertext[self.NONCE_SIZE :]
        aad = self._build_aad(bucket_id, object_id, int(part_number), int(chunk_index), upload_id)
        return bytes(AESGCM(key).decrypt(nonce, body, aad))

    def decrypt_chunk_legacy(
        self,
        ciphertext: bytes,
        *,
        key: bytes,
        object_id: str,
        part_number: int,
        chunk_index: int,
    ) -> bytes:
        # No legacy format for AESGCM suite; keep as strict failure.
        raise CryptoError("decrypt_failed")


class AESGCMChunkedAdapterV2(AESGCMChunkedAdapter):
    """AES-256-GCM adapter variant without upload_id in nonce/AAD.

    Suite ID: hip-enc/aes256gcm
    """

    def _build_aad(
        self,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        parts = []
        for s in [bucket_id, object_id]:
            s_bytes = s.encode("utf-8")
            parts.append(struct.pack("<H", len(s_bytes)))
            parts.append(s_bytes)
        parts.append(struct.pack("<II", int(part_number), int(chunk_index)))
        return b"".join(parts)

    def _derive_nonce(
        self,
        key: bytes,
        bucket_id: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        upload_id: str,
    ) -> bytes:
        info = (
            f"hippius-aesgcm-nonce-v2:"
            f"bucket={bucket_id}:"
            f"object={object_id}:"
            f"part={int(part_number)}:"
            f"chunk={int(chunk_index)}"
        ).encode("utf-8")
        return hmac.new(key, info, hashlib.sha256).digest()[: self.NONCE_SIZE]


class CryptoService:
    """Unified crypto service with adapter-based encryption/decryption.

    Key features:
    - HKDF-derived nonces per chunk
    - AAD binding via nonce derivation (object_id, part_number, chunk_index, upload_id, bucket_id)
    - Streaming decrypt for memory-efficient range operations
    - Legacy decrypt path for backward compatibility
    """

    # Adapter registry
    _ADAPTERS: Dict[str, CryptoAdapter] = {
        "hip-enc/legacy": LegacySecretBoxAdapter(),
        "hip-enc/aes256gcm": AESGCMChunkedAdapterV2(),
    }

    # Default suite for new writes
    DEFAULT_SUITE_ID = "hip-enc/legacy"

    # Backward compat: exposed for existing code
    OVERHEAD_PER_CHUNK = SecretBox.NONCE_SIZE + SecretBox.MACBYTES  # 40 bytes

    @classmethod
    def is_supported_suite_id(cls, suite_id: Optional[str]) -> bool:
        if not suite_id:
            return False
        return suite_id in cls._ADAPTERS

    @classmethod
    def get_adapter(cls, suite_id: Optional[str] = None) -> CryptoAdapter:
        """Get adapter for the given suite ID."""
        if not suite_id or suite_id not in cls._ADAPTERS:
            suite_id = cls.DEFAULT_SUITE_ID
        return cls._ADAPTERS[suite_id]

    @staticmethod
    def derive_key_from_seed(seed_phrase: str) -> bytes:
        """Derive 32-byte encryption key from seed phrase.

        Uses SHA-256 for backward compatibility.
        Future: use HKDF with salt from bucket/object context.
        """
        return hashlib.sha256(seed_phrase.encode("utf-8")).digest()

    # Legacy aliases for backward compatibility
    @staticmethod
    def _derive_key(seed_phrase: str) -> bytes:
        return CryptoService.derive_key_from_seed(seed_phrase)

    @classmethod
    def encrypt_part_to_chunks(
        cls,
        plaintext: bytes,
        *,
        object_id: str,
        part_number: int,
        seed_phrase: str,
        chunk_size: int,
        bucket_id: str = "",
        upload_id: str = "",
        suite_id: Optional[str] = None,
        key: Optional[bytes] = None,
    ) -> List[bytes]:
        """Encrypt plaintext into fixed-size chunks.

        Args:
            plaintext: Data to encrypt
            object_id: Object UUID
            part_number: Part number (1-based)
            seed_phrase: Seed phrase for key derivation (ignored if key provided)
            chunk_size: Plaintext bytes per chunk
            bucket_id: Bucket UUID (for AAD)
            upload_id: Upload UUID (for AAD)
            suite_id: Encryption suite (defaults to configured/legacy)
            key: Optional raw 32-byte key to use instead of seed derivation

        Returns:
            List of ciphertext chunks (each chunk_size + overhead)
        """
        key_bytes = key if key is not None else cls.derive_key_from_seed(seed_phrase)
        adapter = cls.get_adapter(suite_id)

        chunks: List[bytes] = []
        total = len(plaintext)
        if total == 0:
            return []

        num_chunks = (total + chunk_size - 1) // chunk_size
        for i in range(num_chunks):
            start = i * chunk_size
            end = min(start + chunk_size, total)
            ct = adapter.encrypt_chunk(
                plaintext[start:end],
                key=key_bytes,
                bucket_id=bucket_id,
                object_id=object_id,
                part_number=part_number,
                chunk_index=i,
                upload_id=upload_id,
            )
            chunks.append(ct)
        return chunks

    @classmethod
    def decrypt_chunk(
        cls,
        ciphertext_chunk: bytes,
        *,
        seed_phrase: str,
        object_id: str,
        part_number: int,
        chunk_index: int,
        bucket_id: str = "",
        upload_id: str = "",
        suite_id: Optional[str] = None,
        key: Optional[bytes] = None,
    ) -> bytes:
        """Decrypt a single chunk."""
        key_bytes = key if key is not None else cls.derive_key_from_seed(seed_phrase)
        adapter = cls.get_adapter(suite_id)

        # Try new path first
        try:
            return adapter.decrypt_chunk(
                ciphertext_chunk,
                key=key_bytes,
                bucket_id=bucket_id,
                object_id=object_id,
                part_number=part_number,
                chunk_index=chunk_index,
                upload_id=upload_id,
            )
        except Exception:
            # Fall back to legacy path
            try:
                return adapter.decrypt_chunk_legacy(
                    ciphertext_chunk,
                    key=key_bytes,
                    object_id=object_id,
                    part_number=part_number,
                    chunk_index=chunk_index,
                )
            except Exception as exc:
                raise CryptoError("decrypt_failed") from exc

    @classmethod
    def decrypt_part_auto(
        cls,
        ciphertext: bytes,
        *,
        seed_phrase: str,
        object_id: str,
        part_number: int,
        chunk_count: Optional[int] = None,
        chunk_loader: Optional[Callable[[int], bytes]] = None,
        bucket_id: str = "",
        upload_id: str = "",
        suite_id: Optional[str] = None,
    ) -> bytes:
        """Decrypt a part that may be whole-part or chunked.

        Tries whole-part decrypt first (legacy), then per-chunk decrypt.
        """
        key = cls.derive_key_from_seed(seed_phrase)

        # Try whole-part decrypt first (legacy sealed box)
        try:
            legacy_adapter = cls.get_adapter("hip-enc/legacy")
            return legacy_adapter.decrypt_chunk(
                ciphertext,
                key=key,
                bucket_id=bucket_id,
                object_id=object_id,
                part_number=part_number,
                chunk_index=0,
                upload_id=upload_id,
            )
        except Exception:
            pass

        # Fall back to per-chunk decrypt if loader provided
        if chunk_count is not None and chunk_loader is not None:
            out: list[bytes] = []
            for ci in range(int(chunk_count)):
                ct = chunk_loader(ci)
                if not isinstance(ct, (bytes, bytearray)):
                    raise CryptoError("missing_chunk")
                pt = cls.decrypt_chunk(
                    ct,
                    seed_phrase=seed_phrase,
                    object_id=object_id,
                    part_number=part_number,
                    chunk_index=ci,
                    bucket_id=bucket_id,
                    upload_id=upload_id,
                    suite_id=suite_id,
                )
                out.append(pt)
            return b"".join(out)

        raise CryptoError("decrypt_failed")

    @classmethod
    def decrypt_stream(
        cls,
        chunk_iterator: Iterator[Tuple[int, bytes]],
        *,
        seed_phrase: str,
        object_id: str,
        part_number: int,
        bucket_id: str = "",
        upload_id: str = "",
        suite_id: Optional[str] = None,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
    ) -> Iterator[bytes]:
        """Streaming decrypt that yields plaintext slices.

        Memory-efficient: decrypts one chunk at a time and yields only the requested range.

        Args:
            chunk_iterator: Iterator yielding (chunk_index, ciphertext) tuples
            seed_phrase: Seed for key derivation
            object_id: Object UUID
            part_number: Part number
            bucket_id: Bucket UUID (for AAD)
            upload_id: Upload UUID (for AAD)
            suite_id: Encryption suite
            range_start: Starting byte offset (plaintext)
            range_end: Ending byte offset inclusive (plaintext)

        Yields:
            Plaintext byte slices covering the requested range
        """
        plaintext_offset = 0

        for chunk_index, ct_chunk in chunk_iterator:
            # Decrypt this chunk
            pt_chunk = cls.decrypt_chunk(
                ct_chunk,
                seed_phrase=seed_phrase,
                object_id=object_id,
                part_number=part_number,
                chunk_index=chunk_index,
                bucket_id=bucket_id,
                upload_id=upload_id,
                suite_id=suite_id,
            )

            chunk_start = plaintext_offset
            chunk_end = plaintext_offset + len(pt_chunk) - 1
            plaintext_offset += len(pt_chunk)

            # Determine slice within this chunk
            if range_start is not None and range_end is not None:
                # Check if this chunk intersects the range
                if chunk_end < range_start or chunk_start > range_end:
                    continue  # Skip this chunk

                # Compute slice bounds within chunk
                slice_start = max(0, range_start - chunk_start)
                slice_end = min(len(pt_chunk), range_end - chunk_start + 1)
                yield pt_chunk[slice_start:slice_end]
            else:
                # No range: yield entire chunk
                yield pt_chunk

    @classmethod
    async def decrypt_stream_async(
        cls,
        chunk_iterator: AsyncIterator[Tuple[int, bytes]],
        *,
        seed_phrase: str,
        object_id: str,
        part_number: int,
        bucket_id: str = "",
        upload_id: str = "",
        suite_id: Optional[str] = None,
        range_start: Optional[int] = None,
        range_end: Optional[int] = None,
    ) -> AsyncIterator[bytes]:
        """Async version of decrypt_stream for use in async contexts."""
        plaintext_offset = 0

        async for chunk_index, ct_chunk in chunk_iterator:
            # Decrypt this chunk (CPU-bound, but fast enough for now)
            pt_chunk = cls.decrypt_chunk(
                ct_chunk,
                seed_phrase=seed_phrase,
                object_id=object_id,
                part_number=part_number,
                chunk_index=chunk_index,
                bucket_id=bucket_id,
                upload_id=upload_id,
                suite_id=suite_id,
            )

            chunk_start = plaintext_offset
            chunk_end = plaintext_offset + len(pt_chunk) - 1
            plaintext_offset += len(pt_chunk)

            # Determine slice within this chunk
            if range_start is not None and range_end is not None:
                if chunk_end < range_start or chunk_start > range_end:
                    continue

                slice_start = max(0, range_start - chunk_start)
                slice_end = min(len(pt_chunk), range_end - chunk_start + 1)
                yield pt_chunk[slice_start:slice_end]
            else:
                yield pt_chunk
