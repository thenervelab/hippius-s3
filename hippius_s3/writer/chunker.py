from __future__ import annotations

from typing import Any
from typing import AsyncIterator


async def stream_encrypt_to_chunks(
    *,
    plaintext_stream: AsyncIterator[bytes],
    object_id: str,
    part_number: int,
    seed_phrase: str,
    chunk_size: int,
    key_bytes: bytes,
    crypto_service: Any,
) -> AsyncIterator[bytes]:
    """Encrypt a streaming plaintext into fixed-size ciphertext chunks.

    Buffers up to chunk_size to produce one ciphertext chunk at a time.
    """
    buffer = bytearray()
    index = 0
    async for piece in plaintext_stream:
        if not piece:
            continue
        buffer.extend(piece)
        while len(buffer) >= chunk_size:
            pt = bytes(buffer[:chunk_size])
            del buffer[:chunk_size]
            ct = crypto_service.get_adapter(None).encrypt_chunk(
                pt,
                key=key_bytes,
                bucket_id="",
                object_id=object_id,
                part_number=part_number,
                chunk_index=index,
                upload_id="",
            )
            index += 1
            yield ct
    if buffer:
        ct = crypto_service.get_adapter(None).encrypt_chunk(
            bytes(buffer),
            key=key_bytes,
            bucket_id="",
            object_id=object_id,
            part_number=part_number,
            chunk_index=index,
            upload_id="",
        )
        yield ct
