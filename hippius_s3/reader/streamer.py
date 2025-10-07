from __future__ import annotations

import contextlib
from typing import Any
from typing import AsyncGenerator
from typing import Dict
from typing import Iterable

from .decrypter import decrypt_chunk_if_needed
from .decrypter import maybe_slice
from .fetcher import fetch_chunk_blocking
from .types import ChunkPlanItem


async def stream_plan(
    *,
    obj_cache: Any,
    object_id: str,
    plan: Iterable[ChunkPlanItem],
    should_decrypt: bool,
    seed_phrase: str,
    sleep_seconds: float,
    address: str = "",
    bucket_name: str = "",
) -> AsyncGenerator[bytes, None]:
    legacy_pt_by_part: Dict[int, bytes] = {}
    legacy_chunk_size_by_part: Dict[int, int] = {}
    for item in plan:
        c = await fetch_chunk_blocking(
            obj_cache, object_id, int(item.part_number), int(item.chunk_index), sleep_seconds=sleep_seconds
        )
        try:
            pt = await decrypt_chunk_if_needed(
                should_decrypt,
                c,
                seed_phrase=seed_phrase,
                object_id=object_id,
                part_number=int(item.part_number),
                chunk_index=int(item.chunk_index),
                address=address,
                bucket_name=bucket_name,
            )
        except Exception:
            pn = int(item.part_number)
            if pn not in legacy_pt_by_part:
                # Read whole part ciphertext
                ct_chunks: list[bytes] = []
                # Try meta for num_chunks & chunk_size
                num_chunks = 0
                chunk_size = 4 * 1024 * 1024
                try:
                    meta = await obj_cache.get_meta(object_id, pn)  # type: ignore[attr-defined]
                except Exception:
                    meta = None
                if isinstance(meta, dict):
                    try:
                        num_chunks = int(meta.get("num_chunks", 0))
                    except Exception:
                        num_chunks = 0
                    with contextlib.suppress(Exception):
                        chunk_size = int(meta.get("chunk_size", chunk_size))
                idx = 0
                while True:
                    if num_chunks and idx >= num_chunks:
                        break
                    cc = await obj_cache.get_chunk(object_id, pn, idx)  # type: ignore[attr-defined]
                    if cc is None:
                        if num_chunks == 0:
                            break
                        else:
                            break
                    ct_chunks.append(cc)
                    idx += 1
                import base64 as _b64

                import nacl.secret as _secret

                from hippius_s3.ipfs_service import get_encryption_key  # local import

                key_b64 = await get_encryption_key(f"{address}:{bucket_name}")
                key = _b64.b64decode(key_b64)
                box = _secret.SecretBox(key)
                legacy_pt_by_part[pn] = bytes(box.decrypt(b"".join(ct_chunks)))
                legacy_chunk_size_by_part[pn] = int(chunk_size)
            full_pt = legacy_pt_by_part[pn]
            chunk_size_pt = legacy_chunk_size_by_part[pn]
            start = int(item.chunk_index) * int(chunk_size_pt)
            end_excl = min(start + int(chunk_size_pt), len(full_pt))
            pt = full_pt[start:end_excl]
        yield maybe_slice(pt, item.slice_start, item.slice_end_excl)
