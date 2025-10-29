from __future__ import annotations

import logging
from typing import Iterable
from typing import Optional

import asyncpg
import redis.asyncio as async_redis

from hippius_s3.cache.object_parts import ObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.ec.codec import encode_rs_systematic
from hippius_s3.ipfs_service import IPFSService


logger = logging.getLogger(__name__)


async def _get_part_info(
    conn: asyncpg.Connection, object_id: str, object_version: int, part_number: int
) -> Optional[str]:
    row = await conn.fetchrow(
        """
        SELECT p.part_id
        FROM parts p
        WHERE p.object_id = $1 AND p.object_version = $2 AND p.part_number = $3
        LIMIT 1
        """,
        object_id,
        int(object_version),
        int(part_number),
    )
    return str(row[0]) if row else None


async def _get_latest_ec_policy(conn: asyncpg.Connection, part_id: str) -> Optional[tuple[int, int, int, int]]:
    row = await conn.fetchrow(
        """
        SELECT policy_version, k, m, stripes
        FROM part_ec
        WHERE part_id = $1 AND scheme = 'rs-v1' AND state = 'complete'
        ORDER BY policy_version DESC
        LIMIT 1
        """,
        part_id,
    )
    if not row:
        return None
    return int(row[0]), int(row[1]), int(row[2]), int(row[3])


async def _get_parity_cid(
    conn: asyncpg.Connection, part_id: str, policy_version: int, stripe_index: int
) -> Optional[str]:
    row = await conn.fetchrow(
        """
        SELECT cid
        FROM part_parity_chunks
        WHERE part_id = $1 AND policy_version = $2 AND stripe_index = $3 AND parity_index = 0
        LIMIT 1
        """,
        part_id,
        int(policy_version),
        int(stripe_index),
    )
    return str(row[0]) if row and row[0] else None


async def try_recover_missing_chunks(
    *,
    db: asyncpg.Connection,
    redis_client: async_redis.Redis,
    obj_cache: ObjectPartsCache,
    object_id: str,
    object_version: int,
    part_number: int,
    missing_chunk_indices: Iterable[int],
    bucket_name: str,
    address: str,
) -> list[int]:
    """Attempt EC recovery for missing ciphertext data chunks.

    Currently supports rs-v1 with m>=1 and single-miss per stripe via XOR parity.
    If recovery conditions are not met, leaves chunks untouched.
    """
    recovered: list[int] = []
    cfg = get_config()
    part_id = await _get_part_info(db, object_id, int(object_version), int(part_number))
    if not part_id:
        return recovered

    pv_k_m = await _get_latest_ec_policy(db, part_id)
    if not pv_k_m:
        return recovered
    policy_version, k, m, stripes = pv_k_m
    if m <= 0 or k <= 0:
        return recovered
    if m != 1:
        return recovered

    ipfs = IPFSService(cfg, redis_client)

    # Group misses by stripe
    misses = sorted({int(i) for i in missing_chunk_indices})
    for ci in misses:
        stripe_index = int(ci // k)
        # Only attempt simple XOR recovery when exactly one data chunk is missing in this stripe
        stripe_indices = list(range(stripe_index * k, min((stripe_index + 1) * k, stripe_index * k + k)))
        other_indices = [i for i in stripe_indices if i != ci]

        # All other data chunks must be present in cache
        other_bytes = []
        all_present = True
        for oi in other_indices:
            data = await obj_cache.get_chunk(object_id, int(object_version), int(part_number), int(oi))
            if not isinstance(data, (bytes, bytearray)):
                all_present = False
                break
            other_bytes.append(bytes(data))
        if not all_present:
            continue

        # Need at least one parity shard; use parity_index=0 for now
        cid = await _get_parity_cid(db, part_id, int(policy_version), int(stripe_index))
        if not cid:
            continue
        try:
            parity_bytes = await ipfs.download_file(cid, subaccount_id=address, bucket_name=bucket_name, decrypt=False)
            parity_symbol = bytes(parity_bytes)
        except Exception:
            logger.warning("ec_recover: failed to fetch parity from IPFS")
            continue

        symbol_size = max([len(parity_symbol)] + [len(b) for b in other_bytes])
        # XOR parity with other data to reconstruct the missing data
        blocks = other_bytes  # Missing block is derived from parity ^ XOR(other)
        xor_parity = encode_rs_systematic(blocks, k=max(1, k - 1), m=1, symbol_size=symbol_size)[0]
        out = bytearray(symbol_size)
        for i in range(symbol_size):
            pv = parity_symbol[i] if i < len(parity_symbol) else 0
            xv = xor_parity[i]
            out[i] = pv ^ xv

        try:
            await obj_cache.set_chunk(
                object_id,
                int(object_version),
                int(part_number),
                int(ci),
                bytes(out),
                ttl=int(cfg.cache_ttl_seconds),
            )
            recovered.append(int(ci))
        except Exception:
            logger.warning("ec_recover: failed to write reconstructed chunk to cache")

    return recovered
