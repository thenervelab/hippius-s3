from __future__ import annotations

import os
from typing import Iterable
from typing import Optional

import psycopg  # type: ignore[import-untyped]

from hippius_s3.services.crypto_service import CryptoService

from .ipfs import fetch_raw_cid


async def get_key_bytes_async(main_account_id: str, bucket_name: str) -> bytes:
    from hippius_s3.services.key_service import get_or_create_encryption_key_bytes  # local import

    return await get_or_create_encryption_key_bytes(
        main_account_id=main_account_id,
        bucket_name=bucket_name,
    )


def decrypt_cipher_chunk(
    ciphertext: bytes,
    *,
    object_id: str,
    part_number: int,
    chunk_index: int,
    key_bytes: bytes,
) -> bytes:
    return CryptoService.decrypt_chunk(
        ciphertext,
        seed_phrase="",
        object_id=object_id,
        part_number=int(part_number),
        chunk_index=int(chunk_index),
        key=key_bytes,
    )


def get_part_id(
    bucket_name: str,
    object_key: str,
    part_number: int = 1,
    *,
    dsn: Optional[str] = None,
) -> str:
    sql = (
        "SELECT p.part_id FROM parts p "
        "JOIN objects o ON o.object_id = p.object_id AND p.object_version = o.current_object_version "
        "JOIN buckets b ON b.bucket_id = o.bucket_id "
        "WHERE b.bucket_name = %s AND o.object_key = %s AND p.part_number = %s "
        "LIMIT 1"
    )
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, (bucket_name, object_key, int(part_number)))
        row = cur.fetchone()
        if not row:
            raise RuntimeError("part_not_found")
        return str(row[0])


def get_part_ec(
    part_id: str,
    *,
    dsn: Optional[str] = None,
) -> Optional[tuple[int, str, int, int, int]]:
    """Return (policy_version, scheme, k, m, stripes) for latest part_ec row, if any."""
    sql = (
        "SELECT policy_version, scheme, k, m, stripes FROM part_ec WHERE part_id = %s ORDER BY updated_at DESC LIMIT 1"
    )
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, (part_id,))
        row = cur.fetchone()
        if not row:
            return None
        return int(row[0]), str(row[1]), int(row[2]), int(row[3]), int(row[4])


def count_blobs_by_kind(
    part_id: str,
    kind: str,
    statuses: Iterable[str] = ("uploaded", "pinning", "pinned"),
    *,
    dsn: Optional[str] = None,
) -> int:
    status_list = tuple(statuses)
    sql = "SELECT COUNT(*) FROM blobs WHERE part_id = %s AND kind = %s AND status = ANY(%s)"
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, (part_id, kind, list(status_list)))
        row = cur.fetchone()
        return int(row[0]) if row else 0


def get_replica_cids_for_chunk(
    part_id: str,
    chunk_index: int,
    *,
    dsn: Optional[str] = None,
) -> list[str]:
    sql = "SELECT cid FROM part_parity_chunks WHERE part_id = %s AND stripe_index = %s ORDER BY parity_index"
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, (part_id, int(chunk_index)))
        return [str(r[0]) for r in cur.fetchall()]


def get_parity_cid(
    part_id: str,
    policy_version: int,
    stripe_index: int,
    parity_index: int = 0,
    *,
    dsn: Optional[str] = None,
) -> Optional[str]:
    sql = (
        "SELECT cid FROM part_parity_chunks WHERE part_id = %s AND policy_version = %s "
        "AND stripe_index = %s AND parity_index = %s LIMIT 1"
    )
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, (part_id, int(policy_version), int(stripe_index), int(parity_index)))
        row = cur.fetchone()
        return str(row[0]) if row and row[0] else None


def get_data_chunk_cids(
    part_id: str,
    *,
    dsn: Optional[str] = None,
) -> list[str]:
    sql = "SELECT cid FROM part_chunks WHERE part_id = %s ORDER BY chunk_index"
    resolved_dsn = dsn or os.environ.get("DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/hippius")
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, (part_id,))
        return [str(r[0]) for r in cur.fetchall()]


def download_cid(cid: str) -> bytes:
    return fetch_raw_cid(cid)


def reconstruct_single_miss_xor(parity: bytes, others: list[bytes]) -> bytes:
    """Given XOR parity over k data blocks and the other (k-1) blocks, reconstruct the missing one.

    Uses symbol_size = max(len(parity), len(others_i)) and virtual zero extension.
    """
    symbol_size = max([len(parity)] + [len(b) for b in others])
    xor_other = bytearray(symbol_size)
    for b in others:
        for i in range(symbol_size):
            xor_other[i] ^= b[i] if i < len(b) else 0
    out = bytearray(symbol_size)
    for i in range(symbol_size):
        pv = parity[i] if i < len(parity) else 0
        out[i] = pv ^ xor_other[i]
    return bytes(out)
