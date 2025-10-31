from __future__ import annotations

import base64
import hashlib
import json
import os
from typing import Iterable
from typing import Optional

import psycopg  # type: ignore[import-untyped]
import requests

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
    env = os.environ.get("DATABASE_URL")
    resolved_dsn: str = (
        dsn
        if dsn is not None
        else (env if env is not None else "postgresql://postgres:postgres@localhost:5432/hippius")
    )
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
    env = os.environ.get("DATABASE_URL")
    resolved_dsn: str = (
        dsn
        if dsn is not None
        else (env if env is not None else "postgresql://postgres:postgres@localhost:5432/hippius")
    )
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
    env = os.environ.get("DATABASE_URL")
    resolved_dsn: str = (
        dsn
        if dsn is not None
        else (env if env is not None else "postgresql://postgres:postgres@localhost:5432/hippius")
    )
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
    env = os.environ.get("DATABASE_URL")
    resolved_dsn: str = (
        dsn
        if dsn is not None
        else (env if env is not None else "postgresql://postgres:postgres@localhost:5432/hippius")
    )
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
        "AND stripe_index = %s AND parity_index = %s ORDER BY created_at DESC LIMIT 1"
    )
    env = os.environ.get("DATABASE_URL")
    resolved_dsn: str = (
        dsn
        if dsn is not None
        else (env if env is not None else "postgresql://postgres:postgres@localhost:5432/hippius")
    )
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, (part_id, int(policy_version), int(stripe_index), int(parity_index)))
        row = cur.fetchone()
        cid = str(row[0]) if row and row[0] else None
        if cid:
            print(
                "DEBUG get_parity_cid:",
                {
                    "part_id": part_id,
                    "pv": int(policy_version),
                    "stripe": int(stripe_index),
                    "parity_index": int(parity_index),
                    "cid": cid,
                },
            )
        return cid


def get_data_chunk_cids(
    part_id: str,
    *,
    dsn: Optional[str] = None,
) -> list[str]:
    sql = "SELECT cid FROM part_chunks WHERE part_id = %s ORDER BY chunk_index"
    env = os.environ.get("DATABASE_URL")
    resolved_dsn: str = (
        dsn
        if dsn is not None
        else (env if env is not None else "postgresql://postgres:postgres@localhost:5432/hippius")
    )
    with psycopg.connect(resolved_dsn) as conn, conn.cursor() as cur:
        cur.execute(sql, (part_id,))
        return [str(r[0]) for r in cur.fetchall()]


def download_cid(cid: str) -> bytes:
    return fetch_raw_cid(cid)


def reconstruct_single_miss_rs(
    parity_frags: list[bytes],
    others: list[bytes],
    *,
    k: int,
    missing_index: int,
    expected_payload_size: int | None = None,
) -> bytes:
    """Reconstruct a single missing data fragment using Reed–Solomon via PyECLib.

    Parameters:
    - parity_frags: list of parity fragments (length m)
    - others: data fragments for the stripe excluding the missing one, in index order
    - k: number of data fragments in this stripe
    - missing_index: 0-based index of the missing data fragment within the stripe
    """
    if k <= 0:
        return b""
    m = max(1, len(parity_frags))
    original_parity_bundle = bytes(parity_frags[0]) if parity_frags else b""
    # Detect recovery bundle (RSB1) and extract headers + parity
    data_headers: list[bytes] | None = None
    if parity_frags and len(parity_frags[0]) >= 8 and bytes(parity_frags[0][:4]) == b"RSB1":
        import struct

        buf = memoryview(parity_frags[0])
        off = 4
        man_len = struct.unpack_from(">I", buf, off)[0]
        off += 4
        man = json.loads(bytes(buf[off : off + man_len]).decode())
        off += man_len
        num_hdrs = struct.unpack_from(">I", buf, off)[0]
        off += 4
        headers: list[bytes] = []
        for _ in range(num_hdrs):
            length = struct.unpack_from(">I", buf, off)[0]
            off += 4
            headers.append(bytes(buf[off : off + length]))
            off += length
        num_par = struct.unpack_from(">I", buf, off)[0]
        off += 4
        pars: list[bytes] = []
        for _ in range(num_par):
            length = struct.unpack_from(">I", buf, off)[0]
            off += 4
            pars.append(bytes(buf[off : off + length]))
            off += length
        data_headers = headers
        parity_frags = pars
        symbol_size = int(man.get("symbol_size", 0))
    else:
        # Prefer symbol size from parity to avoid drift; fallback to max observed length
        symbol_size = len(parity_frags[0]) if parity_frags and len(parity_frags[0]) > 0 else 0
    if symbol_size == 0:
        for b in others:
            symbol_size = max(symbol_size, len(b))
        for p in parity_frags:
            symbol_size = max(symbol_size, len(p))

    def pad(b: bytes) -> bytes:
        return b.ljust(symbol_size, b"\x00")[:symbol_size]

    def _hex_sample(b: bytes, n: int = 8) -> str:
        if not isinstance(b, (bytes, bytearray)):
            return "<none>"
        if len(b) <= 2 * n:
            return bytes(b).hex()
        return f"{bytes(b)[:n].hex()}...{bytes(b)[-n:].hex()}"

    def _md5(b: bytes) -> str:
        return hashlib.md5(b).hexdigest() if isinstance(b, (bytes, bytearray)) else ""

    print(
        "DEBUG RS reconstruct:",
        {
            "backend": "liberasurecode_rs_vand",
            "k": int(k),
            "m": int(m),
            "missing_index": int(missing_index),
            "symbol_size": int(symbol_size),
            "parity_lens": [len(p) for p in parity_frags],
            "others_lens": [len(b) for b in others],
            "parity_md5": [_md5(p) for p in parity_frags],
            "others_md5": [_md5(b) for b in others],
            "parity_head": [_hex_sample(p) for p in parity_frags],
            "others_head": [_hex_sample(b) for b in others],
        },
    )

    data_frags: list[bytes] = []
    it = iter(others)
    for i in range(k):
        if i == int(missing_index):
            # For liberasurecode backend, use zero-padded placeholder to satisfy length checks
            if data_headers is not None and i < len(data_headers):
                data_frags.append(data_headers[i] + pad(b""))
            else:
                data_frags.append(pad(b""))
        else:
            try:
                plain = pad(next(it))
                if data_headers is not None and i < len(data_headers):
                    data_frags.append(data_headers[i] + plain)
                else:
                    data_frags.append(plain)
            except StopIteration:
                if data_headers is not None and i < len(data_headers):
                    data_frags.append(data_headers[i] + pad(b""))
                else:
                    data_frags.append(pad(b""))
    # Prefer sidecar ISA-L service if configured via EC_HTTP_URL
    sidecar = os.environ.get("EC_HTTP_URL")
    if sidecar:
        try:
            parity_bundle = original_parity_bundle
            payload = {
                "k": int(k),
                "m": int(m),
                "missing_index": int(missing_index),
                # Post the single recovery bundle as parity[0]
                "parity": [base64.b64encode(parity_bundle).decode("ascii")],
                "others": [base64.b64encode(o).decode("ascii") for o in others],
            }
            url = f"{sidecar.rstrip('/')}/reconstruct"
            # Extra debug: confirm we are posting the recovery bundle
            if isinstance(parity_bundle, (bytes, bytearray)):
                head = bytes(parity_bundle[:4]).hex()
                print(
                    "DEBUG sidecar post:",
                    {
                        "parity0_len": len(parity_bundle),
                        "parity0_head": head,
                    },
                )
            resp = requests.post(url, json=payload, timeout=10)
            try:
                resp.raise_for_status()
            except Exception as http_err:  # requests.HTTPError or similar
                body = None
                try:
                    body = resp.text
                except Exception:
                    body = ""
                print(
                    "DEBUG sidecar reconstruct HTTP error:",
                    {
                        "status": getattr(resp, "status_code", None),
                        "body": body[:512] if body else body,
                        "url": url,
                    },
                )
                raise http_err
            data = resp.json()
            return base64.b64decode(str(data.get("reconstructed", "")))
        except Exception as e:
            print("DEBUG sidecar reconstruct error:", {"type": type(e).__name__, "msg": str(e)})
            # fall back to local ISA-L below
    # Tests require the sidecar; no local fallback
    raise RuntimeError("ec_sidecar_required: set EC_HTTP_URL to the redundancy sidecar endpoint for tests")


def reconstruct_single_miss(
    parity: bytes,
    others: list[bytes],
    *,
    k: int,
    missing_index: int,
    expected_payload_size: int | None = None,
) -> bytes:
    """Try RS reconstruction via PyECLib; fall back to XOR when RS is unavailable.

    This keeps local pytest runs working where PyECLib may not be present.
    """
    out = reconstruct_single_miss_rs(
        [parity],
        others,
        k=int(k),
        missing_index=int(missing_index),
        expected_payload_size=expected_payload_size,
    )
    print(
        "DEBUG reconstruct_single_miss:",
        {
            "out_len": len(out),
            "out_md5": hashlib.md5(out).hexdigest() if isinstance(out, (bytes, bytearray)) else "",
        },
    )
    return out
