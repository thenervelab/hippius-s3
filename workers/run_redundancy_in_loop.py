#!/usr/bin/env python3
import asyncio
import base64
import contextlib
import json
import logging
import math
import os
import sys
import threading
from pathlib import Path

import asyncpg
import redis.asyncio as async_redis
from redis.exceptions import BusyLoadingError
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import TimeoutError as RedisTimeoutError


# Add parent directory to path to import hippius_s3 modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from hippius_s3.cache import FileSystemPartsStore
from hippius_s3.cache import RedisObjectPartsCache
from hippius_s3.config import get_config
from hippius_s3.logging_config import setup_loki_logging
from hippius_s3.monitoring import initialize_metrics_collector
from hippius_s3.queue import ParityStagedItem
from hippius_s3.queue import ParityUploadRequest
from hippius_s3.queue import RedundancyRequest
from hippius_s3.queue import ReplicaStagedItem
from hippius_s3.queue import ReplicaUploadRequest
from hippius_s3.queue import dequeue_ec_request
from hippius_s3.queue import enqueue_ec_request
from hippius_s3.queue import enqueue_upload_request
from hippius_s3.redis_cache import initialize_cache_client
from hippius_s3.services.crypto_service import CryptoService
from hippius_s3.services.key_service import get_or_create_encryption_key_bytes
from hippius_s3.services.object_reader import build_chunk_index_plan_for_object


config = get_config()
setup_loki_logging(config, "redundancy")
logger = logging.getLogger(__name__)


def _start_ec_test_http_server() -> None:
    """Optional lightweight HTTP server to expose RS encode/reconstruct for tests.

    Enabled when HIPPIUS_EC_TEST_HTTP_PORT is set. Avoids extra sidecar images by
    serving a minimal API from the redundancy container.
    """
    port_val = os.environ.get("HIPPIUS_EC_TEST_HTTP_PORT")
    if not port_val:
        return
    try:
        port = int(port_val)
    except Exception:
        logger.warning("Invalid HIPPIUS_EC_TEST_HTTP_PORT: %s", port_val)
        return

    from http.server import BaseHTTPRequestHandler, HTTPServer  # lazy import
    from pyeclib.ec_iface import ECDriver  # type: ignore

    class Handler(BaseHTTPRequestHandler):
        def _json(self, code: int, obj: dict) -> None:
            body = json.dumps(obj).encode()
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self):  # noqa: N802
            if self.path == "/health":
                try:
                    ECDriver(k=2, m=1, ec_type="isa_l_rs_vand", chksum_type="none")
                    self._json(200, {"status": "ok", "backend": "isa_l_rs_vand"})
                except Exception as e:  # pragma: no cover
                    self._json(500, {"status": "error", "error": str(e)})
                return
            self._json(404, {"error": "not found"})

        def do_POST(self):  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            try:
                payload = json.loads(raw.decode() or "{}")
            except Exception:
                self._json(400, {"error": "invalid json"})
                return

            try:
                if self.path == "/reconstruct":
                    # Expect a single recovery bundle in parity[0]
                    k = int(payload.get("k", 0))
                    m = int(payload.get("m", 0))
                    missing_index = int(payload.get("missing_index", 0))
                    orig_missing_index = missing_index
                    parity_list = payload.get("parity", [])
                    if not parity_list:
                        logger.debug("EC reconstruct: missing parity entry; payload_keys=%s", list(payload.keys()))
                        self._json(400, {"error": "missing recovery bundle in 'parity'"})
                        return
                    bundle = base64.b64decode(parity_list[0])
                    # Parse RSB1 bundle
                    import struct as _struct

                    head = bundle[:4]
                    if not (len(bundle) >= 8 and head == b"RSB1"):
                        logger.debug(
                            "EC reconstruct: invalid bundle magic len=%s head_hex=%s",
                            len(bundle),
                            head.hex(),
                        )
                        self._json(
                            400, {"error": "invalid recovery bundle magic", "len": len(bundle), "head": head.hex()}
                        )
                        return
                    off = 4
                    man_len = _struct.unpack_from(">I", bundle, off)[0]
                    off += 4
                    man = json.loads(bundle[off : off + man_len].decode())
                    off += man_len
                    num_hdrs = _struct.unpack_from(">I", bundle, off)[0]
                    off += 4
                    headers: list[bytes] = []
                    for _ in range(num_hdrs):
                        seg_len = _struct.unpack_from(">I", bundle, off)[0]
                        off += 4
                        headers.append(bytes(bundle[off : off + seg_len]))
                        off += seg_len
                    num_par = _struct.unpack_from(">I", bundle, off)[0]
                    off += 4
                    parity_frags: list[bytes] = []
                    for _ in range(num_par):
                        seg_len = _struct.unpack_from(">I", bundle, off)[0]
                        off += 4
                        parity_frags.append(bytes(bundle[off : off + seg_len]))
                        off += seg_len
                    symbol = int(man.get("symbol_size", 0))
                    logger.debug(
                        "EC reconstruct parsed: man=%s num_hdrs=%s num_par=%s symbol=%s missing=%s",
                        man,
                        len(headers),
                        len(parity_frags),
                        symbol,
                        missing_index,
                    )
                    # Extra integrity logging: header indices and payload md5s
                    try:

                        def _hx(b: object, n: int = 8) -> str:
                            if not isinstance(b, (bytes, bytearray)):
                                return "<none>"
                            bb = bytes(b)
                            return bb[:n].hex() + ("..." + bb[-n:].hex() if len(bb) > 2 * n else "")

                        def _md5(b: bytes) -> str:
                            import hashlib as _hashlib

                            return _hashlib.md5(bytes(b)).hexdigest()

                        header_idxs = [int.from_bytes(h[:4], "little") if len(h) >= 4 else -1 for h in headers]
                        parity_payload_md5 = []
                        for pf in parity_frags:
                            hlen = len(headers[0]) if headers else 0
                            pf_payload = pf[hlen:]
                            parity_payload_md5.append(_md5(pf_payload))
                        logger.debug(
                            {
                                "header_idxs": header_idxs,
                                "header_len": len(headers[0]) if headers else 0,
                                "parity_count": len(parity_frags),
                                "parity_payload_md5": parity_payload_md5,
                            }
                        )
                    except Exception:
                        pass
                    driver = ECDriver(k=k, m=m, ec_type="isa_l_rs_vand", chksum_type="none")

                    def pad(b: bytes) -> bytes:
                        return b.ljust(symbol, b"\x00")[:symbol]

                    others = [base64.b64decode(o) for o in payload.get("others", [])]
                    it = iter(others)
                    data_frags: list[bytes | None] = []
                    for i in range(k):
                        hdr = headers[i] if i < len(headers) else b""
                        if i == missing_index:
                            # For PyECLib reconstruct, mark missing fragment as None (do not supply zeros)
                            data_frags.append(None)
                        else:
                            try:
                                data_frags.append(hdr + pad(next(it)))
                            except StopIteration:
                                data_frags.append(hdr + pad(b""))
                    # Validate header index alignment with fragment positions for present shards
                    try:
                        header_idxs = [int.from_bytes(h[:4], "little") if len(h) >= 4 else -1 for h in headers]
                        present_map = {
                            i: header_idxs[i] for i in range(min(k, len(header_idxs))) if data_frags[i] is not None
                        }
                        logger.debug({"present_header_index_map": present_map, "missing_index": missing_index})
                    except Exception:
                        pass
                    try:

                        def _hx(b: object, n: int = 8) -> str:
                            if not isinstance(b, (bytes, bytearray)):
                                return "<none>"
                            bb = bytes(b)
                            return bb[:n].hex() + ("..." + bb[-n:].hex() if len(bb) > 2 * n else "")

                        logger.debug(
                            {
                                "headers_len": [len(h) for h in headers],
                                "headers_head": [_hx(h) for h in headers],
                                "stitched_head": [_hx(df) for df in data_frags],
                                "missing_index": missing_index,
                            }
                        )
                    except Exception:
                        pass
                    try:
                        data_lens = [len(f) if isinstance(f, (bytes, bytearray)) else None for f in data_frags]
                        parity_lens = [len(pf) for pf in parity_frags]
                    except Exception:
                        data_lens = [None for _ in data_frags]
                        parity_lens = [len(pf) for pf in parity_frags]
                    logger.debug(
                        "EC reconstruct building frags: data_lens=%s parity_lens=%s",
                        data_lens,
                        parity_lens,
                    )
                    # Ensure first fragment provided to PyECLib is not None to satisfy size probe
                    if data_frags and data_frags[0] is None:
                        swap_idx = next((i for i, df in enumerate(data_frags) if df is not None), None)
                        if swap_idx is None:
                            self._json(400, {"error": "no available data fragment to probe size"})
                            return
                        data_frags[0], data_frags[swap_idx] = data_frags[swap_idx], data_frags[0]
                        logger.debug("EC reconstruct: swapped probe into index 0; probe_from=%s", swap_idx)
                    # Prefer decode path: provide any k full fragments (header+payload), then slice symbol
                    selected: list[bytes] = []
                    # Use all present data frags first
                    for i in range(k):
                        df = data_frags[i]
                        if isinstance(df, (bytes, bytearray)):
                            selected.append(bytes(df))
                    # Fill up with parity until we have k
                    pi = 0
                    while len(selected) < k and pi < len(parity_frags):
                        selected.append(bytes(parity_frags[pi]))
                        pi += 1
                    # Sanity: must have exactly k
                    if len(selected) != k:
                        self._json(
                            400, {"error": "insufficient fragments for decode", "have": len(selected), "need": int(k)}
                        )
                        return
                    try:
                        flen = len(selected[0])
                        if not all(len(f) == flen for f in selected):
                            self._json(400, {"error": "fragment size mismatch for decode"})
                            return
                    except Exception:
                        pass
                    decoded = driver.decode(selected)
                    # Slice out the missing symbol payload
                    start = int(orig_missing_index) * int(symbol)
                    end = start + int(symbol)
                    rec_payload = bytes(decoded[start:end])
                    with contextlib.suppress(Exception):
                        logger.debug(
                            {
                                "rec_payload_len": len(rec_payload),
                                "rec_payload_head": rec_payload[:16].hex(),
                                "decode_joined_len": len(decoded) if isinstance(decoded, (bytes, bytearray)) else None,
                                "slice": [start, end],
                            }
                        )
                    self._json(
                        200,
                        {"backend": "isa_l_rs_vand", "reconstructed": base64.b64encode(rec_payload).decode("ascii")},
                    )
                    return
                if self.path == "/encode":
                    k = int(payload.get("k", 0))
                    m = int(payload.get("m", 0))
                    symbol_size = int(payload.get("symbol_size", 0))
                    blocks = [base64.b64decode(b) for b in payload.get("blocks", [])]
                    driver = ECDriver(k=k, m=m, ec_type="isa_l_rs_vand", chksum_type="none")
                    padded = [b.ljust(symbol_size, b"\x00")[:symbol_size] for b in blocks[:k]]
                    joined = b"".join(padded)
                    frags = driver.encode(joined)
                    parity = frags[k : k + m]
                    self._json(
                        200,
                        {"backend": "isa_l_rs_vand", "parity": [base64.b64encode(p).decode("ascii") for p in parity]},
                    )
                    return
                else:
                    self._json(404, {"error": "not found"})
            except Exception as e:
                try:
                    logger.error(
                        "EC test HTTP error on %s: %s | payload_keys=%s",
                        self.path,
                        str(e),
                        list(payload.keys()) if isinstance(payload, dict) else "n/a",
                        exc_info=True,
                    )
                finally:
                    self._json(500, {"error": str(e)})

    def _serve() -> None:
        srv = HTTPServer(("0.0.0.0", port), Handler)
        logger.info("EC test HTTP server listening on :%s", port)
        srv.serve_forever()

    t = threading.Thread(target=_serve, name="ec-test-http", daemon=True)
    t.start()


async def _get_part_row(conn: asyncpg.Connection, object_id: str, object_version: int, part_number: int):
    return await conn.fetchrow(
        """
        SELECT p.part_id, COALESCE(p.size_bytes, 0) AS size_bytes
          FROM parts p
         WHERE p.object_id = $1 AND p.object_version = $2 AND p.part_number = $3
         LIMIT 1
        """,
        object_id,
        int(object_version),
        int(part_number),
    )


async def _upsert_part_ec_replication(
    conn: asyncpg.Connection,
    *,
    part_id: str,
    policy_version: int,
    replication_factor: int,
    shard_size_bytes: int,
) -> None:
    await conn.execute(
        """
        INSERT INTO part_ec (part_id, policy_version, scheme, k, m, shard_size_bytes, stripes, state)
        VALUES ($1, $2, 'rep-v1', 1, $3, $4, 1, 'pending_upload')
        ON CONFLICT (part_id, policy_version)
        DO UPDATE SET scheme='rep-v1', k=1, m=EXCLUDED.m, shard_size_bytes=EXCLUDED.shard_size_bytes, stripes=1, state='pending_upload', updated_at=now()
        """,
        part_id,
        int(policy_version),
        int(max(0, replication_factor - 1)),
        int(shard_size_bytes),
    )


async def _ensure_ciphertext_in_cache(
    *,
    db: asyncpg.Connection,
    obj_cache: RedisObjectPartsCache,
    object_id: str,
    object_version: int,
    part_number: int,
    address: str,
    bucket_name: str,
) -> list[tuple[int, str | None, int | None]]:
    # Build an index-only plan using the reader planner (independent of DB part_chunks CIDs)
    items = await build_chunk_index_plan_for_object(db, object_id, int(object_version))
    return [(int(it.chunk_index), None, None) for it in items if int(getattr(it, "part_number", 1)) == int(part_number)]


async def process_redundancy_request(
    req: RedundancyRequest,
    db: asyncpg.Pool,
    redis_client: async_redis.Redis,
) -> bool:
    obj_cache = RedisObjectPartsCache(redis_client)
    fs_store = FileSystemPartsStore(getattr(config, "object_cache_dir", "/var/lib/hippius/object_cache"))

    async with db.acquire() as conn:
        # Quick probe for parts placeholder; if missing, requeue and move on (avoid blocking worker)
        part_row = None
        for _ in range(3):  # fast retries to bridge tiny commit races
            part_row = await _get_part_row(conn, req.object_id, int(req.object_version), int(req.part_number))
            if part_row:
                break
            await asyncio.sleep(0.01)
        if not part_row:
            logger.warning(
                f"redundancy: part row missing object_id={req.object_id} v={req.object_version} part={req.part_number}"
            )
            # Re-enqueue a few times to bridge commit visibility races
            with contextlib.suppress(Exception):
                if int(getattr(req, "attempts", 0)) < 5:
                    req.attempts = int(getattr(req, "attempts", 0)) + 1
                    await enqueue_ec_request(req)
                    return True
            return False
        part_id = str(part_row[0])

        chunk_plan = await _ensure_ciphertext_in_cache(
            db=conn,
            obj_cache=obj_cache,
            object_id=req.object_id,
            object_version=int(req.object_version),
            part_number=int(req.part_number),
            address=req.address,
            bucket_name=req.bucket_name,
        )
        if not chunk_plan:
            logger.warning(
                f"redundancy: no chunk plan rows object_id={req.object_id} part={req.part_number}; requeueing"
            )
            with contextlib.suppress(Exception):
                if int(getattr(req, "attempts", 0)) < 5:
                    req.attempts = int(getattr(req, "attempts", 0)) + 1
                    await enqueue_ec_request(req)
                    return True
            return False

        meta = await obj_cache.get_meta(req.object_id, int(req.object_version), int(req.part_number))
        cipher_size = int((meta or {}).get("size_bytes", 0))
        if cipher_size <= 0:
            try:
                cipher_size = int(getattr(req, "part_size_bytes", 0) or 0)
            except Exception:
                cipher_size = 0
        threshold = int(config.ec_k) * int(config.ec_min_chunk_size_bytes)
        replication = cipher_size < threshold

        if replication:
            R = max(1, int(getattr(config, "ec_replication_factor", 2)))
            if R <= 1:
                logger.info("redundancy: replication factor <=1; nothing to do")
                return True

            # Ensure part_ec exists to satisfy FK
            await _upsert_part_ec_replication(
                conn,
                part_id=part_id,
                policy_version=int(req.policy_version),
                replication_factor=R,
                shard_size_bytes=int(cipher_size),
            )

            key_bytes = await get_or_create_encryption_key_bytes(
                main_account_id=req.address,
                bucket_name=req.bucket_name,
            )

            rep_staged: list[ReplicaStagedItem] = []
            # Stage replica files under FS and enqueue uploader
            for ci, _cid, _clen in sorted(chunk_plan, key=lambda x: int(x[0])):
                ct = await obj_cache.get_chunk(req.object_id, int(req.object_version), int(req.part_number), int(ci))
                if not isinstance(ct, (bytes, bytearray)) or len(ct) == 0:
                    # Fallback to FS if cache miss
                    try:
                        chunk_path = (
                            Path(fs_store.part_path(req.object_id, int(req.object_version), int(req.part_number)))
                            / f"chunk_{int(ci)}.bin"
                        )
                        if chunk_path.exists():
                            ct = chunk_path.read_bytes()
                    except Exception:
                        ct = b""
                if not isinstance(ct, (bytes, bytearray)) or len(ct) == 0:
                    continue
                pt = CryptoService.decrypt_chunk(
                    bytes(ct),
                    seed_phrase="",
                    object_id=req.object_id,
                    part_number=int(req.part_number),
                    chunk_index=int(ci),
                    key=key_bytes,
                )
                for r in range(1, R):
                    replica_chunks = CryptoService.encrypt_part_to_chunks(
                        pt,
                        object_id=req.object_id,
                        part_number=int(req.part_number),
                        seed_phrase="",
                        chunk_size=len(pt),
                        key=key_bytes,
                    )
                    replica_ct = replica_chunks[0] if replica_chunks else b""
                    if not replica_ct:
                        continue
                    # Write staged file
                    part_dir = Path(fs_store.part_path(req.object_id, int(req.object_version), int(req.part_number)))
                    rep_dir = part_dir / f"rep_{int(r)}"
                    rep_dir.mkdir(parents=True, exist_ok=True)
                    file_path = rep_dir / f"chunk_{int(ci)}.bin"

                    def _write(fp=file_path, rc=replica_ct) -> None:
                        with fp.open("wb") as f:
                            f.write(rc)
                            f.flush()

                    await asyncio.to_thread(_write)
                    rep_staged.append(
                        ReplicaStagedItem(
                            file_path=str(file_path),
                            chunk_index=int(ci),
                            replica_index=int(r - 1),
                        )
                    )

            logger.info(
                f"redundancy: staged replicas count={len(rep_staged)} object_id={req.object_id} part={req.part_number}"
            )
            await enqueue_upload_request(
                ReplicaUploadRequest(
                    address=req.address,
                    bucket_name=req.bucket_name,
                    object_key="",
                    object_id=req.object_id,
                    object_version=int(req.object_version),
                    part_number=int(req.part_number),
                    kind="replica",
                    policy_version=int(req.policy_version),
                    staged=rep_staged,
                )
            )
            return True

        k = max(1, int(config.ec_k))
        m = max(0, int(config.ec_m))

        if m <= 0:
            logger.info("redundancy: EC m<=0; skipping parity generation")
            return True

        # Determine shard size: prefer the chunk_size stored in meta (writer's per-part chunk size)
        shard_size = int(config.ec_min_chunk_size_bytes)
        if isinstance(meta, dict):
            # meta from cache stores key as 'chunk_size'
            with contextlib.suppress(Exception):
                shard_size = max(int(meta.get("chunk_size", shard_size)), shard_size)
            # Backward compatibility if key differs
            with contextlib.suppress(Exception):
                shard_size = max(int(meta.get("chunk_size_bytes", shard_size)), shard_size)

        stripes = int(math.ceil(len(chunk_plan) / float(k))) if chunk_plan else 0
        if stripes <= 0:
            logger.info("redundancy: EC found no stripes; skipping")
            return True

        # Upsert part_ec to 'pending_upload'
        await conn.execute(
            """
            INSERT INTO part_ec (part_id, policy_version, scheme, k, m, shard_size_bytes, stripes, state)
            VALUES ($1, $2, 'rs-v1', $3, $4, $5, $6, 'pending_upload')
            ON CONFLICT (part_id, policy_version)
            DO UPDATE SET scheme='rs-v1', k=EXCLUDED.k, m=EXCLUDED.m, shard_size_bytes=EXCLUDED.shard_size_bytes,
                          stripes=EXCLUDED.stripes, state='pending_upload', updated_at=now()
            """,
            part_id,
            int(req.policy_version),
            int(k),
            int(m),
            int(shard_size),
            int(stripes),
        )

        part_dir = Path(fs_store.part_path(req.object_id, int(req.object_version), int(req.part_number)))
        pv_dir = part_dir / f"pv_{int(req.policy_version)}"
        pv_dir.mkdir(parents=True, exist_ok=True)

        par_staged: list[ParityStagedItem] = []
        # Process stripes
        logger.info(
            f"redundancy: planning parity stripes={stripes} k={k} object_id={req.object_id} part={req.part_number}"
        )
        for s in range(stripes):
            start = s * k
            end = min(start + k, len(chunk_plan))
            stripe_items = chunk_plan[start:end]
            if not stripe_items:
                continue
            # Load ciphertext for this stripe
            blocks: list[bytes] = []
            max_len = 0
            for ci, _cid, _clen in stripe_items:
                ct = await obj_cache.get_chunk(req.object_id, int(req.object_version), int(req.part_number), int(ci))
                if not isinstance(ct, (bytes, bytearray)) or len(ct) == 0:
                    try:
                        chunk_path = (
                            Path(fs_store.part_path(req.object_id, int(req.object_version), int(req.part_number)))
                            / f"chunk_{int(ci)}.bin"
                        )
                        if chunk_path.exists():
                            ct = chunk_path.read_bytes()
                    except Exception:
                        ct = b""
                b = bytes(ct or b"")
                blocks.append(b)
                max_len = max(max_len, len(b))
            # Compute parity and capture data headers (ISA-L) for recovery bundle
            if not blocks:
                continue
            max_len = max(max_len, shard_size)
            effective_k = min(k, len(blocks))
            try:
                from pyeclib.ec_iface import ECDriver  # type: ignore

                driver = ECDriver(k=int(effective_k), m=1, ec_type="isa_l_rs_vand", chksum_type="none")
                # Pad data blocks to symbol size and join
                padded = [bytes(b).ljust(max_len, b"\x00")[:max_len] for b in blocks[:effective_k]]
                joined = b"".join(padded)
                fragments = driver.encode(joined)
                if not isinstance(fragments, list) or len(fragments) < effective_k + 1:
                    logger.error(
                        "redundancy: encode returned unexpected fragments len=%s",
                        len(fragments) if isinstance(fragments, list) else None,
                    )
                    continue
                data_frags = fragments[:effective_k]
                parity_frags = fragments[effective_k : effective_k + 1]
                parity_block = bytes(parity_frags[0])
                # Derive header sizes by subtracting payload size
                header_and_sizes = []
                data_headers: list[bytes] = []
                for df in data_frags:
                    df_bytes = bytes(df)
                    header_len = max(0, len(df_bytes) - max_len)
                    data_headers.append(df_bytes[:header_len])
                    header_and_sizes.append(header_len)
                # Build single-file recovery bundle: magic + json + headers + parity
                import json as _json
                import struct as _struct

                manifest = {
                    "version": 1,
                    "backend": "isa_l_rs_vand",
                    "k": int(effective_k),
                    "m": 1,
                    "symbol_size": int(max_len),
                    "stripe_index": int(s),
                    "header_sizes": header_and_sizes,
                }
                man_bytes = _json.dumps(manifest, separators=(",", ":")).encode()
                buf = bytearray()
                buf.extend(b"RSB1")
                buf.extend(_struct.pack(">I", len(man_bytes)))
                buf.extend(man_bytes)
                # data headers
                buf.extend(_struct.pack(">I", len(data_headers)))
                for hdr in data_headers:
                    buf.extend(_struct.pack(">I", len(hdr)))
                    buf.extend(hdr)
                # parity frags
                buf.extend(_struct.pack(">I", len(parity_frags)))
                for pf in parity_frags:
                    pf_b = bytes(pf)
                    buf.extend(_struct.pack(">I", len(pf_b)))
                    buf.extend(pf_b)

            finally:
                try:

                    def _hex_sample(b: bytes, n: int = 8) -> str:
                        if not isinstance(b, (bytes, bytearray)):
                            return "<none>"
                        if len(b) <= 2 * n:
                            return bytes(b).hex()
                        return f"{bytes(b)[:n].hex()}...{bytes(b)[-n:].hex()}"

                    import hashlib as _hl

                    logger.debug(
                        {
                            "stripe_index": int(s),
                            "config_k": int(k),
                            "effective_k": int(effective_k),
                            "num_blocks": len(blocks),
                            "block_lens": [len(b) for b in blocks],
                            "symbol_size": int(max_len),
                            "parity_len": len(parity_block) if "parity_block" in locals() else None,
                            "parity_md5": _hl.md5(parity_block).hexdigest() if "parity_block" in locals() else None,
                            "parity_head": _hex_sample(parity_block) if "parity_block" in locals() else None,
                        }
                    )
                except Exception:
                    pass

            # Write staged parity file
            file_path = pv_dir / f"stripe_{s}.recovery.bin"

            def _write_parity(fp=file_path, pb=bytes(buf)) -> None:  # type: ignore[name-defined]
                with fp.open("wb") as f:
                    f.write(pb)
                    f.flush()

            await asyncio.to_thread(_write_parity)

            par_staged.append(ParityStagedItem(file_path=str(file_path), stripe_index=int(s), parity_index=0))

        logger.info(
            f"redundancy: staged parity files count={len(par_staged)} object_id={req.object_id} part={req.part_number}"
        )
        if par_staged:
            await enqueue_upload_request(
                ParityUploadRequest(
                    address=req.address,
                    bucket_name=req.bucket_name,
                    object_key="",
                    object_id=req.object_id,
                    object_version=int(req.object_version),
                    part_number=int(req.part_number),
                    kind="parity",
                    policy_version=int(req.policy_version),
                    staged=par_staged,
                )
            )
        return True


async def run_redundancy_loop() -> None:
    db_pool = await asyncpg.create_pool(config.database_url, min_size=2, max_size=10)
    redis_client = async_redis.from_url(config.redis_url)
    redis_queues_client = async_redis.from_url(config.redis_queues_url)

    from hippius_s3.queue import initialize_queue_client

    initialize_queue_client(redis_queues_client)
    initialize_cache_client(redis_client)
    initialize_metrics_collector(redis_client)

    logger.info("Starting redundancy service...")
    # Start optional test HTTP server for RS encode/reconstruct
    _start_ec_test_http_server()
    with contextlib.suppress(Exception):
        logger.info(
            f"EC config: enabled={config.ec_enabled} scheme={config.ec_scheme} "
            f"k={config.ec_k} m={config.ec_m} threshold_bytes={getattr(config, 'ec_threshold_bytes', 0)} "
            f"queue={config.ec_queue_name}"
        )

    try:
        while True:
            try:
                req = await dequeue_ec_request()
            except (BusyLoadingError, RedisConnectionError, RedisTimeoutError) as e:
                logger.warning(f"Redis error while dequeuing redundancy request: {e}. Reconnecting in 2s...")
                with contextlib.suppress(Exception):
                    await redis_queues_client.aclose()
                await asyncio.sleep(2)
                redis_queues_client = async_redis.from_url(config.redis_queues_url)
                initialize_queue_client(redis_queues_client)
                continue
            except Exception as e:
                logger.error(f"Failed to dequeue/parse redundancy request, skipping: {e}", exc_info=True)
                continue

            if req:
                try:
                    ok = await process_redundancy_request(req, db_pool, redis_client)
                    if ok:
                        logger.info(
                            f"Successfully processed redundancy request object_id={req.object_id} part={req.part_number}"
                        )
                    else:
                        logger.error(
                            f"Failed to process redundancy request object_id={req.object_id} part={req.part_number}"
                        )
                except (RedisConnectionError, RedisTimeoutError, BusyLoadingError) as e:
                    logger.warning(
                        f"Redis connection issue during redundancy processing: {e}. Reconnecting in 2s and continuing..."
                    )
                    with contextlib.suppress(Exception):
                        await redis_client.aclose()
                    await asyncio.sleep(2)
                    redis_client = async_redis.from_url(config.redis_url)
                    initialize_cache_client(redis_client)
                    continue
            else:
                await asyncio.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("Redundancy service stopping...")
    except Exception as e:
        logger.error(f"Error in redundancy loop: {e}")
        raise
    finally:
        await redis_client.aclose()
        await redis_queues_client.aclose()
        await db_pool.close()


if __name__ == "__main__":
    asyncio.run(run_redundancy_loop())
