#!/usr/bin/env python3
# Hydrate the OVH backup DLQ from Arion, then replay it — a node6-free recovery.
#
# After the s3-2.1 cutover a closed cohort (~8k objects written 08:14–08:18Z by the old
# api to node6's now-retired local cache) sits in ovh_upload_requests:dlq: their parts are
# ABSENT from the CephFS pool the backup reads, so the backup can never find them. But every
# one has an Arion copy. This tool recovers them WITHOUT node6, in four phases:
#
#   1 hydrate : enqueue an Arion DownloadChainRequest per DLQ object → arion-downloader pulls
#               its parts Arion→CephFS pool (writing meta.json first, the readiness signal).
#   2 wait    : poll the pool until each object's parts are present (meta.json on disk).
#   3 requeue : re-enqueue the now-servable DLQ entries to ovh_upload_requests (force, since
#               the legacy entries are error_type=permanent) so the backup backs them to OVH.
#   4 drain   : poll until the recovered objects show an arion→OVH chunk_backend row.
#
# DRY-RUN by default — prints exactly what it WOULD do and mutates nothing. Pass --apply to
# execute. Use --limit N to prove the pipeline on a small batch first (do 1 before 8k).
#
# Order is load-bearing: hydrate + wait BEFORE requeue, else the requeue re-fails on a part
# that isn't in the pool yet and re-poisons the worker pool.
#
# Run in a pod that has the hippius_s3 package AND mounts object-cache-pvc (an arion-downloader
# or api-local pod), or as a one-off Job with the api image + the pool mount. Reads go to a
# read-only replica (DATABASE_REPLICA_URL); the only writes are Redis LPUSHes.
from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
import uuid
from pathlib import Path
from typing import Any

import asyncpg
import redis.asyncio as aioredis

from hippius_s3.queue import DownloadChainRequest
from hippius_s3.queue import PartChunkSpec
from hippius_s3.queue import PartToDownload
from hippius_s3.queue import initialize_queue_client


REDIS_QUEUES_URL = os.environ.get("REDIS_QUEUES_URL", "redis://redis-queues:6379")
DB_URL = os.environ.get("DATABASE_REPLICA_URL") or os.environ.get("DATABASE_URL", "")
POOL_DIR = os.environ.get("POOL_CACHE_DIR", "/var/lib/hippius/object_cache")
SUBSTRATE_URL = os.environ.get("SUBSTRATE_URL") or os.environ.get("HIPPIUS_SUBSTRATE_URL", "")
DCR_EXPIRE_SECONDS = float(os.environ.get("HYDRATE_DCR_EXPIRE_SECONDS", str(6 * 3600)))

OVH_DLQ = "ovh_upload_requests:dlq"
OVH_QUEUE = "ovh_upload_requests"
ARION_DL_QUEUE = "arion_download_requests"

# NB: address comes from the DLQ payload (not this query) — object_versions.address is the new
# cutover migration column and a lagging read replica may not have the DDL yet.
_META_QUERY = """
SELECT ov.object_id::text AS object_id, ov.object_version, ov.size_bytes, ov.multipart,
       ov.storage_version,
       p.part_number, pc.chunk_index, pc.cid, pc.cipher_size_bytes,
       bool_or(cb.backend = 'arion' AND NOT cb.deleted) AS on_arion
FROM object_versions ov
JOIN parts p ON p.object_id = ov.object_id AND p.object_version = ov.object_version
JOIN part_chunks pc ON pc.part_id = p.part_id
LEFT JOIN chunk_backend cb ON cb.chunk_id = pc.id
WHERE ov.object_id = ANY($1::uuid[])
GROUP BY ov.object_id, ov.object_version, ov.size_bytes, ov.multipart, ov.storage_version,
         p.part_number, pc.chunk_index, pc.cid, pc.cipher_size_bytes
"""


def _log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


async def scan_dlq(redis: aioredis.Redis, limit: int | None) -> dict[tuple[str, int], dict[str, Any]]:
    """Read (peek, don't remove) the OVH DLQ and return unique objects keyed by (object_id, version).

    The DLQ entry wraps the original UploadChainRequest under ``payload`` — that carries
    object_id/version/bucket/key/address/upload_id, everything we need except the exact chunk
    indices and Arion presence (filled from the replica in build_targets)."""
    raw = await redis.lrange(OVH_DLQ, 0, -1)  # ty: ignore[invalid-await]
    out: dict[tuple[str, int], dict[str, Any]] = {}
    for entry_json in raw:
        try:
            entry = json.loads(entry_json)
        except json.JSONDecodeError:
            continue
        p = entry.get("payload") or {}
        oid = p.get("object_id") or entry.get("object_id")
        ver = int(p.get("object_version", 1))
        if not oid:
            continue
        key = (str(oid), ver)
        if key not in out:
            out[key] = {
                "object_id": str(oid),
                "object_version": ver,
                "object_key": p.get("object_key", ""),
                "bucket_name": p.get("bucket_name", ""),
                "address": p.get("address", ""),
                "upload_id": p.get("upload_id"),
            }
        if limit and len(out) >= limit:
            break
    return out


async def build_targets(
    pool: asyncpg.Pool,
    dlq: dict[tuple[str, int], dict[str, Any]],
    require_arion: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Enrich each DLQ object with its parts/chunk-indices + Arion presence from the replica.

    Returns (hydratable, skipped). An object is skipped when the replica has no rows for it
    (no such version) or, with require_arion, when Arion doesn't hold every chunk — hydration
    can't invent bytes that aren't on the durable backend."""
    ids = list({oid for (oid, _v) in dlq})
    rows: list[asyncpg.Record] = []
    for i in range(0, len(ids), 500):
        rows.extend(await pool.fetch(_META_QUERY, ids[i : i + 500]))

    # collapse rows → per (object_id, version): meta + {part_number: [(index, cid, size, on_arion)]}
    agg: dict[tuple[str, int], dict[str, Any]] = {}
    for r in rows:
        key = (r["object_id"], int(r["object_version"]))
        d = agg.setdefault(
            key,
            {
                "size": int(r["size_bytes"] or 0),
                "multipart": bool(r["multipart"]),
                "storage_version": int(r["storage_version"] or 0),
                "parts": {},
            },
        )
        d["parts"].setdefault(int(r["part_number"]), []).append(
            (int(r["chunk_index"]), r["cid"], r["cipher_size_bytes"], bool(r["on_arion"]))
        )

    hydratable: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []
    for key, base in dlq.items():
        meta = agg.get(key)
        if not meta or not meta["parts"]:
            skipped.append({**base, "reason": "no rows on replica (version absent)"})
            continue
        all_on_arion = all(on_arion for chunks in meta["parts"].values() for (_i, _c, _s, on_arion) in chunks)
        if require_arion and not all_on_arion:
            skipped.append({**base, "reason": "arion copy incomplete"})
            continue
        if not base.get("address"):
            skipped.append({**base, "reason": "no address in DLQ payload"})
            continue
        hydratable.append({**base, **meta})
    return hydratable, skipped


def object_present(obj: dict[str, Any]) -> bool:
    """True when every part of the object has a meta.json in the pool (backup-readable)."""
    root = Path(POOL_DIR) / obj["object_id"] / f"v{int(obj['object_version'])}"
    return all((root / f"part_{int(pn)}" / "meta.json").exists() for pn in obj["parts"])


def build_dcr(obj: dict[str, Any]) -> DownloadChainRequest:
    dl_parts = [
        PartToDownload(
            part_number=int(pn),
            chunks=[
                PartChunkSpec(index=int(i), cid=cid, cipher_size_bytes=size) for (i, cid, size, _a) in sorted(chunks)
            ],
        )
        for pn, chunks in sorted(obj["parts"].items())
    ]
    return DownloadChainRequest(
        request_id=f"hydrate::{obj['object_id']}::{uuid.uuid4().hex[:8]}",
        attempts=0,
        first_enqueued_at=time.time(),
        ray_id=f"hydrate-{obj['object_id'][:8]}",
        object_id=obj["object_id"],
        object_version=int(obj["object_version"]),
        object_storage_version=int(obj.get("storage_version") or 0),
        object_key=obj.get("object_key", ""),
        bucket_name=obj.get("bucket_name", ""),
        address=obj["address"],
        subaccount=obj["address"],
        substrate_url=SUBSTRATE_URL,
        size=int(obj.get("size", 0)),
        multipart=bool(obj.get("multipart")),
        chunks=dl_parts,
        download_backends=["arion"],
        expire_at=time.time() + DCR_EXPIRE_SECONDS,
    )


async def phase_hydrate(redis: aioredis.Redis, hydratable: list[dict[str, Any]], apply: bool) -> int:
    already = [o for o in hydratable if object_present(o)]
    todo = [o for o in hydratable if o not in already]
    _log(f"phase 1 hydrate: {len(hydratable)} objects — {len(already)} already in pool, {len(todo)} to enqueue")
    if todo:
        sample = build_dcr(todo[0])
        _log(f"  sample DCR → {ARION_DL_QUEUE}: {sample.model_dump_json()[:320]}...")
    if not apply:
        _log("  DRY-RUN: not enqueuing. Re-run with --apply to hydrate.")
        return 0
    pipe = redis.pipeline(transaction=False)
    for o in todo:
        pipe.lpush(ARION_DL_QUEUE, build_dcr(o).model_dump_json())
    await pipe.execute()
    _log(f"  enqueued {len(todo)} Arion download requests")
    return len(todo)


async def phase_wait_pool(hydratable: list[dict[str, Any]], timeout: float, interval: float) -> list[dict[str, Any]]:  # noqa: ASYNC109
    _log(f"phase 2 wait: polling the pool for {len(hydratable)} objects (timeout {int(timeout)}s)")
    deadline = time.time() + timeout
    while True:
        present = [o for o in hydratable if object_present(o)]
        _log(f"  {len(present)}/{len(hydratable)} present in pool")
        if len(present) == len(hydratable) or time.time() >= deadline:
            if len(present) != len(hydratable):
                _log(
                    f"  TIMEOUT: {len(hydratable) - len(present)} still absent — proceeding with the {len(present)} ready"
                )
            return present
        await asyncio.sleep(interval)


async def phase_requeue(redis: aioredis.Redis, present: list[dict[str, Any]], apply: bool) -> int:
    from hippius_s3.dlq.upload_dlq import UploadDLQManager

    _log(f"phase 3 requeue: {len(present)} hydrated objects back onto {OVH_QUEUE}")
    if not apply:
        _log("  DRY-RUN: not requeuing.")
        return 0
    mgr = UploadDLQManager(redis, backend_name="ovh")
    n = 0
    for o in present:
        if await mgr.requeue(o["object_id"], force=True):
            n += 1
    _log(f"  requeued {n} objects (force)")
    return n


async def phase_drain(
    redis: aioredis.Redis, pool: asyncpg.Pool, present: list[dict[str, Any]], timeout_s: float, interval: float
) -> None:
    _log(f"phase 4 drain: waiting for {len(present)} objects to reach OVH (timeout {int(timeout_s)}s)")
    ids = [o["object_id"] for o in present]
    deadline = time.time() + timeout_s
    q = (
        "SELECT count(DISTINCT p.object_id) FROM parts p JOIN part_chunks pc ON pc.part_id=p.part_id "
        "JOIN chunk_backend cb ON cb.chunk_id=pc.id "
        "WHERE p.object_id = ANY($1::uuid[]) AND cb.backend='ovh' AND NOT cb.deleted"
    )
    while True:
        done = await pool.fetchval(q, ids) if ids else 0
        dlq_n = await redis.llen(OVH_DLQ)  # ty: ignore[invalid-await]
        _log(f"  {done}/{len(ids)} on OVH · ovh dlq={dlq_n}")
        if done >= len(ids) or time.time() >= deadline:
            return
        await asyncio.sleep(interval)


async def main() -> None:
    ap = argparse.ArgumentParser(description="Hydrate the OVH backup DLQ from Arion, then replay it.")
    ap.add_argument("--apply", action="store_true", help="actually mutate (default: dry-run)")
    ap.add_argument(
        "--limit", type=int, default=None, help="only process the first N unique objects (test small first)"
    )
    ap.add_argument(
        "--phases",
        default="hydrate,wait,requeue,drain",
        help="comma list of phases to run (default all). e.g. --phases hydrate,wait",
    )
    ap.add_argument("--no-require-arion", action="store_true", help="don't skip objects Arion lacks (NOT recommended)")
    ap.add_argument("--wait-timeout", type=float, default=1800.0)
    ap.add_argument("--drain-timeout", type=float, default=1800.0)
    ap.add_argument("--poll-interval", type=float, default=10.0)
    args = ap.parse_args()
    phases = {p.strip() for p in args.phases.split(",") if p.strip()}

    if not DB_URL:
        _log("ERROR: set DATABASE_REPLICA_URL (or DATABASE_URL) to a read-only replica")
        sys.exit(1)

    _log(f"mode={'APPLY' if args.apply else 'DRY-RUN'} pool_dir={POOL_DIR} replica={DB_URL.split('@')[-1]}")
    redis = aioredis.from_url(REDIS_QUEUES_URL)
    initialize_queue_client(redis)  # UploadDLQManager.requeue enqueues via the shared queue client
    pool = await asyncpg.create_pool(DB_URL, min_size=1, max_size=4)
    try:
        dlq = await scan_dlq(redis, args.limit)
        _log(
            f"scanned {OVH_DLQ}: {len(dlq)} unique (object_id, version) targets"
            + (f" [limit {args.limit}]" if args.limit else "")
        )
        if not dlq:
            _log("DLQ empty — nothing to do.")
            return

        hydratable, skipped = await build_targets(pool, dlq, require_arion=not args.no_require_arion)
        _log(f"hydratable={len(hydratable)}  skipped={len(skipped)}")
        for s in skipped[:10]:
            _log(f"  skip {s['object_id']} v{s['object_version']}: {s['reason']}")
        if len(skipped) > 10:
            _log(f"  ... +{len(skipped) - 10} more skipped")
        # LAG GUARD: a large fraction skipped as "no rows ... version absent" almost never means the
        # objects are genuinely gone — it means we are reading a STALE READ REPLICA that hasn't
        # replayed these rows yet (observed live: postgres-nvme-ro returned parts=0 for rows that
        # exist on the primary). Silently doing nothing would be a false "cleared". Fail loud.
        absent = [s for s in skipped if "no rows" in s["reason"]]
        if dlq and len(absent) >= max(20, int(0.30 * len(dlq))):
            _log(
                f"ABORT: {len(absent)}/{len(dlq)} objects missing from the DB at {DB_URL.split('@')[-1]}. "
                "This is almost certainly a LAGGING READ REPLICA, not genuinely-absent data. "
                "Re-run against the PRIMARY: unset DATABASE_REPLICA_URL (falls back to DATABASE_URL) "
                "or point it at postgres-nvme-rw."
            )
            sys.exit(2)
        if not hydratable:
            _log("nothing hydratable — stopping.")
            return

        if "hydrate" in phases:
            await phase_hydrate(redis, hydratable, args.apply)
        present = hydratable
        if "wait" in phases and args.apply:
            present = await phase_wait_pool(hydratable, args.wait_timeout, args.poll_interval)
        elif "wait" in phases:
            _log("phase 2 wait: skipped in DRY-RUN")
            present = [o for o in hydratable if object_present(o)]
        if "requeue" in phases:
            await phase_requeue(redis, present, args.apply)
        if "drain" in phases and args.apply:
            await phase_drain(redis, pool, present, args.drain_timeout, args.poll_interval)

        _log("done.")
    finally:
        await pool.close()
        await redis.aclose()


if __name__ == "__main__":
    asyncio.run(main())
