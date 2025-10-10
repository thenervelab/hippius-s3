#!/usr/bin/env python3
"""
Deployment-aware smoke test for Hippius S3 service.

Phases:
  - pre: create test objects (single-part + multipart), verify reads, write state file
  - post: read state file, verify all pre objects are retrievable (auth and anonymous for public),
          then create new objects and verify reads

Credentials/endpoint:
  - Uses AWS_* env vars for keys/region
  - Uses S3_ENDPOINT_URL env var for endpoint (or --endpoint-url)

Example:
  # Pre-deploy
  python3 hippius-s3/scripts/deploy_smoke.py \
    --phase pre \
    --state-file deploy_state.json \
    --bucket-private my-private \
    --bucket-public my-public \
    --total-size-mb 64 \
    --part-sizes 8

  # ... deploy ...

  # Post-deploy
  python3 hippius-s3/scripts/deploy_smoke.py \
    --phase post \
    --state-file deploy_state.json \
    --bucket-private my-private \
    --bucket-public my-public
"""

from __future__ import annotations

import argparse
import contextlib
import csv
import hashlib
import io
import json
import os
import random
import string
import sys
import time
from dataclasses import asdict
from dataclasses import dataclass
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

import boto3  # type: ignore[import-not-found]
import requests  # type: ignore[import-not-found]
from botocore.config import Config as BotoConfig  # type: ignore[import-not-found]


def _rand_suffix(n: int = 6) -> str:
    return "".join(random.choices(string.ascii_lowercase + string.digits, k=n))


def _generate_bytes(size_mb: int, seed_byte: int) -> bytes:
    size = size_mb * 1024 * 1024
    block = bytes([seed_byte % 256]) * (1024 * 1024)
    blocks, rem = divmod(size, len(block))
    return block * blocks + block[:rem]


def _multipart_upload(
    s3,
    bucket: str,
    key: str,
    total_size_mb: int,
    part_size_mb: int,
    *,
    progress: bool = True,
) -> Tuple[str, float]:
    if total_size_mb % part_size_mb != 0:
        raise ValueError("total_size_mb must be divisible by part_size_mb")
    num_parts = total_size_mb // part_size_mb
    create = s3.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = create["UploadId"]

    def _upload_one(part_number: int) -> str:
        data = _generate_bytes(part_size_mb, seed_byte=part_number)
        resp = s3.upload_part(
            Bucket=bucket,
            Key=key,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=io.BytesIO(data),
        )
        return resp["ETag"]

    t0 = time.monotonic()
    etags: List[Tuple[int, str]] = []
    try:
        for pn in range(1, num_parts + 1):
            if progress:
                print(f"UPLOAD multipart {bucket}/{key} part {pn}/{num_parts} ({part_size_mb}MB)", flush=True)
            etags.append((pn, _upload_one(pn)))
        parts = [{"PartNumber": pn, "ETag": etag} for pn, etag in etags]
        comp = s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        return comp.get("ETag", ""), (time.monotonic() - t0)
    except Exception:
        with contextlib.suppress(Exception):
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


def _get_s3_client(endpoint_url: Optional[str]):
    session = boto3.session.Session()
    cfg = BotoConfig(retries={"max_attempts": 5, "mode": "standard"})
    return session.client(
        "s3",
        endpoint_url=endpoint_url or os.environ.get("S3_ENDPOINT_URL"),
        aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
        aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
        region_name=os.environ.get("AWS_REGION", "us-east-1"),
        config=cfg,
    )


def _auth_get_and_drain(s3, bucket: str, key: str) -> Tuple[float, float, int]:
    t0 = time.monotonic()
    obj = s3.get_object(Bucket=bucket, Key=key)
    body = obj["Body"]
    ttfb_start = time.monotonic()
    it = body.iter_chunks(chunk_size=8192)
    first = next(it, b"")
    ttfb_ms = (time.monotonic() - ttfb_start) * 1000.0
    total = len(first)
    for c in it:
        total += len(c)
    return ttfb_ms, (time.monotonic() - t0), total


def _anon_get_and_drain(endpoint_url: str, bucket: str, key: str) -> Tuple[float, float, int]:
    url = f"{endpoint_url.rstrip('/')}/{bucket}/{key}"
    r = requests.get(url, stream=True, timeout=30)
    r.raise_for_status()
    t0 = time.monotonic()
    it = r.iter_content(chunk_size=8192)
    first = next(it, b"")
    ttfb_ms = (time.monotonic() - t0) * 1000.0
    total = len(first)
    for c in it:
        total += len(c)
    return ttfb_ms, (time.monotonic() - t0), total


@dataclass
class CreatedObject:
    bucket: str
    key: str
    visibility: str  # "private" or "public"
    kind: str  # "single" or "multi"
    size_mb: int
    part_size_mb: Optional[int]
    etag: str
    md5: str = ""


@dataclass
class State:
    endpoint_url: str
    created_pre: List[CreatedObject]
    created_post: List[CreatedObject]


@dataclass
class TimingRecord:
    phase: str
    endpoint_url: str
    visibility: str
    bucket: str
    key: str
    operation: str  # auth_get[_cached|_after_clear] or anon_get[_cached|_after_clear]
    attempts: int
    ttfb_ms_avg: float
    total_s_avg: float


def _measure_auth_get_avg(s3, bucket: str, key: str, attempts: int) -> Tuple[float, float]:
    if attempts <= 0:
        attempts = 1
    ttfb_vals: List[float] = []
    total_vals: List[float] = []
    for _ in range(attempts):
        ttfb_ms, total_s, _ = _auth_get_and_drain(s3, bucket, key)
        ttfb_vals.append(ttfb_ms)
        total_vals.append(total_s)
    return (sum(ttfb_vals) / len(ttfb_vals), sum(total_vals) / len(total_vals))


def _measure_anon_get_avg(endpoint_url: str, bucket: str, key: str, attempts: int) -> Tuple[float, float]:
    if attempts <= 0:
        attempts = 1
    ttfb_vals: List[float] = []
    total_vals: List[float] = []
    for _ in range(attempts):
        ttfb_ms, total_s, _ = _anon_get_and_drain(endpoint_url, bucket, key)
        ttfb_vals.append(ttfb_ms)
        total_vals.append(total_s)
    return (sum(ttfb_vals) / len(ttfb_vals), sum(total_vals) / len(total_vals))


def _measure_auth_get_avg_round_robin(
    s3,
    bucket: str,
    keys: List[str],
    attempts: int,
) -> Dict[str, Tuple[float, float]]:
    """Round-robin through keys to compute per-key averages fairly over time."""
    if attempts <= 0:
        attempts = 1
    per_key_ttfb: Dict[str, List[float]] = {k: [] for k in keys}
    per_key_total: Dict[str, List[float]] = {k: [] for k in keys}
    for _ in range(attempts):
        for k in keys:
            ttfb_ms, total_s, _ = _auth_get_and_drain(s3, bucket, k)
            per_key_ttfb[k].append(ttfb_ms)
            per_key_total[k].append(total_s)
    return {
        k: (sum(per_key_ttfb[k]) / len(per_key_ttfb[k]), sum(per_key_total[k]) / len(per_key_total[k])) for k in keys
    }


def run_pre(
    s3,
    endpoint_url: str,
    private_bucket: str,
    public_bucket: Optional[str],
    total_size_mb: int,
    part_sizes_mb: List[int],
    prefix: str,
    read_attempts: int,
    phase: str,
    timings: List[TimingRecord],
    *,
    attempt_files: int = 1,
    include_anon: bool = False,
    cache_bench: bool = False,
) -> List[CreatedObject]:
    created: List[CreatedObject] = []
    # Helper to optionally include public bucket
    vis_list: List[Tuple[Optional[str], str]] = [(private_bucket, "private")]
    if public_bucket:
        vis_list.append((public_bucket, "public"))

    # Accumulators for interleaved cache bench
    single_keys_by_vis: Dict[str, Tuple[str, List[str]]] = {}
    multi_keys_by_vis: Dict[str, Tuple[str, List[str]]] = {}

    # Single-part (per visibility)
    for vis_bucket, vis in vis_list:
        if not vis_bucket:
            continue
        keys: List[str] = []
        print(f"CREATE single {vis} {vis_bucket} x{max(1, attempt_files)} of {total_size_mb}MB", flush=True)
        for i in range(1, max(1, attempt_files) + 1):
            key = f"{prefix}/single-{total_size_mb}MB-{_rand_suffix()}-{i}.bin"
            body = _generate_bytes(total_size_mb, 0xA5)
            md5 = hashlib.md5(body).hexdigest()
            s3.put_object(Bucket=vis_bucket, Key=key, Body=io.BytesIO(body), ContentType="application/octet-stream")
            created.append(CreatedObject(vis_bucket, key, vis, "single", total_size_mb, None, etag="", md5=md5))
            keys.append(key)
        single_keys_by_vis[vis] = (vis_bucket, keys)

        if not cache_bench:
            # Round-robin averaging across keys for fairness
            rr = _measure_auth_get_avg_round_robin(s3, vis_bucket, keys, read_attempts)
            for key, (ttfb_avg, total_avg) in rr.items():
                print(
                    f"AUTH {vis} {vis_bucket}/{key} attempts={read_attempts} avg_ttfb_ms={ttfb_avg:.2f} avg_total_s={total_avg:.3f}"
                )
                timings.append(
                    TimingRecord(
                        phase=phase,
                        endpoint_url=endpoint_url,
                        visibility=vis,
                        bucket=vis_bucket,
                        key=key,
                        operation="auth_get",
                        attempts=read_attempts,
                        ttfb_ms_avg=ttfb_avg,
                        total_s_avg=total_avg,
                    )
                )
                if include_anon and vis == "public":
                    ttfb_avg_p, total_avg_p = _measure_anon_get_avg(endpoint_url, vis_bucket, key, read_attempts)
                    print(
                        f"ANON public {vis_bucket}/{key} attempts={read_attempts} avg_ttfb_ms={ttfb_avg_p:.2f} avg_total_s={total_avg_p:.3f}"
                    )
                    timings.append(
                        TimingRecord(
                            phase=phase,
                            endpoint_url=endpoint_url,
                            visibility=vis,
                            bucket=vis_bucket,
                            key=key,
                            operation="anon_get",
                            attempts=read_attempts,
                            ttfb_ms_avg=ttfb_avg_p,
                            total_s_avg=total_avg_p,
                        )
                    )

    # Multipart
    for vis_bucket, vis in vis_list:
        if not vis_bucket:
            continue
        for ps in part_sizes_mb:
            if total_size_mb % ps != 0:
                continue
            keys: List[str] = []
            print(
                f"CREATE multi {vis} {vis_bucket} x{max(1, attempt_files)} total={total_size_mb}MB part={ps}MB",
                flush=True,
            )
            for i in range(1, max(1, attempt_files) + 1):
                key = f"{prefix}/multi-{total_size_mb}MB-{ps}MB-{_rand_suffix()}-{i}.bin"
                # Pre-compute expected md5 for the deterministic multipart content
                one_mb = 1024 * 1024
                hasher = hashlib.md5()
                for pn in range(1, (total_size_mb // ps) + 1):
                    block = bytes([pn % 256]) * one_mb
                    for _ in range(ps):
                        hasher.update(block)
                etag, _ = _multipart_upload(s3, vis_bucket, key, total_size_mb, ps, progress=True)
                created.append(
                    CreatedObject(vis_bucket, key, vis, "multi", total_size_mb, ps, etag=etag, md5=hasher.hexdigest())
                )
                keys.append(key)
            multi_keys_by_vis[vis] = (vis_bucket, keys)

            if not cache_bench:
                rr = _measure_auth_get_avg_round_robin(s3, vis_bucket, keys, read_attempts)
                for key, (ttfb_avg, total_avg) in rr.items():
                    print(
                        f"AUTH {vis} {vis_bucket}/{key} attempts={read_attempts} avg_ttfb_ms={ttfb_avg:.2f} avg_total_s={total_avg:.3f}"
                    )
                    timings.append(
                        TimingRecord(
                            phase=phase,
                            endpoint_url=endpoint_url,
                            visibility=vis,
                            bucket=vis_bucket,
                            key=key,
                            operation="auth_get",
                            attempts=read_attempts,
                            ttfb_ms_avg=ttfb_avg,
                            total_s_avg=total_avg,
                        )
                    )
                    if include_anon and vis == "public":
                        ttfb_avg_p, total_avg_p = _measure_anon_get_avg(endpoint_url, vis_bucket, key, read_attempts)
                        print(
                            f"ANON public {vis_bucket}/{key} attempts={read_attempts} avg_ttfb_ms={ttfb_avg_p:.2f} avg_total_s={total_avg_p:.3f}"
                        )
                        timings.append(
                            TimingRecord(
                                phase=phase,
                                endpoint_url=endpoint_url,
                                visibility=vis,
                                bucket=vis_bucket,
                                key=key,
                                operation="anon_get",
                                attempts=read_attempts,
                                ttfb_ms_avg=ttfb_avg_p,
                                total_s_avg=total_avg_p,
                            )
                        )
    # If cache_bench, do interleaved cached pass now (auth, and optionally anon for public)
    if cache_bench:

        def interleave_and_bench(keys_by_vis: Dict[str, Tuple[str, List[str]]], label: str) -> None:
            # Build lists maintaining order private, public if available
            order: List[Tuple[str, str, List[str]]] = []
            if "private" in keys_by_vis:
                b, lst = keys_by_vis["private"]
                order.append(("private", b, lst))
            if "public" in keys_by_vis:
                b, lst = keys_by_vis["public"]
                order.append(("public", b, lst))
            max_len = max((len(lst) for (_, _, lst) in order), default=0)
            for i in range(max_len):
                for vis, bucket_name, lst in order:
                    if i >= len(lst):
                        continue
                    key = lst[i]
                    ttfb_ms, total_s, _ = _auth_get_and_drain(s3, bucket_name, key)
                    print(f"AUTH_{label} {vis} {bucket_name}/{key} ttfb_ms={ttfb_ms:.2f} total_s={total_s:.3f}")
                    timings.append(
                        TimingRecord(
                            phase=phase,
                            endpoint_url=endpoint_url,
                            visibility=vis,
                            bucket=bucket_name,
                            key=key,
                            operation=f"auth_get_{label.lower()}",
                            attempts=1,
                            ttfb_ms_avg=ttfb_ms,
                            total_s_avg=total_s,
                        )
                    )
                    if include_anon and vis == "public":
                        ttfb_ms_p, total_s_p, _ = _anon_get_and_drain(endpoint_url, bucket_name, key)
                        print(
                            f"ANON_{label} public {bucket_name}/{key} ttfb_ms={ttfb_ms_p:.2f} total_s={total_s_p:.3f}"
                        )
                        timings.append(
                            TimingRecord(
                                phase=phase,
                                endpoint_url=endpoint_url,
                                visibility=vis,
                                bucket=bucket_name,
                                key=key,
                                operation=f"anon_get_{label.lower()}",
                                attempts=1,
                                ttfb_ms_avg=ttfb_ms_p,
                                total_s_avg=total_s_p,
                            )
                        )

        # Singles then multis in interleaved order
        print("BENCH cached singles", flush=True)
        interleave_and_bench(single_keys_by_vis, "CACHED")
        print("BENCH cached multis", flush=True)
        interleave_and_bench(multi_keys_by_vis, "CACHED")

    return created


def run_post_verify(
    s3,
    endpoint_url: str,
    created_pre: List[CreatedObject],
    *,
    timings: List[TimingRecord],
    phase: str,
    include_anon: bool,
) -> None:
    for obj in created_pre:
        # Authenticated GET and record timing
        if getattr(obj, "md5", ""):
            # Verify MD5 when present in state
            t0 = time.monotonic()
            obj_r = s3.get_object(Bucket=obj.bucket, Key=obj.key)
            body = obj_r["Body"]
            ttfb_start = time.monotonic()
            hasher = hashlib.md5()
            it = body.iter_chunks(chunk_size=8192)
            first = next(it, b"")
            ttfb_ms = (time.monotonic() - ttfb_start) * 1000.0
            if first:
                hasher.update(first)
            total = len(first)
            for c in it:
                hasher.update(c)
                total += len(c)
            total_s = time.monotonic() - t0
            md5_hex = hasher.hexdigest()
            if md5_hex != obj.md5:
                print(
                    f"MD5 MISMATCH {obj.bucket}/{obj.key} expected={obj.md5} got={md5_hex}",
                    file=sys.stderr,
                )
        else:
            ttfb_ms, total_s, _ = _auth_get_and_drain(s3, obj.bucket, obj.key)
        print(f"AUTH_VERIFY {obj.visibility} {obj.bucket}/{obj.key} ttfb_ms={ttfb_ms:.2f} total_s={total_s:.3f}")
        timings.append(
            TimingRecord(
                phase=phase,
                endpoint_url=endpoint_url,
                visibility=obj.visibility,
                bucket=obj.bucket,
                key=obj.key,
                operation="auth_get_verify",
                attempts=1,
                ttfb_ms_avg=ttfb_ms,
                total_s_avg=total_s,
            )
        )
        # Anonymous for public if requested
        if include_anon and obj.visibility == "public":
            ttfb_ms_p, total_s_p, _ = _anon_get_and_drain(endpoint_url, obj.bucket, obj.key)
            print(f"ANON_VERIFY public {obj.bucket}/{obj.key} ttfb_ms={ttfb_ms_p:.2f} total_s={total_s_p:.3f}")
            timings.append(
                TimingRecord(
                    phase=phase,
                    endpoint_url=endpoint_url,
                    visibility=obj.visibility,
                    bucket=obj.bucket,
                    key=obj.key,
                    operation="anon_get_verify",
                    attempts=1,
                    ttfb_ms_avg=ttfb_ms_p,
                    total_s_avg=total_s_p,
                )
            )


def main() -> None:
    ap = argparse.ArgumentParser(description="Deployment-aware smoke test (pre/post)")
    ap.add_argument("--phase", choices=["pre", "post"], required=True)
    ap.add_argument("--state-file", required=True)
    ap.add_argument("--bucket-private", required=True)
    ap.add_argument("--bucket-public", default="")
    ap.add_argument("--endpoint-url", default=os.environ.get("S3_ENDPOINT_URL", ""))
    ap.add_argument("--total-size-mb", type=int, default=64)
    ap.add_argument("--part-sizes", default="8")
    ap.add_argument("--prefix", default=f"deploy-smoke-{int(time.time())}")
    ap.add_argument("--read-attempts", type=int, default=1, help="Repeat reads per object to compute average time")
    ap.add_argument(
        "--attempt-files", type=int, default=1, help="Create N distinct files per scenario (e.g., 3 => -1,-2,-3)"
    )
    ap.add_argument(
        "--cache-bench",
        action="store_true",
        help="Measure cached round then pause for manual cache clear, then measure cold round",
    )
    ap.add_argument("--include-anon", action="store_true", help="Also benchmark anonymous GETs for public bucket")
    ap.add_argument("--csv", default="", help="Optional CSV timings output path")
    ap.add_argument(
        "--no-post-create",
        action="store_true",
        help="On post phase, do not create new objects; only verify and benchmark existing pre objects",
    )
    args = ap.parse_args()

    endpoint_url = args.endpoint_url or os.environ.get("S3_ENDPOINT_URL", "")
    if not endpoint_url:
        print("Missing endpoint URL (provide --endpoint-url or S3_ENDPOINT_URL)", file=sys.stderr)
        sys.exit(2)

    s3 = _get_s3_client(endpoint_url)
    part_sizes = [int(x.strip()) for x in args.part_sizes.split(",") if x.strip()]

    timings: List[TimingRecord] = []

    if args.phase == "pre":
        created_pre = run_pre(
            s3=s3,
            endpoint_url=endpoint_url,
            private_bucket=args.bucket_private,
            public_bucket=(args.bucket_public or None),
            total_size_mb=args.total_size_mb,
            part_sizes_mb=part_sizes,
            prefix=args.prefix,
            read_attempts=args.read_attempts,
            phase="pre",
            timings=timings,
            attempt_files=max(1, args.attempt_files),
            include_anon=bool(args.include_anon),
            cache_bench=bool(args.cache_bench),
        )
        state = State(endpoint_url=endpoint_url, created_pre=created_pre, created_post=[])
        with open(args.state_file, "w") as f:
            json.dump(
                {
                    "endpoint_url": state.endpoint_url,
                    "created_pre": [asdict(o) for o in state.created_pre],
                    "created_post": [],
                },
                f,
                indent=2,
            )
        print(f"Wrote state to {args.state_file} with {len(created_pre)} objects")
        # Optional CSV
        if args.csv:
            with open(args.csv, "w", newline="") as f:
                w = csv.writer(f)
                w.writerow(
                    [
                        "phase",
                        "endpoint",
                        "visibility",
                        "bucket",
                        "key",
                        "operation",
                        "attempts",
                        "avg_ttfb_ms",
                        "avg_total_s",
                    ]
                )
                for t in timings:
                    w.writerow(
                        [
                            t.phase,
                            t.endpoint_url,
                            t.visibility,
                            t.bucket,
                            t.key,
                            t.operation,
                            t.attempts,
                            f"{t.ttfb_ms_avg:.3f}",
                            f"{t.total_s_avg:.6f}",
                        ]
                    )
        return

    # post
    try:
        with open(args.state_file, "r") as f:
            data: Dict = json.load(f)
    except Exception as e:
        print(f"Failed to read state file {args.state_file}: {e}", file=sys.stderr)
        sys.exit(2)

    created_pre = [CreatedObject(**o) for o in data.get("created_pre", [])]
    run_post_verify(
        s3=s3,
        endpoint_url=endpoint_url,
        created_pre=created_pre,
        timings=timings,
        phase="post",
        include_anon=bool(args.include_anon),
    )

    # Create new files after deploy as an additional check
    if args.no_post_create:
        created_post = []
        print("Skipping post-create as requested (using only pre objects)")
    else:
        created_post = run_pre(
            s3=s3,
            endpoint_url=endpoint_url,
            private_bucket=args.bucket_private,
            public_bucket=(args.bucket_public or None),
            total_size_mb=args.total_size_mb,
            part_sizes_mb=part_sizes,
            prefix=f"{args.prefix}-post",
            read_attempts=args.read_attempts,
            phase="post",
            timings=timings,
            attempt_files=max(1, args.attempt_files),
            include_anon=bool(args.include_anon),
            cache_bench=bool(args.cache_bench),
        )

    # Persist updated state
    with open(args.state_file, "w") as f:
        json.dump(
            {
                "endpoint_url": endpoint_url,
                "created_pre": [asdict(o) for o in created_pre],
                "created_post": [asdict(o) for o in created_post],
            },
            f,
            indent=2,
        )
    print(f"Post verification complete. Added {len(created_post)} new objects. Updated {args.state_file}.")
    if args.cache_bench:
        # Pause for manual cache clear
        try:
            input("Press Enter after clearing cache to run cold download timings...")
        except EOFError:
            pass

        # Interleaved cold round: single then multi, private then public per index
        def interleave_after_clear(objs: List[CreatedObject]) -> None:
            # Group by (visibility, kind)
            singles: Dict[str, List[CreatedObject]] = {"private": [], "public": []}
            multis: Dict[str, List[CreatedObject]] = {"private": [], "public": []}
            for o in objs:
                target = singles if o.kind == "single" else multis
                target.setdefault(o.visibility, []).append(o)
            # Preserve insertion order
            max_len_single = max(len(singles.get("private", [])), len(singles.get("public", [])))
            for i in range(max_len_single):
                for vis in ("private", "public"):
                    arr = singles.get(vis, [])
                    if i < len(arr):
                        o = arr[i]
                        ttfb_ms, total_s, _ = _auth_get_and_drain(s3, o.bucket, o.key)
                        print(
                            f"AUTH_AFTER_CLEAR {o.visibility} {o.bucket}/{o.key} ttfb_ms={ttfb_ms:.2f} total_s={total_s:.3f}"
                        )
                        timings.append(
                            TimingRecord(
                                phase="post",
                                endpoint_url=endpoint_url,
                                visibility=o.visibility,
                                bucket=o.bucket,
                                key=o.key,
                                operation="auth_get_after_clear",
                                attempts=1,
                                ttfb_ms_avg=ttfb_ms,
                                total_s_avg=total_s,
                            )
                        )
            max_len_multi = max(len(multis.get("private", [])), len(multis.get("public", [])))
            for i in range(max_len_multi):
                for vis in ("private", "public"):
                    arr = multis.get(vis, [])
                    if i < len(arr):
                        o = arr[i]
                        ttfb_ms, total_s, _ = _auth_get_and_drain(s3, o.bucket, o.key)
                        print(
                            f"AUTH_AFTER_CLEAR {o.visibility} {o.bucket}/{o.key} ttfb_ms={ttfb_ms:.2f} total_s={total_s:.3f}"
                        )
                        timings.append(
                            TimingRecord(
                                phase="post",
                                endpoint_url=endpoint_url,
                                visibility=o.visibility,
                                bucket=o.bucket,
                                key=o.key,
                                operation="auth_get_after_clear",
                                attempts=1,
                                ttfb_ms_avg=ttfb_ms,
                                total_s_avg=total_s,
                            )
                        )

        print("BENCH after-clear singles/multis", flush=True)
        interleave_after_clear(created_pre + created_post)
    if args.csv:
        # Append to CSV (or create if missing)
        mode = "a" if os.path.exists(args.csv) else "w"
        with open(args.csv, mode, newline="") as f:
            w = csv.writer(f)
            if mode == "w":
                w.writerow(
                    [
                        "phase",
                        "endpoint",
                        "visibility",
                        "bucket",
                        "key",
                        "operation",
                        "attempts",
                        "avg_ttfb_ms",
                        "avg_total_s",
                    ]
                )
            for t in timings:
                w.writerow(
                    [
                        t.phase,
                        t.endpoint_url,
                        t.visibility,
                        t.bucket,
                        t.key,
                        t.operation,
                        t.attempts,
                        f"{t.ttfb_ms_avg:.3f}",
                        f"{t.total_s_avg:.6f}",
                    ]
                )


if __name__ == "__main__":
    main()
