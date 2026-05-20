#!/usr/bin/env python3
# Benchmark hippius-s3 upload + download against two buckets:
#   - private bucket: gateway emits Cache-Control: private,no-store → ATS forwards every GET
#   - public bucket:  gateway emits Cache-Control: public,max-age=300 → ATS caches
# First anon GET on a freshly-uploaded public object is TCP_MISS (cold); subsequent GETs
# hit TCP_HIT / TCP_MEM_HIT. We classify warm/cold from the Age header and
# x-hippius-ray-id continuity. Source .aws.cli.env before running.

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import os
import secrets
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from urllib.parse import urlsplit

import boto3
import requests
from botocore.client import Config as BotoConfig
from dotenv import load_dotenv


REPO_ROOT = Path(__file__).resolve().parent.parent
RESULTS_BUCKET = "veggies"
RESULTS_ENDPOINT = "https://s3.hippius.com"
ENDPOINT_BY_ZONE = {
    "eu": "https://eu-central-1.hippius.com",
    "us": "https://us-east-1.hippius.com",
}
GITHUB_REPO = "thenervelab/hippius-s3"
GITHUB_TRACKED_BRANCH = "k8s-production"


@dataclass
class GetResult:
    label: str
    bucket: str
    key: str
    status: int
    bytes_received: int
    ttfb_seconds: float
    total_seconds: float
    age: int | None
    ats_hit: bool  # set later, based on ray-id continuity per URL
    x_hippius_source: str | None
    x_hippius_ray_id: str | None
    md5: str


def make_s3_client(endpoint: str):
    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        config=BotoConfig(signature_version="s3v4", retries={"max_attempts": 3, "mode": "standard"}),
    )


def ensure_bucket(s3, bucket: str, public: bool) -> None:
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if bucket not in existing:
        s3.create_bucket(Bucket=bucket)
    s3.put_bucket_acl(Bucket=bucket, ACL="public-read" if public else "private")


def generate_random_file(path: Path, size_mb: int) -> str:
    h = hashlib.md5()
    remaining = size_mb * 1024 * 1024
    chunk = 1024 * 1024
    with path.open("wb") as f:
        while remaining > 0:
            n = min(chunk, remaining)
            buf = secrets.token_bytes(n)
            f.write(buf)
            h.update(buf)
            remaining -= n
    return h.hexdigest()


def upload(s3, bucket: str, key: str, path: Path) -> float:
    t0 = time.monotonic()
    with path.open("rb") as f:
        s3.put_object(Bucket=bucket, Key=key, Body=f)
    return time.monotonic() - t0


def download(label: str, bucket: str, key: str, url: str, session: requests.Session) -> GetResult:
    h = hashlib.md5()
    bytes_in = 0
    ttfb: float | None = None
    t0 = time.monotonic()
    with session.get(url, stream=True, timeout=300) as r:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if ttfb is None:
                ttfb = time.monotonic() - t0
            if chunk:
                h.update(chunk)
                bytes_in += len(chunk)
        if ttfb is None:
            ttfb = time.monotonic() - t0
        total = time.monotonic() - t0
        age_raw = r.headers.get("age")
        age = int(age_raw) if age_raw is not None and age_raw.isdigit() else None
        return GetResult(
            label=label,
            bucket=bucket,
            key=key,
            status=r.status_code,
            bytes_received=bytes_in,
            ttfb_seconds=ttfb,
            total_seconds=total,
            age=age,
            ats_hit=False,
            x_hippius_source=r.headers.get("x-hippius-source"),
            x_hippius_ray_id=r.headers.get("x-hippius-ray-id"),
            md5=h.hexdigest(),
        )


def presign(s3, bucket: str, key: str) -> str:
    return s3.generate_presigned_url("get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=3600)


def public_url(endpoint: str, bucket: str, key: str) -> str:
    parts = urlsplit(endpoint)
    return f"{parts.scheme}://{parts.netloc}/{bucket}/{key}"


def cleanup(s3, bucket: str, keys: list[str], remove_bucket: bool) -> None:
    for k in keys:
        s3.delete_object(Bucket=bucket, Key=k)
    if remove_bucket:
        s3.delete_bucket(Bucket=bucket)


def stats(values: list[float]) -> dict:
    if not values:
        return {"n": 0, "min": 0.0, "mean": 0.0, "p50": 0.0, "p95": 0.0, "max": 0.0}
    s = sorted(values)
    n = len(s)
    return {
        "n": n,
        "min": s[0],
        "mean": statistics.fmean(s),
        "p50": s[n // 2],
        "p95": s[min(n - 1, int(n * 0.95))],
        "max": s[-1],
    }


def _series(values: list[float]) -> dict:
    if not values:
        return {"min": 0.0, "max": 0.0, "avg": 0.0, "n": 0}
    return {
        "min": round(min(values), 2),
        "max": round(max(values), 2),
        "avg": round(statistics.fmean(values), 2),
        "n": len(values),
    }


def fetch_tracked_commit_sha() -> str | None:
    url = f"https://api.github.com/repos/{GITHUB_REPO}/commits/{GITHUB_TRACKED_BRANCH}"
    r = requests.get(url, headers={"Accept": "application/vnd.github+json"}, timeout=10)
    if r.status_code != 200:
        print(f"WARN: github sha lookup returned {r.status_code}; recording null", file=sys.stderr)
        return None
    return r.json().get("sha")


def build_result_payload(
    zone: str,
    endpoint: str,
    size_mb: int,
    trials: int,
    warm_replays: int,
    upload_speeds: list[float],
    upload_times: list[float],
    rows: list[GetResult],
    commit_sha: str | None,
    total_runtime_seconds: float,
) -> dict:
    speeds_by_label: dict[str, list[float]] = {}
    ttfb_by_label: dict[str, list[float]] = {}
    for r in rows:
        if r.total_seconds > 0:
            speeds_by_label.setdefault(r.label, []).append(size_mb / r.total_seconds)
        ttfb_by_label.setdefault(r.label, []).append(r.ttfb_seconds)
    uncached_speed = speeds_by_label.get("private", []) + speeds_by_label.get("public-cold", [])
    cached_speed = speeds_by_label.get("public-warm", [])
    uncached_ttfb = ttfb_by_label.get("private", []) + ttfb_by_label.get("public-cold", [])
    cached_ttfb = ttfb_by_label.get("public-warm", [])
    return {
        "schema": 3,
        "timestamp": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "zone": zone,
        "endpoint": endpoint,
        "file_size_mb": size_mb,
        "trials": trials,
        "warm_replays": warm_replays,
        "commit_sha": commit_sha,
        "commit_repo": GITHUB_REPO,
        "commit_branch": GITHUB_TRACKED_BRANCH,
        "total_runtime_seconds": round(total_runtime_seconds, 2),
        "upload_mb_s": _series(upload_speeds),
        "download_uncached_mb_s": _series(uncached_speed),
        "download_cached_mb_s": _series(cached_speed),
        "upload_seconds": _series(upload_times),
        "download_uncached_ttfb_seconds": _series(uncached_ttfb),
        "download_cached_ttfb_seconds": _series(cached_ttfb),
    }


def publish_results(payload: dict) -> str:
    s3 = make_s3_client(RESULTS_ENDPOINT)
    existing = {b["Name"] for b in s3.list_buckets().get("Buckets", [])}
    if RESULTS_BUCKET not in existing:
        s3.create_bucket(Bucket=RESULTS_BUCKET)
        s3.put_bucket_acl(Bucket=RESULTS_BUCKET, ACL="public-read")
    ts_compact = payload["timestamp"].replace("-", "").replace(":", "")
    key = f"s3/{payload['zone']}/results/benchmark_{ts_compact}.json"
    body = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
    s3.put_object(
        Bucket=RESULTS_BUCKET,
        Key=key,
        Body=body,
        ACL="public-read",
        ContentType="application/json",
        CacheControl="public, max-age=60",
    )
    return f"{RESULTS_ENDPOINT}/{RESULTS_BUCKET}/{key}"


def summarize(rows: list[GetResult], size_mb: int) -> None:
    by_label: dict[str, list[GetResult]] = {}
    for r in rows:
        by_label.setdefault(r.label, []).append(r)

    print()
    header = f"{'label':<14} {'n':>3} {'ttfb_p50':>9} {'ttfb_p95':>9} {'tot_p50':>9} {'tot_p95':>9} {'MBs_p50':>8} {'MBs_p95':>8} {'age_p50':>7} {'hit%':>5}"
    print(header)
    print("-" * len(header))
    for label in ("private", "public-cold", "public-warm"):
        if label not in by_label:
            continue
        items = by_label[label]
        ttfbs = [it.ttfb_seconds for it in items]
        totals = [it.total_seconds for it in items]
        speeds = [size_mb / it.total_seconds for it in items if it.total_seconds > 0]
        ages = sorted(it.age for it in items if it.age is not None)
        hits = sum(1 for it in items if it.ats_hit)
        s_t = stats(totals)
        s_ttfb = stats(ttfbs)
        s_s = stats(speeds)
        age_p50 = ages[len(ages) // 2] if ages else None
        hit_pct = 100.0 * hits / len(items)
        age_str = str(age_p50) if age_p50 is not None else "-"
        print(
            f"{label:<14} {len(items):>3} {s_ttfb['p50']:>9.3f} {s_ttfb['p95']:>9.3f} "
            f"{s_t['p50']:>9.3f} {s_t['p95']:>9.3f} {s_s['p50']:>8.1f} {s_s['p95']:>8.1f} "
            f"{age_str:>7} {hit_pct:>4.0f}%"
        )


def main() -> int:
    p = argparse.ArgumentParser(description="hippius-s3 ATS cold/warm download benchmark")
    p.add_argument("--endpoint", help="override endpoint URL (default: derived from zone)")
    p.add_argument("--zone", help="override HIPPIUS_BENCH_ZONE from .aws.cli.env (eu | us)")
    p.add_argument("--private-bucket", default=f"bench-private-{int(time.time())}")
    p.add_argument("--public-bucket", default=f"bench-public-{int(time.time())}")
    p.add_argument("--size", type=int, default=2048, help="object size in MB")
    p.add_argument("--trials", type=int, default=3)
    p.add_argument("--warm-replays", type=int, default=3, help="warm GETs per trial after the cold GET")
    p.add_argument("--keep", action="store_true", help="don't delete benchmark buckets/keys at the end")
    p.add_argument("--csv", help="write per-request rows to this CSV path (NEVER uploaded; local debug only)")
    p.add_argument("--no-publish", action="store_true", help="skip uploading the JSON summary to the results bucket")
    args = p.parse_args()

    # .aws.cli.env is a bash script with `aws configure set ...` and `echo` lines
    # that dotenv warns about per-line. Silence those — credentials still load.
    import logging
    logging.getLogger("dotenv.main").setLevel(logging.ERROR)
    load_dotenv(REPO_ROOT / ".aws.cli.env", override=False)

    if not (os.environ.get("AWS_ACCESS_KEY_ID") and os.environ.get("AWS_SECRET_ACCESS_KEY")):
        print("ERROR: AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY missing — populate .aws.cli.env", file=sys.stderr)
        return 2

    zone = (args.zone or os.environ.get("HIPPIUS_BENCH_ZONE", "")).strip().lower()
    if zone not in ENDPOINT_BY_ZONE:
        print(
            f"ERROR: HIPPIUS_BENCH_ZONE must be one of {sorted(ENDPOINT_BY_ZONE)} "
            "(set in .aws.cli.env or pass --zone)",
            file=sys.stderr,
        )
        return 2

    endpoint = args.endpoint or ENDPOINT_BY_ZONE[zone]

    commit_sha = fetch_tracked_commit_sha()
    s3 = make_s3_client(endpoint)
    print(f"Zone:            {zone}")
    print(f"Endpoint:        {endpoint}")
    print(f"Tracked SHA:     {commit_sha[:12] + '…' if commit_sha else 'unavailable'} ({GITHUB_REPO}@{GITHUB_TRACKED_BRANCH})")
    print(f"Private bucket:  {args.private_bucket}  (Cache-Control: private,no-store → ATS bypass)")
    print(f"Public bucket:   {args.public_bucket}  (Cache-Control: public,max-age=300 → ATS caches)")
    print(f"Size: {args.size} MB | Trials: {args.trials} | Warm replays per trial: {args.warm_replays}")
    print()

    ensure_bucket(s3, args.private_bucket, public=False)
    ensure_bucket(s3, args.public_bucket, public=True)

    tmp = Path(f"/tmp/s3-bench-{os.getpid()}.bin")
    rows: list[GetResult] = []
    upload_speeds: list[float] = []
    upload_times: list[float] = []
    keys_private: list[str] = []
    keys_public: list[str] = []
    session_signed = requests.Session()
    session_anon = requests.Session()
    # Per-URL first ray-id seen — subsequent GETs that report the same ray-id are
    # serving from a prior origin response, i.e. ATS cache hits.
    first_ray_by_url: dict[str, str] = {}
    # Measure only active S3 time — first upload to last download, excluding
    # file generation, bucket setup, github SHA lookup, and cleanup.
    s3_active_start: float | None = None

    def classify(url: str, result: GetResult) -> None:
        rid = result.x_hippius_ray_id
        if rid is None:
            return
        prior = first_ray_by_url.get(url)
        if prior is None:
            first_ray_by_url[url] = rid
            return
        result.ats_hit = rid == prior

    for trial in range(1, args.trials + 1):
        key = f"bench/trial-{trial}-{int(time.time() * 1000)}.bin"
        print(f"=== Trial {trial}/{args.trials} (key={key}) ===")
        expected_md5 = generate_random_file(tmp, args.size)
        print(f"  Generated {args.size}MB md5={expected_md5}")

        if s3_active_start is None:
            s3_active_start = time.monotonic()
        priv_elapsed = upload(s3, args.private_bucket, key, tmp)
        pub_elapsed = upload(s3, args.public_bucket, key, tmp)
        keys_private.append(key)
        keys_public.append(key)
        upload_speeds.append(args.size / priv_elapsed)
        upload_speeds.append(args.size / pub_elapsed)
        upload_times.append(priv_elapsed)
        upload_times.append(pub_elapsed)
        print(f"  Upload private: {priv_elapsed:.2f}s ({args.size / priv_elapsed:.1f} MB/s)")
        print(f"  Upload public:  {pub_elapsed:.2f}s ({args.size / pub_elapsed:.1f} MB/s)")

        url_priv = presign(s3, args.private_bucket, key)
        r = download("private", args.private_bucket, key, url_priv, session_signed)
        assert r.status == 200, f"private GET status {r.status}"
        assert r.md5 == expected_md5, "private GET md5 mismatch"
        classify(url_priv, r)
        rows.append(r)
        print(
            f"  Private DL:     ttfb={r.ttfb_seconds:.3f}s total={r.total_seconds:.3f}s "
            f"age={r.age} src={r.x_hippius_source} ray={r.x_hippius_ray_id}"
        )

        url_pub = public_url(endpoint, args.public_bucket, key)
        r_cold = download("public-cold", args.public_bucket, key, url_pub, session_anon)
        assert r_cold.status == 200, f"public cold GET status {r_cold.status}"
        assert r_cold.md5 == expected_md5, "public cold GET md5 mismatch"
        classify(url_pub, r_cold)
        rows.append(r_cold)
        print(
            f"  Public cold DL: ttfb={r_cold.ttfb_seconds:.3f}s total={r_cold.total_seconds:.3f}s "
            f"age={r_cold.age} src={r_cold.x_hippius_source} ray={r_cold.x_hippius_ray_id}"
        )

        for i in range(1, args.warm_replays + 1):
            r_warm = download("public-warm", args.public_bucket, key, url_pub, session_anon)
            assert r_warm.status == 200, f"public warm GET status {r_warm.status}"
            classify(url_pub, r_warm)
            rows.append(r_warm)
            print(
                f"  Public warm #{i}: ttfb={r_warm.ttfb_seconds:.3f}s total={r_warm.total_seconds:.3f}s "
                f"age={r_warm.age} ats_hit={r_warm.ats_hit}"
            )

    s3_active_end = time.monotonic()
    total_runtime_seconds = s3_active_end - (s3_active_start or s3_active_end)

    summarize(rows, args.size)
    s_up = stats(upload_speeds)
    print()
    print(f"Upload throughput (MB/s): p50={s_up['p50']:.1f}  p95={s_up['p95']:.1f}  max={s_up['max']:.1f}")
    print(f"Active S3 runtime:        {total_runtime_seconds:.2f}s (first upload → last download)")
    payload = build_result_payload(
        zone=zone,
        endpoint=endpoint,
        size_mb=args.size,
        trials=args.trials,
        warm_replays=args.warm_replays,
        upload_speeds=upload_speeds,
        upload_times=upload_times,
        rows=rows,
        commit_sha=commit_sha,
        total_runtime_seconds=total_runtime_seconds,
    )
    print()
    print("Result summary:")
    print(json.dumps(payload, indent=2, sort_keys=True))

    if args.no_publish:
        print("Skipped publish (--no-publish)")
    else:
        public_url_out = publish_results(payload)
        print(f"Published: {public_url_out}")

    if args.csv:
        with open(args.csv, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "label", "bucket", "key", "status", "bytes",
                    "ttfb_s", "total_s", "age", "ats_hit", "source", "ray_id", "md5",
                ]
            )
            for r in rows:
                w.writerow(
                    [
                        r.label, r.bucket, r.key, r.status, r.bytes_received,
                        f"{r.ttfb_seconds:.4f}", f"{r.total_seconds:.4f}",
                        r.age if r.age is not None else "",
                        int(r.ats_hit), r.x_hippius_source or "", r.x_hippius_ray_id or "", r.md5,
                    ]
                )
        print(f"CSV written to {args.csv}")

    tmp.unlink(missing_ok=True)

    if not args.keep:
        cleanup(s3, args.private_bucket, keys_private, remove_bucket=True)
        cleanup(s3, args.public_bucket, keys_public, remove_bucket=True)
        print("Cleaned up buckets and keys")
    else:
        print("Skipped cleanup (--keep)")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
