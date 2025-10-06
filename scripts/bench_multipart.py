#!/usr/bin/env python3
import argparse
import concurrent.futures
import contextlib
import csv
import io
import os
import sys
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from typing import List
from typing import Tuple

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError


@dataclass
class UploadResult:
    size_mb: int
    run: int
    key: str
    seconds: float
    etag: str


@dataclass
class DownloadResult:
    size_mb: int
    run: int
    key: str
    ttfb_ms: float
    seconds: float
    status: str


def _generate_part_bytes(part_size_mb: int, seed_byte: int) -> bytes:
    """Generate deterministic bytes for a part without allocating massive buffers repeatedly."""
    size = part_size_mb * 1024 * 1024
    # Use a repeating pattern; memory copy is acceptable for <= 64MB parts
    pattern = bytes([seed_byte % 256]) * 1024 * 1024  # 1MB block
    blocks, rem = divmod(size, len(pattern))
    return pattern * blocks + pattern[:rem]


def _multipart_upload(
    s3,
    bucket: str,
    key: str,
    total_size_mb: int,
    part_size_mb: int,
    max_workers: int,
) -> str:
    if total_size_mb % part_size_mb != 0:
        raise ValueError("total_size_mb must be divisible by part_size_mb")

    num_parts = total_size_mb // part_size_mb
    create = s3.create_multipart_upload(Bucket=bucket, Key=key)
    upload_id = create["UploadId"]

    uploaded: List[Tuple[int, str]] = []

    def _upload_one(part_number: int) -> Tuple[int, str]:
        data = _generate_part_bytes(part_size_mb, seed_byte=part_number)
        resp = s3.upload_part(
            Bucket=bucket,
            Key=key,
            PartNumber=part_number,
            UploadId=upload_id,
            Body=io.BytesIO(data),
        )
        return part_number, resp["ETag"]

    try:
        if max_workers <= 1:
            uploaded.extend(_upload_one(pn) for pn in range(1, num_parts + 1))
        else:
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as pool:
                for pn, etag in pool.map(_upload_one, range(1, num_parts + 1)):
                    uploaded.append((pn, etag))

        parts = [{"PartNumber": pn, "ETag": etag} for pn, etag in sorted(uploaded)]
        comp = s3.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=upload_id,
            MultipartUpload={"Parts": parts},
        )
        return comp.get("ETag", "")
    except Exception:
        with contextlib.suppress(Exception):
            s3.abort_multipart_upload(Bucket=bucket, Key=key, UploadId=upload_id)
        raise


def benchmark_upload(
    s3,
    bucket: str,
    prefix: str,
    sizes_mb: Iterable[int],
    runs: int,
    total_size_mb: int,
    max_workers: int,
) -> List[UploadResult]:
    results: List[UploadResult] = []
    for size_mb in sizes_mb:
        for run in range(1, runs + 1):
            key = f"{prefix.rstrip('/')}/testfile-{size_mb}-run{run}.bin"
            t0 = time.monotonic()
            etag = _multipart_upload(
                s3=s3,
                bucket=bucket,
                key=key,
                total_size_mb=total_size_mb,
                part_size_mb=size_mb,
                max_workers=max_workers,
            )
            dt = time.monotonic() - t0
            results.append(UploadResult(size_mb=size_mb, run=run, key=key, seconds=dt, etag=etag))
    return results


def benchmark_download(
    s3,
    bucket: str,
    keys: List[Tuple[int, int, str]],
) -> List[DownloadResult]:
    results: List[DownloadResult] = []
    for size_mb, run, key in keys:
        t0 = time.monotonic()
        ttfb_ms: float = float("nan")
        status: str = ""
        total_seconds: float = 0.0
        try:
            obj = s3.get_object(Bucket=bucket, Key=key)
            body = obj["Body"]
            # Time to first chunk
            ttfb_start = time.monotonic()
            chunk = next(body.iter_chunks(chunk_size=8192))
            ttfb_ms = (time.monotonic() - ttfb_start) * 1000.0
            # Drain the rest
            total_bytes = len(chunk)
            for c in body.iter_chunks(chunk_size=1024 * 1024):
                total_bytes += len(c)
            total_seconds = time.monotonic() - t0
            status = f"ok:{total_bytes}"
        except Exception as e:
            total_seconds = time.monotonic() - t0
            if isinstance(e, ClientError):
                http_status = e.response.get("ResponseMetadata", {}).get("HTTPStatusCode")
                error_code = e.response.get("Error", {}).get("Code")
                status = f"error:{http_status or error_code}"
            else:
                status = f"error:{e.__class__.__name__}"
        results.append(
            DownloadResult(
                size_mb=size_mb,
                run=run,
                key=key,
                ttfb_ms=ttfb_ms,
                seconds=total_seconds,
                status=status,
            )
        )
    return results


def _parse_sizes(s: str) -> List[int]:
    return [int(x.strip()) for x in s.split(",") if x.strip()]


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark multipart upload/download at different part sizes")
    ap.add_argument("--bucket", required=True, help="S3 bucket name")
    ap.add_argument("--prefix", default="bench-multipart", help="Key prefix to write under")
    ap.add_argument("--endpoint-url", default=os.environ.get("S3_ENDPOINT_URL", ""), help="S3 endpoint URL")
    ap.add_argument("--sizes", default="8,16,32", help="Comma-separated part sizes in MB")
    ap.add_argument("--runs", type=int, default=3, help="Number of runs per size")
    ap.add_argument("--total-size-mb", type=int, default=96, help="Total object size per test (MB)")
    ap.add_argument("--workers", type=int, default=4, help="Concurrent upload workers for parts")
    ap.add_argument("--csv", default="", help="Optional CSV output file path")
    ap.add_argument("--mode", choices=["upload", "download", "both"], default="both")
    ap.add_argument(
        "--confirm-after-upload",
        action="store_true",
        help="Prompt for Enter after all uploads complete (to manually clear Redis)",
    )
    ap.add_argument("--no-cleanup", action="store_true", help="Don't delete uploaded files after testing")
    args = ap.parse_args()

    sizes_mb = _parse_sizes(args.sizes)
    for s in sizes_mb:
        if args.total_size_mb % s != 0:
            sys.exit(2)

    boto_cfg = BotoConfig(retries={"max_attempts": 5, "mode": "standard"})
    session = boto3.session.Session()
    s3 = session.client("s3", endpoint_url=(args.endpoint_url or None), config=boto_cfg)

    upload_rows: List[UploadResult] = []
    download_rows: List[DownloadResult] = []

    if args.mode in ("upload", "both"):
        upload_rows = benchmark_upload(
            s3=s3,
            bucket=args.bucket,
            prefix=args.prefix,
            sizes_mb=sizes_mb,
            runs=args.runs,
            total_size_mb=args.total_size_mb,
            max_workers=args.workers,
        )

        # Single confirmation after all uploads complete
        if args.confirm_after_upload and upload_rows:
            with contextlib.suppress(EOFError):
                input(f"All uploads complete ({len(upload_rows)} files). Clear Redis, then press Enter to continue...")

    if args.mode in ("download", "both"):
        # Use keys from prior upload set if available; otherwise derive names
        keys: List[Tuple[int, int, str]] = []
        if upload_rows:
            keys = [(r.size_mb, r.run, r.key) for r in upload_rows]
        else:
            for size_mb in sizes_mb:
                for run in range(1, args.runs + 1):
                    key = f"{args.prefix.rstrip('/')}/testfile-{size_mb}-run{run}.bin"
                    keys.append((size_mb, run, key))
        download_rows = benchmark_download(
            s3=s3,
            bucket=args.bucket,
            keys=keys,
        )

    # Cleanup: delete uploaded files
    if upload_rows and not args.no_cleanup:
        for result in upload_rows:
            try:
                s3.delete_object(Bucket=args.bucket, Key=result.key)
            except ClientError:
                pass
            except Exception:
                pass

    if args.csv:
        with Path(args.csv).open("w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(
                ["phase", "size_mb", "run", "key", "upload_seconds", "ttfb_ms", "download_seconds", "status", "etag"]
            )
            for r in upload_rows:
                writer.writerow(["upload", r.size_mb, r.run, r.key, f"{r.seconds:.3f}", "", "", "", r.etag])
            for r in download_rows:
                writer.writerow(
                    ["download", r.size_mb, r.run, r.key, "", f"{r.ttfb_ms:.1f}", f"{r.seconds:.3f}", r.status, ""]
                )


if __name__ == "__main__":
    main()
