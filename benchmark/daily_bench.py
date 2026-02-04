#!/usr/bin/env python3
"""Lightweight daily benchmark for trend tracking."""

import csv
import json
import os
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path

import boto3
from boto3.s3.transfer import TransferConfig
from botocore.config import Config
from botocore.exceptions import ClientError

FILE_SIZES_MB = [1, 100, 1024]
ITERATIONS = 3
BENCH_PREFIX = "bench-data/"

RESULTS_BUCKET = "hippius-benchmarks"
CSV_KEY = "daily.csv"

MULTIPART_THRESHOLD = 64 * 1024 * 1024
MULTIPART_CHUNKSIZE = 64 * 1024 * 1024
MAX_CONCURRENT_REQUESTS = 10


def ensure_bucket_exists(client, bucket: str):
    """Create bucket if it doesn't exist, with public-read policy for GetObject."""
    try:
        client.head_bucket(Bucket=bucket)
        print(f"Bucket {bucket} exists")
        return
    except ClientError as e:
        if e.response["Error"]["Code"] != "404":
            raise

    print(f"Creating bucket {bucket}")
    client.create_bucket(Bucket=bucket)

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Principal": "*",
                "Action": ["s3:GetObject"],
                "Resource": [f"arn:aws:s3:::{bucket}/*"],
            }
        ],
    }
    client.put_bucket_policy(Bucket=bucket, Policy=json.dumps(policy))
    print(f"Set public-read policy on {bucket}")


def cleanup_bench_data(client, bucket: str):
    """Delete all objects under the bench-data/ prefix."""
    paginator = client.get_paginator("list_objects_v2")
    to_delete: list[dict[str, str]] = []

    for page in paginator.paginate(Bucket=bucket, Prefix=BENCH_PREFIX):
        for obj in page.get("Contents", []) or []:
            k = obj.get("Key")
            if k:
                to_delete.append({"Key": str(k)})
                if len(to_delete) >= 1000:
                    client.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
                    to_delete = []

    if to_delete:
        client.delete_objects(Bucket=bucket, Delete={"Objects": to_delete})
        print(f"Cleaned up {BENCH_PREFIX} in {bucket}")


def size_label(size_mb: int) -> str:
    if size_mb >= 1024:
        return f"{size_mb // 1024}gb"
    return f"{size_mb}mb"


def download_csv_from_s3(client, bucket: str, key: str, local_path: Path) -> bool:
    """Download CSV from S3. Returns True if file existed."""
    try:
        client.download_file(bucket, key, str(local_path))
        print(f"Downloaded existing {key} from S3")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "404":
            print(f"No existing {key} in S3, starting fresh")
            return False
        raise


def upload_csv_to_s3(client, bucket: str, key: str, local_path: Path):
    """Upload CSV to S3."""
    client.upload_file(str(local_path), bucket, key)
    print(f"Uploaded {key} to S3")


def run_benchmark(client, bucket: str) -> list[dict]:
    transfer_config = TransferConfig(
        multipart_threshold=MULTIPART_THRESHOLD,
        multipart_chunksize=MULTIPART_CHUNKSIZE,
        max_concurrency=MAX_CONCURRENT_REQUESTS,
        use_threads=True,
    )

    # Clean up any leftover bench data from previous runs
    cleanup_bench_data(client, bucket)

    results = []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for size_mb in FILE_SIZES_MB:
        label = size_label(size_mb)

        # Generate test data once per size
        with tempfile.NamedTemporaryFile(delete=False) as f:
            remaining = size_mb * 1024 * 1024
            chunk = 1024 * 1024
            while remaining > 0:
                to_write = min(chunk, remaining)
                f.write(os.urandom(to_write))
                remaining -= to_write
            temp_path = f.name

        put_durations = []
        get_durations = []

        try:
            for i in range(ITERATIONS):
                key = f"{BENCH_PREFIX}{label}-{i}"

                # Measure upload
                try:
                    start = time.perf_counter()
                    client.upload_file(temp_path, bucket, key, Config=transfer_config)
                    duration = time.perf_counter() - start
                    put_durations.append(duration)
                    print(f"  put_{label} iter {i + 1}/{ITERATIONS}: {size_mb / duration:.2f} MB/s")
                except Exception as e:
                    print(f"  put_{label} iter {i + 1}/{ITERATIONS}: error: {e}")

                # Measure download
                dl_path = temp_path + f".dl{i}"
                try:
                    start = time.perf_counter()
                    client.download_file(bucket, key, dl_path, Config=transfer_config)
                    duration = time.perf_counter() - start
                    get_durations.append(duration)
                    print(f"  get_{label} iter {i + 1}/{ITERATIONS}: {size_mb / duration:.2f} MB/s")
                except Exception as e:
                    print(f"  get_{label} iter {i + 1}/{ITERATIONS}: error: {e}")
                finally:
                    if os.path.exists(dl_path):
                        os.unlink(dl_path)
        finally:
            os.unlink(temp_path)

        # Record averaged put result
        if put_durations:
            avg_duration = sum(put_durations) / len(put_durations)
            avg_throughput = size_mb / avg_duration
            results.append({
                "date": today,
                "scenario": f"put_{label}",
                "size_mb": size_mb,
                "duration_ms": int(avg_duration * 1000),
                "throughput_mbps": round(avg_throughput, 2),
                "iterations": len(put_durations),
                "status": "ok",
            })
        else:
            results.append({
                "date": today,
                "scenario": f"put_{label}",
                "size_mb": size_mb,
                "duration_ms": 0,
                "throughput_mbps": 0,
                "iterations": 0,
                "status": "error",
            })

        # Record averaged get result
        if get_durations:
            avg_duration = sum(get_durations) / len(get_durations)
            avg_throughput = size_mb / avg_duration
            results.append({
                "date": today,
                "scenario": f"get_{label}",
                "size_mb": size_mb,
                "duration_ms": int(avg_duration * 1000),
                "throughput_mbps": round(avg_throughput, 2),
                "iterations": len(get_durations),
                "status": "ok",
            })
        else:
            results.append({
                "date": today,
                "scenario": f"get_{label}",
                "size_mb": size_mb,
                "duration_ms": 0,
                "throughput_mbps": 0,
                "iterations": 0,
                "status": "error",
            })

    # Clean up bench data
    cleanup_bench_data(client, bucket)

    return results


def append_to_csv(results: list[dict], csv_path: Path):
    file_exists = csv_path.exists()
    with csv_path.open("a", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "date",
                "scenario",
                "size_mb",
                "duration_ms",
                "throughput_mbps",
                "iterations",
                "status",
            ],
        )
        if not file_exists:
            writer.writeheader()
        writer.writerows(results)


def main():
    endpoint = os.environ.get("HIPPIUS_ENDPOINT") or os.environ.get("S3_ENDPOINT_URL", "https://s3.hippius.com")
    access_key = os.environ.get("AWS_ACCESS_KEY") or os.environ.get("AWS_ACCESS_KEY_ID")
    secret_key = os.environ.get("AWS_SECRET_KEY") or os.environ.get("AWS_SECRET_ACCESS_KEY")

    if not access_key or not secret_key:
        print("Missing AWS_ACCESS_KEY/AWS_ACCESS_KEY_ID or AWS_SECRET_KEY/AWS_SECRET_ACCESS_KEY")
        sys.exit(1)

    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="decentralized",
        config=Config(s3={"addressing_style": "path"}),
    )

    # Ensure results bucket exists
    ensure_bucket_exists(client, RESULTS_BUCKET)

    # Download existing CSV from S3
    csv_path = Path(tempfile.gettempdir()) / "daily_bench.csv"
    download_csv_from_s3(client, RESULTS_BUCKET, CSV_KEY, csv_path)

    # Run benchmark using the same bucket
    print(f"\nRunning benchmark ({ITERATIONS} iterations per size)...")
    results = run_benchmark(client, RESULTS_BUCKET)

    # Print summary
    print(f"\n{'=' * 50}")
    print("RESULTS (averaged)")
    print(f"{'=' * 50}")
    for r in results:
        print(f"  {r['scenario']}: {r['throughput_mbps']} MB/s ({r['iterations']}/{ITERATIONS} iters, {r['status']})")

    # Append to CSV and upload
    append_to_csv(results, csv_path)
    upload_csv_to_s3(client, RESULTS_BUCKET, CSV_KEY, csv_path)
    print(f"Results uploaded to s3://{RESULTS_BUCKET}/{CSV_KEY}")


if __name__ == "__main__":
    main()
