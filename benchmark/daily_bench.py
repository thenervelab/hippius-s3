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
from botocore.config import Config
from botocore.exceptions import ClientError

SCENARIOS = [
    ("put_1mb", 1),
    ("get_1mb", 1),
    ("put_100mb", 100),
    ("get_100mb", 100),
]

RESULTS_BUCKET = "hippius-benchmarks"
CSV_KEY = "daily.csv"


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


def run_benchmark(endpoint: str, access_key: str, secret_key: str) -> list[dict]:
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name="decentralized",
        config=Config(s3={"addressing_style": "path"}),
    )

    bucket = f"daily-bench-{int(time.time())}"
    client.create_bucket(Bucket=bucket)

    results = []
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    try:
        for scenario, size_mb in SCENARIOS:
            op = "put" if scenario.startswith("put") else "get"

            with tempfile.NamedTemporaryFile(delete=False) as f:
                data = os.urandom(size_mb * 1024 * 1024)
                f.write(data)
                temp_path = f.name

            key = f"bench-{scenario}-{int(time.time())}"

            try:
                if op == "put":
                    start = time.perf_counter()
                    client.upload_file(temp_path, bucket, key)
                    duration = time.perf_counter() - start
                else:
                    # First upload, then measure download
                    client.upload_file(temp_path, bucket, key)
                    dl_path = temp_path + ".dl"
                    start = time.perf_counter()
                    client.download_file(bucket, key, dl_path)
                    duration = time.perf_counter() - start
                    os.unlink(dl_path)

                throughput = size_mb / duration if duration > 0 else 0
                status = "ok"
            except Exception as e:
                duration = 0
                throughput = 0
                status = f"error: {e}"

            results.append({
                "date": today,
                "scenario": scenario,
                "size_mb": size_mb,
                "duration_ms": int(duration * 1000),
                "throughput_mbps": round(throughput, 2),
                "status": "ok" if status == "ok" else "error",
            })

            os.unlink(temp_path)
            # Cleanup key
            try:
                client.delete_object(Bucket=bucket, Key=key)
            except Exception:
                pass
    finally:
        # Cleanup bucket
        try:
            client.delete_bucket(Bucket=bucket)
        except Exception:
            pass

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

    # Create S3 client for results storage
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
    csv_existed = download_csv_from_s3(client, RESULTS_BUCKET, CSV_KEY, csv_path)

    # Run benchmark
    results = run_benchmark(endpoint, access_key, secret_key)

    # Print results
    for r in results:
        print(f"{r['scenario']}: {r['throughput_mbps']} MB/s ({r['status']})")

    # Append to CSV
    append_to_csv(results, csv_path)

    # Upload CSV back to S3
    upload_csv_to_s3(client, RESULTS_BUCKET, CSV_KEY, csv_path)
    print(f"Results uploaded to s3://{RESULTS_BUCKET}/{CSV_KEY}")


if __name__ == "__main__":
    main()
