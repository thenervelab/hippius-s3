#!/usr/bin/env python3
import argparse
import csv
import hashlib
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple

from dotenv import load_dotenv


load_dotenv()

TEST_FILES = {
    "5mb.bin": 5,
    "50mb.bin": 50,
    "500mb.bin": 500,
    "2gb.bin": 2048,
    "5gb.bin": 5120,
}

MULTIPART_THRESHOLD = 64 * 1024 * 1024
MULTIPART_CHUNKSIZE = 64 * 1024 * 1024
MAX_CONCURRENT_REQUESTS = 10


def generate_test_file(path: Path, size_mb: int) -> None:
    if path.exists():
        current_size_mb = path.stat().st_size / (1024 * 1024)
        if abs(current_size_mb - size_mb) < 0.1:
            print(f"  {path.name} already exists ({size_mb}MB)")
            return

    print(f"  Generating {path.name} ({size_mb}MB)...")
    chunk_size = 1024 * 1024
    total_bytes = size_mb * chunk_size
    written = 0

    with path.open("wb") as f:
        while written < total_bytes:
            to_write = min(chunk_size, total_bytes - written)
            f.write(os.urandom(to_write))
            written += to_write


def compute_md5(file_path: Path) -> str:
    md5 = hashlib.md5()
    with file_path.open("rb") as f:
        while chunk := f.read(8192):
            md5.update(chunk)
    return md5.hexdigest()


def sanitize_endpoint_name(endpoint: str) -> str:
    from urllib.parse import urlparse
    parsed = urlparse(endpoint)
    hostname = parsed.netloc or parsed.path
    return hostname.replace(":", "-").replace("/", "-")


def get_endpoint_config(endpoint_name: Optional[str], custom_endpoint: Optional[str]) -> Tuple[str, str, str, str]:
    if custom_endpoint:
        access_key = os.environ.get("AWS_ACCESS_KEY_ID") or os.environ.get("AWS_ACCESS_KEY")
        secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY") or os.environ.get("AWS_ACCESS_SECRET")
        region = os.environ.get("AWS_REGION", "us-east-1")

        if not access_key or not secret_key:
            print("Error: AWS credentials not found in environment variables")
            print("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
            sys.exit(1)

        return custom_endpoint, region, access_key, secret_key

    if endpoint_name == "hippius":
        endpoint = os.environ.get("HIPPIUS_ENDPOINT")
        access_key = os.environ.get("HIPPIUS_ACCESS_KEY")
        secret_key = os.environ.get("HIPPIUS_SECRET_KEY")
        region = "decentralized"

        if not endpoint or not access_key or not secret_key:
            print("Error: Hippius credentials not found in .env file")
            print("Please ensure HIPPIUS_ENDPOINT, HIPPIUS_ACCESS_KEY, and HIPPIUS_SECRET_KEY are set")
            sys.exit(1)

        return endpoint, region, access_key, secret_key

    if endpoint_name == "r2":
        endpoint = os.environ.get("R2_ENDPOINT")
        access_key = os.environ.get("R2_ACCESS_KEY")
        secret_key = os.environ.get("R2_SECRET_KEY")
        region = "auto"

        if not endpoint or not access_key or not secret_key:
            print("Error: R2 credentials not found in .env file")
            print("Please ensure R2_ENDPOINT, R2_ACCESS_KEY, and R2_SECRET_KEY are set")
            sys.exit(1)

        return endpoint, region, access_key, secret_key

    print("Error: No endpoint specified. Use --hippius, --r2, or --endpoint")
    sys.exit(1)


def setup_aws_cli_config(endpoint: str, region: str, access_key: str, secret_key: str) -> Dict[str, str]:
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = access_key
    env["AWS_SECRET_ACCESS_KEY"] = secret_key
    env["AWS_DEFAULT_REGION"] = region

    subprocess.run(
        ["aws", "configure", "set", "default.s3.multipart_threshold", str(MULTIPART_THRESHOLD)],
        env=env,
        capture_output=True,
    )
    subprocess.run(
        ["aws", "configure", "set", "default.s3.multipart_chunksize", str(MULTIPART_CHUNKSIZE)],
        env=env,
        capture_output=True,
    )
    subprocess.run(
        ["aws", "configure", "set", "default.s3.max_concurrent_requests", str(MAX_CONCURRENT_REQUESTS)],
        env=env,
        capture_output=True,
    )

    return env


def create_bucket(bucket: str, endpoint: str, env: Dict[str, str]) -> bool:
    print(f"\nCreating bucket: {bucket}")

    cmd = ["aws", "s3api", "create-bucket", "--bucket", bucket, "--endpoint-url", endpoint]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    if result.returncode == 0:
        print(f"  Bucket created")
        return True

    print(f"  Error creating bucket: {result.stderr}")
    return False


def upload_file(file_path: Path, bucket: str, key: str, endpoint: str, env: Dict[str, str]) -> Tuple[float, float]:
    size_mb = file_path.stat().st_size / (1024 * 1024)

    print(f"\nUploading {key} ({size_mb:.2f}MB)...")

    cmd = ["aws", "s3", "cp", str(file_path), f"s3://{bucket}/{key}", "--endpoint-url", endpoint]

    start_time = time.perf_counter()
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    end_time = time.perf_counter()

    if result.returncode != 0:
        print(f"  Error uploading: {result.stderr}")
        return 0, 0

    upload_time = end_time - start_time
    upload_speed = size_mb / upload_time if upload_time > 0 else 0

    print(f"  Upload time: {upload_time:.2f}s")
    print(f"  Upload speed: {upload_speed:.2f} MB/s")

    return upload_time, upload_speed


def download_file(bucket: str, key: str, download_path: Path, endpoint: str, env: Dict[str, str]) -> Tuple[float, float]:
    print(f"\nDownloading {key}...")

    cmd = ["aws", "s3", "cp", f"s3://{bucket}/{key}", str(download_path), "--endpoint-url", endpoint]

    start_time = time.perf_counter()
    result = subprocess.run(cmd, env=env, capture_output=True, text=True)
    end_time = time.perf_counter()

    if result.returncode != 0:
        print(f"  Error downloading: {result.stderr}")
        return 0, 0

    download_time = end_time - start_time
    size_mb = download_path.stat().st_size / (1024 * 1024)
    download_speed = size_mb / download_time if download_time > 0 else 0

    print(f"  Download time: {download_time:.2f}s")
    print(f"  Download speed: {download_speed:.2f} MB/s")

    return download_time, download_speed


def delete_bucket(bucket: str, endpoint: str, env: Dict[str, str]) -> None:
    print(f"\nCleaning up bucket {bucket}...")

    cmd = ["aws", "s3", "rb", f"s3://{bucket}", "--force", "--endpoint-url", endpoint]

    result = subprocess.run(cmd, env=env, capture_output=True, text=True)

    if result.returncode == 0:
        print("  Bucket deleted")
    else:
        print(f"  Error deleting bucket: {result.stderr}")


def ensure_test_files(test_files_dir: Path, regenerate: bool) -> None:
    test_files_dir.mkdir(parents=True, exist_ok=True)

    print("Checking test files...")
    for filename, size_mb in TEST_FILES.items():
        file_path = test_files_dir / filename
        if regenerate or not file_path.exists():
            generate_test_file(file_path, size_mb)


def compute_hashes(test_files_dir: Path, files: List[str]) -> Dict[str, str]:
    print("\nComputing MD5 hashes for test files...")
    hashes = {}
    for filename in files:
        file_path = test_files_dir / filename
        hashes[filename] = compute_md5(file_path)
        print(f"  {filename}: {hashes[filename]}")
    return hashes


def run_benchmark(
    bucket: str,
    endpoint: str,
    env: Dict[str, str],
    test_files_dir: Path,
    download_dir: Path,
    verify: bool,
) -> List[Dict[str, any]]:
    results = []
    test_file_list = list(TEST_FILES.keys())

    original_hashes = {}
    if verify:
        original_hashes = compute_hashes(test_files_dir, test_file_list)

    print(f"\n{'='*80}")
    print("UPLOAD PHASE")
    print(f"{'='*80}")

    for filename in test_file_list:
        file_path = test_files_dir / filename
        size_mb = file_path.stat().st_size / (1024 * 1024)

        upload_time, upload_speed = upload_file(file_path, bucket, filename, endpoint, env)

        results.append({
            "file_name": filename,
            "file_size_mb": f"{size_mb:.2f}",
            "upload_time_s": f"{upload_time:.2f}" if upload_time > 0 else "ERROR",
            "upload_speed_mbps": f"{upload_speed:.2f}" if upload_speed > 0 else "ERROR",
            "download_time_s": "",
            "download_speed_mbps": "",
            "hash_match": "",
            "status": "uploaded" if upload_time > 0 else "upload_failed",
        })

    print(f"\n{'='*80}")
    print("DOWNLOAD PHASE")
    print(f"{'='*80}")

    for idx, filename in enumerate(test_file_list):
        if results[idx]["status"] == "upload_failed":
            print(f"\nSkipping {filename} (upload failed)")
            results[idx]["download_time_s"] = "SKIPPED"
            results[idx]["download_speed_mbps"] = "SKIPPED"
            results[idx]["hash_match"] = "skipped"
            continue

        download_path = download_dir / filename

        download_time, download_speed = download_file(bucket, filename, download_path, endpoint, env)

        hash_match = "skipped"
        if verify and download_time > 0:
            print(f"  Verifying integrity...")
            downloaded_hash = compute_md5(download_path)
            hash_match = "true" if downloaded_hash == original_hashes[filename] else "false"
            if hash_match == "false":
                print(f"  WARNING: Hash mismatch!")
                print(f"    Original: {original_hashes[filename]}")
                print(f"    Downloaded: {downloaded_hash}")

        results[idx]["download_time_s"] = f"{download_time:.2f}" if download_time > 0 else "ERROR"
        results[idx]["download_speed_mbps"] = f"{download_speed:.2f}" if download_speed > 0 else "ERROR"
        results[idx]["hash_match"] = hash_match
        results[idx]["status"] = "ok" if download_time > 0 else "download_failed"

    return results


def save_results(results: List[Dict[str, any]], csv_path: Path) -> None:
    print(f"\nSaving results to {csv_path}...")

    with csv_path.open("w", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "file_name",
                "file_size_mb",
                "upload_time_s",
                "upload_speed_mbps",
                "download_time_s",
                "download_speed_mbps",
                "hash_match",
                "status",
            ],
        )
        writer.writeheader()
        writer.writerows(results)

    print(f"  Results saved to {csv_path}")


def print_summary(results: List[Dict[str, any]]) -> None:
    print(f"\n{'='*80}")
    print("SUMMARY")
    print(f"{'='*80}")

    total_upload_time = 0
    total_download_time = 0
    total_size_mb = 0

    for r in results:
        if r["upload_time_s"] != "ERROR" and r["upload_time_s"] != "SKIPPED":
            total_upload_time += float(r["upload_time_s"])
        if r["download_time_s"] != "ERROR" and r["download_time_s"] != "SKIPPED" and r["download_time_s"]:
            total_download_time += float(r["download_time_s"])
        total_size_mb += float(r["file_size_mb"])

    avg_upload_speed = total_size_mb / total_upload_time if total_upload_time > 0 else 0
    avg_download_speed = total_size_mb / total_download_time if total_download_time > 0 else 0

    print(f"Total data transferred: {total_size_mb:.2f} MB")
    print(f"Total upload time: {total_upload_time:.2f}s")
    print(f"Total download time: {total_download_time:.2f}s")
    print(f"Average upload speed: {avg_upload_speed:.2f} MB/s")
    print(f"Average download speed: {avg_download_speed:.2f} MB/s")

    print(f"\nAWS CLI Configuration:")
    print(f"  Multipart threshold: {MULTIPART_THRESHOLD / (1024 * 1024):.0f} MB")
    print(f"  Multipart chunk size: {MULTIPART_CHUNKSIZE / (1024 * 1024):.0f} MB")
    print(f"  Max concurrent requests: {MAX_CONCURRENT_REQUESTS}")

    failed = [r for r in results if r["status"] != "ok"]
    if failed:
        print(f"\nFailed transfers: {len(failed)}")
        for r in failed:
            print(f"  - {r['file_name']}: {r['status']}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark S3-compatible endpoint upload/download performance using AWS CLI"
    )

    endpoint_group = parser.add_mutually_exclusive_group()
    endpoint_group.add_argument(
        "--hippius",
        action="store_true",
        help="Use Hippius S3 endpoint (from .env)",
    )
    endpoint_group.add_argument(
        "--r2",
        action="store_true",
        help="Use Cloudflare R2 endpoint (from .env)",
    )
    endpoint_group.add_argument(
        "--endpoint",
        help="Custom S3 endpoint URL (requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY)",
    )

    parser.add_argument(
        "--keep-bucket",
        action="store_true",
        help="Keep S3 bucket after benchmark (default: auto-delete)",
    )
    parser.add_argument(
        "--no-verify",
        action="store_true",
        help="Skip MD5 hash verification (default: verify)",
    )
    parser.add_argument(
        "--regenerate",
        action="store_true",
        help="Force regeneration of test files even if they exist",
    )

    args = parser.parse_args()

    endpoint_name = None
    if args.hippius:
        endpoint_name = "hippius"
    elif args.r2:
        endpoint_name = "r2"

    if not endpoint_name and not args.endpoint:
        parser.error("Must specify one of: --hippius, --r2, or --endpoint")

    endpoint, region, access_key, secret_key = get_endpoint_config(endpoint_name, args.endpoint)

    script_dir = Path(__file__).parent
    test_files_dir = script_dir / "test_files"

    print("="*80)
    print("S3 Endpoint Benchmark (AWS CLI with Multipart)")
    print("="*80)
    print(f"Endpoint: {endpoint}")
    print(f"Region: {region}")
    print(f"Verify hashes: {not args.no_verify}")
    print(f"Keep bucket: {args.keep_bucket}")
    print(f"Multipart threshold: {MULTIPART_THRESHOLD / (1024 * 1024):.0f}MB")
    print(f"Multipart chunk size: {MULTIPART_CHUNKSIZE / (1024 * 1024):.0f}MB")
    print(f"Max concurrent requests: {MAX_CONCURRENT_REQUESTS}")

    ensure_test_files(test_files_dir, args.regenerate)

    env = setup_aws_cli_config(endpoint, region, access_key, secret_key)

    timestamp = int(time.time())
    bucket = f"speedtest-{timestamp}"

    if not create_bucket(bucket, endpoint, env):
        print("Failed to create bucket, exiting")
        sys.exit(1)

    with tempfile.TemporaryDirectory() as temp_dir:
        download_dir = Path(temp_dir)

        results = run_benchmark(
            bucket,
            endpoint,
            env,
            test_files_dir,
            download_dir,
            verify=not args.no_verify,
        )

        print_summary(results)

        sanitized_endpoint = sanitize_endpoint_name(endpoint)
        csv_filename = f"{sanitized_endpoint}-benchmark-{timestamp}.csv"
        csv_path = script_dir / csv_filename

        save_results(results, csv_path)

    if not args.keep_bucket:
        delete_bucket(bucket, endpoint, env)
    else:
        print(f"\nBucket {bucket} was kept (use without --keep-bucket to auto-delete)")

    print("\nBenchmark completed successfully!")


if __name__ == "__main__":
    main()
