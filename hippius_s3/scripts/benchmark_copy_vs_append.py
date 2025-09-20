#!/usr/bin/env python3
"""Benchmark script comparing AWS Multipart Upload vs Hippius S4 Append performance.

This script measures end-to-end latency and throughput for building objects via:
- AWS S3 Multipart Upload with upload_part operations (client sends each part)
- Hippius S4 Append operations (via S3-compatible API with append metadata)

Both modes send all data from client to server, providing fair client-upload parity.

Usage examples:
    # Basic AWS benchmark (50 MiB total, 5 MiB parts)
    python benchmark_copy_vs_append.py --mode aws --bucket test-bucket --size-mib 50 --part-mib 5

    # Hippius S4 benchmark (local gateway)
    python benchmark_copy_vs_append.py --mode hippius --bucket test-bucket --size-mib 50 --part-mib 5 \\
        --endpoint-url http://localhost:8000

    # With concurrency and CSV output
    python benchmark_copy_vs_append.py --mode hippius --bucket test-bucket --size-mib 100 --part-mib 10 \\
        --concurrency 4 --repeats 5 --csv results.csv

    # Keep objects for inspection
    python benchmark_copy_vs_append.py --mode aws --bucket test-bucket --size-mib 50 --part-mib 5 --keep
"""

import argparse
import contextlib
import csv
import json
import os
import sys
import time
import traceback
import uuid
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import ClientError


# Type alias for boto3 client
Boto3Client = Any


@dataclass
class BenchmarkResult:
    """Result of a single benchmark run."""

    mode: str
    total_bytes: int
    part_bytes: int
    parts: int
    concurrency: int
    repeat_idx: int
    t_init_ms: float
    t_complete_ms: float
    t_download_ms: float
    t_total_ms: float
    throughput_mib_s: float
    download_throughput_mib_s: float
    per_part_ms: List[float]
    client_bytes_sent: int = 0
    server_bytes_copied: int = 0

    def to_csv_row(self) -> Dict[str, Any]:
        """Convert to CSV row dictionary."""
        return {
            "mode": self.mode,
            "total_bytes": self.total_bytes,
            "part_bytes": self.part_bytes,
            "parts": self.parts,
            "concurrency": self.concurrency,
            "repeat_idx": self.repeat_idx,
            "t_init_ms": round(self.t_init_ms, 1),
            "t_complete_ms": round(self.t_complete_ms, 1),
            "t_download_ms": round(self.t_download_ms, 1),
            "t_total_ms": round(self.t_total_ms, 1),
            "throughput_mib_s": round(self.throughput_mib_s, 2),
            "download_throughput_mib_s": round(self.download_throughput_mib_s, 2),
            "per_part_ms_json": json.dumps([round(t, 1) for t in self.per_part_ms]),
            "client_bytes_sent": self.client_bytes_sent,
            "server_bytes_copied": self.server_bytes_copied,
        }

    def __str__(self) -> str:
        """Human-readable summary."""
        total_mib = self.total_bytes / (1024 * 1024)
        part_mib = self.part_bytes / (1024 * 1024)
        client_mib = self.client_bytes_sent / (1024 * 1024)
        return (
            f"{self.mode} | {total_mib:.0f} MiB | {part_mib:.0f} MiB parts | "
            f"client_sent={client_mib:.0f} MiB | conc={self.concurrency} | total={self.t_total_ms / 1000:.1f}s | "
            f"{self.throughput_mib_s:.1f} MiB/s "
            f"(init={self.t_init_ms / 1000:.1f}s, parts={sum(self.per_part_ms) / 1000:.1f}s, "
            f"complete={self.t_complete_ms / 1000:.1f}s, dl={self.t_download_ms / 1000:.1f}s @ {self.download_throughput_mib_s:.1f} MiB/s)"
        )


class BenchmarkRunner:
    """Base class for benchmark runners."""

    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.client = self._create_client()
        self.bucket = args.bucket
        self.object_prefix = args.object_prefix or f"benchmark-{uuid.uuid4()}"
        self.total_bytes = args.size_mib * 1024 * 1024
        self.part_bytes = args.part_mib * 1024 * 1024
        self.parts = max(1, self.total_bytes // self.part_bytes + (1 if self.total_bytes % self.part_bytes != 0 else 0))

        # Adjust last part size if needed
        if self.parts * self.part_bytes > self.total_bytes:
            # Last part will be smaller
            pass

        # Generate deterministic test data
        self.test_data = self._generate_test_data()

        # Ensure bucket exists
        self._ensure_bucket()

    # -------- logging helpers --------
    def _v(self) -> int:
        return int(getattr(self.args, "verbose", 0) or 0)

    def _log(self, level: int, msg: str) -> None:
        if self._v() >= level:
            print(msg)

    def _ts(self) -> str:
        # Timestamp helper for logs
        return time.strftime("%Y-%m-%d %H:%M:%S")

    def _create_client(self) -> Boto3Client:
        """Create boto3 client with appropriate configuration."""
        session_kwargs: Dict[str, Any] = {}
        if self.args.profile:
            # Profiles must be configured via Session
            session_kwargs["profile_name"] = self.args.profile

        session = boto3.session.Session(**session_kwargs)

        client_kwargs: Dict[str, Any] = {
            "region_name": self.args.region,
        }
        if self.args.endpoint_url:
            client_kwargs["endpoint_url"] = self.args.endpoint_url
        # For Hippius or any custom endpoint, prefer path-style addressing
        # to avoid virtual-hosted-style URLs like bucket.endpoint
        if self.args.mode == "hippius" or (self.args.endpoint_url and "hippius" in self.args.endpoint_url):
            client_kwargs["config"] = BotoConfig(s3={"addressing_style": "path"})

        # Apply timeouts and retries
        connect_to = getattr(self.args, "connect_timeout", 5)
        read_to = getattr(self.args, "read_timeout", 60)
        retries_max = getattr(self.args, "retries", 3)
        cfg = client_kwargs.get("config") or BotoConfig()
        cfg = BotoConfig(
            signature_version=getattr(cfg, "signature_version", None),
            s3=getattr(cfg, "s3", None),  # type: ignore
            retries={"max_attempts": retries_max, "mode": "standard"},
            connect_timeout=connect_to,
            read_timeout=read_to,
        )
        client_kwargs["config"] = cfg

        client = session.client("s3", **client_kwargs)
        self._log(
            2,
            f"[{self._ts()}] [client] endpoint={getattr(client._endpoint, 'host', 'unknown')} mode={self.args.mode} path_style={getattr(client.meta.config, 's3', {})}",
        )
        return client  # type: ignore

    def _generate_test_data(self) -> bytes:
        """Generate deterministic test data for validation."""
        # Create data that can be sliced into parts
        seed = b"benchmark_seed_12345"
        data = bytearray()
        while len(data) < self.total_bytes:
            data.extend(seed)
        return bytes(data[: self.total_bytes])

    def _ensure_bucket(self) -> None:
        """Ensure the bucket exists or create it (optionally with a unique suffix)."""
        try:
            self.client.head_bucket(Bucket=self.bucket)
            self._log(1, f"[{self._ts()}] [bucket] exists: {self.bucket}")
            return
        except ClientError as e:
            # Fall through and attempt creation on common not-found/forbidden cases
            err = str(e.response.get("Error", {}).get("Code", ""))
            self._log(1, f"[{self._ts()}] [bucket] head error code={err} for {self.bucket}")
            if err not in {"404", "NoSuchBucket", "NotFound", "403", "AccessDenied"}:
                raise

        # Try to create the desired bucket name first
        try:
            print(f"[{self._ts()}] Creating bucket: {self.bucket}")
            self.client.create_bucket(
                Bucket=self.bucket,
                CreateBucketConfiguration={"LocationConstraint": self.args.region}
                if self.args.region != "us-east-1"
                else {},
            )
            return
        except ClientError as ce:
            code = str(ce.response.get("Error", {}).get("Code", ""))
            self._log(1, f"[{self._ts()}] [bucket] create error code={code} for {self.bucket}")
            if code in {"BucketAlreadyOwnedByYou"}:
                return
            if code in {"BucketAlreadyExists", "AccessDenied"} and getattr(self.args, "auto_bucket", False):
                # Generate a unique bucket name with suffix
                unique_name = f"{self.bucket}-{uuid.uuid4().hex[:8]}"
                print(f"[{self._ts()}] Bucket name in use or forbidden; creating unique bucket: {unique_name}")
                self.client.create_bucket(
                    Bucket=unique_name,
                    CreateBucketConfiguration={"LocationConstraint": self.args.region}
                    if self.args.region != "us-east-1"
                    else {},
                )
                self.bucket = unique_name
                return
            raise

    def _get_part_data(self, part_idx: int) -> bytes:
        """Get data for a specific part."""
        start = part_idx * self.part_bytes
        end = min(start + self.part_bytes, self.total_bytes)
        return self.test_data[start:end]

    def _validate_object(self, key: str) -> None:
        """Validate the final object."""
        if not self.args.validate:
            return

        # Check size
        head = self.client.head_object(Bucket=self.bucket, Key=key)
        size = head["ContentLength"]
        if size != self.total_bytes:
            raise ValueError(f"Object size mismatch: expected {self.total_bytes}, got {size}")

        if self.args.validate_content:
            # Full content validation (expensive for large objects)
            obj = self.client.get_object(Bucket=self.bucket, Key=key)
            content = obj["Body"].read()
            if content != self.test_data:
                # Debug content mismatch
                content_len = len(content)
                expected_len = len(self.test_data)
                self._log(
                    1, f"[{self._ts()}] [validate] Content mismatch: got {content_len} bytes, expected {expected_len}"
                )

                # Check first and last few bytes
                if content_len > 0 and expected_len > 0:
                    self._log(
                        1,
                        f"[{self._ts()}] [validate] First 16 bytes - got: {content[:16].hex()}, expected: {self.test_data[:16].hex()}",
                    )
                    self._log(
                        1,
                        f"[{self._ts()}] [validate] Last 16 bytes - got: {content[-16:].hex()}, expected: {self.test_data[-16:].hex()}",
                    )

                # Check if content is just the base part repeated
                base_part = self.test_data[: self.part_bytes]
                if content == base_part * (len(content) // len(base_part)):
                    self._log(
                        1,
                        f"[{self._ts()}] [validate] Content appears to be base part repeated {len(content) // len(base_part)} times",
                    )

                raise ValueError("Object content mismatch")

    def _time_and_download(self, key: str) -> Tuple[float, float]:
        """Download the object, measure time, and assert content if validate_content."""
        td0 = time.time()

        # First check the object exists and get its size
        head = self.client.head_object(Bucket=self.bucket, Key=key)
        head_size = head.get("ContentLength", 0)
        self._log(2, f"[{self._ts()}] [download] HEAD size={head_size}, expected={self.total_bytes}")

        obj = self.client.get_object(Bucket=self.bucket, Key=key)
        content = obj["Body"].read()
        t_download_ms = (time.time() - td0) * 1000

        self._log(2, f"[{self._ts()}] [download] Downloaded {len(content)} bytes in {t_download_ms:.1f}ms")

        if len(content) != self.total_bytes:
            raise ValueError(f"Download size mismatch: expected {self.total_bytes}, got {len(content)}")

        if self.args.validate_content and content != self.test_data:
            raise ValueError("Downloaded content mismatch")

        download_throughput_mib_s = (self.total_bytes / (1024 * 1024)) / (t_download_ms / 1000)
        return t_download_ms, download_throughput_mib_s

    def _cleanup(self, keys: List[str]) -> None:
        """Clean up test objects."""
        if self.args.keep:
            return

        for key in keys:
            try:
                self.client.delete_object(Bucket=self.bucket, Key=key)
            except Exception as e:
                print(f"Warning: failed to delete {key}: {e}")

    def run_benchmark(self) -> BenchmarkResult:
        """Run the benchmark. Must be implemented by subclasses."""
        raise NotImplementedError


class AWSBenchmarkRunner(BenchmarkRunner):
    """Benchmark runner for AWS Multipart Upload using upload_part (client sends parts)."""

    def run_benchmark(self) -> BenchmarkResult:
        target_key = f"{self.object_prefix}-target"

        t_init_start = time.time()
        # Start MPU
        mpu = self.client.create_multipart_upload(Bucket=self.bucket, Key=target_key)
        upload_id = mpu["UploadId"]
        parts_info = []
        t_init_ms = (time.time() - t_init_start) * 1000

        per_part_ms = []
        client_sent = 0

        for i in range(self.parts):
            part_start = time.time()
            part_number = i + 1
            body = self._get_part_data(i)
            self._log(2, f"[{self._ts()}] [aws-mpu] upload_part {part_number} bytes={len(body)}")
            resp = self.client.upload_part(
                Bucket=self.bucket,
                Key=target_key,
                UploadId=upload_id,
                PartNumber=part_number,
                Body=body,
            )
            client_sent += len(body)
            per_part_ms.append((time.time() - part_start) * 1000)
            parts_info.append({"ETag": resp["ETag"], "PartNumber": part_number})

        # Complete
        t_complete_start = time.time()
        self.client.complete_multipart_upload(
            Bucket=self.bucket, Key=target_key, UploadId=upload_id, MultipartUpload={"Parts": parts_info}
        )
        t_complete_ms = (time.time() - t_complete_start) * 1000

        t_total_ms = (time.time() - t_init_start) * 1000
        throughput_mib_s = (self.total_bytes / (1024 * 1024)) / (t_total_ms / 1000)

        if getattr(self.args, "skip_download", False):
            t_download_ms = 0.0
            download_throughput_mib_s = 0.0
        else:
            t_download_ms, download_throughput_mib_s = self._time_and_download(target_key)

        self._validate_object(target_key)
        self._cleanup([target_key])

        return BenchmarkResult(
            mode="aws",
            total_bytes=self.total_bytes,
            part_bytes=self.part_bytes,
            parts=self.parts,
            concurrency=1,
            repeat_idx=0,
            t_init_ms=t_init_ms,
            t_complete_ms=t_complete_ms,
            t_total_ms=t_total_ms,
            throughput_mib_s=throughput_mib_s,
            t_download_ms=t_download_ms,
            download_throughput_mib_s=download_throughput_mib_s,
            per_part_ms=per_part_ms,
            client_bytes_sent=client_sent,
            server_bytes_copied=0,
        )


class HippiusBenchmarkRunner(BenchmarkRunner):
    """Benchmark runner for Hippius S4 Append."""

    def run_benchmark(self) -> BenchmarkResult:
        """Run Hippius S4 Append benchmark."""
        target_key = f"{self.object_prefix}-target"

        # Initial put (base object)
        t_init_start = time.time()
        base_data = self._get_part_data(0)
        t0 = time.time()
        self._log(1, f"[{self._ts()}] [hippius] PUT base: key={target_key} bytes={len(base_data)}")
        put_resp = self.client.put_object(Bucket=self.bucket, Key=target_key, Body=base_data)
        self._log(1, f"[{self._ts()}] [hippius] PUT base done in {int((time.time() - t0) * 1000)} ms")

        # Read initial version directly from PUT response headers
        current_version = (
            put_resp.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("x-amz-meta-append-version", "0")
        )
        self._log(1, f"[{self._ts()}] [hippius] current append-version={current_version}")

        t_init_ms = (time.time() - t_init_start) * 1000

        # Append remaining parts
        per_part_ms = []
        client_sent = len(base_data)

        for i in range(1, self.parts):
            part_start = time.time()
            part_data = self._get_part_data(i)

            # Append with proper metadata
            metadata = {"append": "true", "append-if-version": current_version, "append-id": str(uuid.uuid4())}
            self._log(
                1, f"[{self._ts()}] [hippius] PUT append: part={i} bytes={len(part_data)} version={current_version}"
            )
            # Retry on version precondition failures
            max_retries: int = getattr(self.args, "append_retries", 5)
            retry_wait: float = getattr(self.args, "append_retry_wait", 0.5)
            attempt = 0
            while True:
                attempt += 1
                try:
                    pa0 = time.time()
                    resp = self.client.put_object(
                        Bucket=self.bucket,
                        Key=target_key,
                        Body=part_data,
                        Metadata=metadata,
                    )
                    self._log(
                        1, f"[{self._ts()}] [hippius] PUT append part={i} ok in {int((time.time() - pa0) * 1000)} ms"
                    )
                    # Update version from server response header to avoid HEAD
                    next_version = (
                        resp.get("ResponseMetadata", {}).get("HTTPHeaders", {}).get("x-amz-meta-append-version")
                    )
                    current_version = str(next_version) if next_version is not None else str(int(current_version) + 1)
                    break
                except ClientError as e:
                    code = str(e.response.get("Error", {}).get("Code"))
                    msg = str(e.response.get("Error", {}).get("Message"))
                    self._log(
                        1,
                        f"[{self._ts()}] [hippius] append failed (attempt {attempt}/{max_retries}) code={code} msg={msg}",
                    )
                    if self._v() >= 3:
                        traceback.print_exc()

                    # On version mismatch, use server-provided current version and optional Retry-After; avoid HEAD
                    if code in {"PreconditionFailed", "412"}:
                        hdrs = e.response.get("ResponseMetadata", {}).get("HTTPHeaders", {})
                        next_version = hdrs.get("x-amz-meta-append-version")
                        retry_after = hdrs.get("retry-after")
                        if next_version is not None:
                            current_version = str(next_version)
                            metadata["append-if-version"] = str(current_version)
                            self._log(
                                1, f"[{self._ts()}] [hippius] updated append-version from server: {current_version}"
                            )
                        if attempt < max_retries:
                            sleep_s = retry_wait
                            if retry_after and str(retry_after).replace(".", "", 1).isdigit():
                                with contextlib.suppress(Exception):
                                    sleep_s = float(retry_after)
                            time.sleep(sleep_s)
                            continue

                    raise
            # Version already updated from PUT response; no HEAD needed
            self._log(1, f"[{self._ts()}] [hippius] new append-version={current_version}")

            client_sent += len(part_data)
            part_ms = (time.time() - part_start) * 1000
            per_part_ms.append(part_ms)

        # No separate complete step for append
        t_complete_ms = 0.0

        t_total_ms = (time.time() - t_init_start) * 1000
        throughput_mib_s = (self.total_bytes / (1024 * 1024)) / (t_total_ms / 1000)

        # Validate (with small delay for eventual consistency)
        if self.args.post_append_sleep > 0:
            time.sleep(self.args.post_append_sleep)
        self._validate_object(target_key)

        # Download and validate content (and time it)
        if getattr(self.args, "skip_download", False):
            t_download_ms = 0.0
            download_throughput_mib_s = 0.0
        else:
            t_download_ms, download_throughput_mib_s = self._time_and_download(target_key)

        # Cleanup
        self._cleanup([target_key])

        return BenchmarkResult(
            mode="hippius",
            total_bytes=self.total_bytes,
            part_bytes=self.part_bytes,
            parts=self.parts,
            concurrency=1,  # Append is sequential by design
            repeat_idx=0,  # Set by caller
            t_init_ms=t_init_ms,
            t_complete_ms=t_complete_ms,
            t_total_ms=t_total_ms,
            throughput_mib_s=throughput_mib_s,
            t_download_ms=t_download_ms,
            download_throughput_mib_s=download_throughput_mib_s,
            per_part_ms=per_part_ms,
            client_bytes_sent=client_sent,
            server_bytes_copied=0,
        )


def run_single_benchmark(args: argparse.Namespace, repeat_idx: int) -> BenchmarkResult:
    """Run a single benchmark iteration."""
    runner: BenchmarkRunner
    if args.mode == "aws":
        runner = AWSBenchmarkRunner(args)
    elif args.mode == "hippius":
        runner = HippiusBenchmarkRunner(args)
    else:
        raise ValueError(f"Unknown mode: {args.mode}")

    result = runner.run_benchmark()
    result.repeat_idx = repeat_idx
    return result


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark AWS UploadPartCopy vs Hippius S4 Append performance",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument("--mode", required=True, choices=["aws", "hippius"], help="Benchmark mode")
    parser.add_argument("--bucket", required=True, help="Target bucket name")
    parser.add_argument("--size-mib", type=int, default=50, help="Total object size in MiB (default: 50)")
    parser.add_argument("--part-mib", type=int, default=5, help="Part size in MiB (default: 5)")

    # Connection options
    parser.add_argument("--endpoint-url", help="S3 endpoint URL (for local/Hippius)")
    parser.add_argument(
        "--region",
        default=os.environ.get("AWS_REGION", "us-east-1"),
        help="AWS region (default: us-east-1 or AWS_REGION)",
    )
    parser.add_argument("--profile", help="AWS profile name (uses boto3 Session)")

    # Benchmark options
    parser.add_argument("--concurrency", type=int, default=1, help="Number of concurrent operations (default: 1)")
    parser.add_argument("--repeats", type=int, default=3, help="Number of benchmark repeats for averaging (default: 3)")

    # Output options
    parser.add_argument("--csv", help="CSV output file path")
    parser.add_argument("--object-prefix", help="Object key prefix (default: auto-generated)")
    parser.add_argument("--keep", action="store_true", help="Keep test objects (default: delete them)")
    parser.add_argument(
        "--auto-bucket", action="store_true", help="Auto-create a unique bucket name if target is not accessible"
    )
    parser.add_argument("--validate", action="store_true", default=True, help="Validate final objects (default: True)")
    parser.add_argument(
        "--validate-content", action="store_true", help="Validate full content (expensive, default: size only)"
    )
    parser.add_argument("-v", "--verbose", action="count", default=0, help="Increase verbosity (-v, -vv, -vvv)")
    parser.add_argument("--connect-timeout", type=int, default=5, help="HTTP connect timeout seconds (default: 5)")
    parser.add_argument("--read-timeout", type=int, default=60, help="HTTP read timeout seconds (default: 60)")
    parser.add_argument("--retries", type=int, default=3, help="HTTP retry max attempts (default: 3)")
    parser.add_argument(
        "--append-retries", type=int, default=5, help="Retries for append PreconditionFailed (default: 5)"
    )
    parser.add_argument(
        "--append-retry-wait", type=float, default=0.5, help="Sleep between append retries seconds (default: 0.5)"
    )

    # Optional download step
    parser.add_argument(
        "--skip-download", action="store_true", help="Skip timed GET/download and content verification step"
    )
    parser.add_argument(
        "--post-append-sleep",
        type=float,
        default=0,
        help="Sleep seconds before Hippius validation (set 0 for symmetry with AWS)",
    )

    args = parser.parse_args()

    # Validate arguments
    if args.mode == "aws" and args.size_mib > args.part_mib and args.part_mib < 5:
        raise ValueError("AWS requires parts >= 5 MiB (except last).")
    if args.mode == "hippius" and not args.endpoint_url:
        print("Warning: --endpoint-url recommended for Hippius mode")

    # Run benchmarks
    results = []
    for repeat_idx in range(args.repeats):
        print(f"\nRunning {args.mode} benchmark (repeat {repeat_idx + 1}/{args.repeats})...")
        try:
            result = run_single_benchmark(args, repeat_idx)
            results.append(result)
            print(f"  {result}")
        except Exception as e:
            print(f"  Error in repeat {repeat_idx + 1}: {e}")
            if not args.keep:
                # Try to clean up any partial objects
                pass

    if not results:
        print("No successful runs")
        sys.exit(1)

    # Summary
    print("\n=== Summary ===")
    total_runs = len(results)
    avg_total = sum(r.t_total_ms for r in results) / total_runs
    avg_throughput = sum(r.throughput_mib_s for r in results) / total_runs

    print(f"Average total time: {avg_total / 1000:.1f}s")
    print(f"Average throughput: {avg_throughput:.1f} MiB/s")

    # CSV output
    if args.csv:
        with open(args.csv, "w", newline="") as f:  # noqa: PTH123  # Script, not library
            writer = csv.DictWriter(f, fieldnames=results[0].to_csv_row().keys())
            writer.writeheader()
            for result in results:
                writer.writerow(result.to_csv_row())
        print(f"Results written to {args.csv}")


if __name__ == "__main__":
    main()
