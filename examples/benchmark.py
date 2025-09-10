import base64
import csv
import json
import logging
import os
import subprocess
import tempfile
import time
from pathlib import Path

import dotenv


# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

dotenv.load_dotenv()

# S3 Configuration
s3_seed_phrase = os.environ["HIPPIUS_SEED_PHRASE"]
s3_access_key = base64.b64encode(s3_seed_phrase.encode("utf-8")).decode("utf-8")

# R2 Configuration
r2_access_key = os.environ["R2_ACCESS_KEY"]
r2_secret_key = os.environ["R2_SECRET_KEY"]
r2_endpoint = os.environ["R2_ENDPOINT"]

bucket_name = f"speed-test-{int(time.time())}"

# AWS CLI transfer configuration
CHUNK_SIZE_128MB = 128 * 1024 * 1024  # 134217728 bytes
AWS_CLI_CONFIG = {
    "multipart_threshold": CHUNK_SIZE_128MB,
    "multipart_chunksize": CHUNK_SIZE_128MB,
    "max_concurrent_requests": 10,
    "max_bandwidth": None,  # No bandwidth limit
}

# Test files
files = [
    "test_data/5mb.bin",
    "test_data/100mb.bin",
    "test_data/250mb.bin",
    "test_data/500mb.bin",
    "test_data/2.5gb.bin",
    "test_data/5gb.bin",
]

on_production = True
endpoint_url = "https://s3.hippius.com" if on_production else "http://localhost:8000"
use_ssl = on_production


def get_file_size(file_path):
    """Get file size in bytes."""
    return os.path.getsize(file_path)


def format_speed(bytes_per_second):
    """Format speed in human readable format."""
    if bytes_per_second > 1024 * 1024:
        return f"{bytes_per_second / (1024 * 1024):.2f} MB/s"
    if bytes_per_second > 1024:
        return f"{bytes_per_second / 1024:.2f} KB/s"
    return f"{bytes_per_second:.2f} B/s"


def format_size(bytes_size):
    """Format file size in human readable format."""
    if bytes_size > 1024 * 1024:
        return f"{bytes_size / (1024 * 1024):.2f} MB"
    if bytes_size > 1024:
        return f"{bytes_size / 1024:.2f} KB"
    return f"{bytes_size} B"


def setup_aws_cli_config(endpoint_url, access_key, secret_key, region="decentralized"):
    """Configure AWS CLI environment variables and config."""
    env = os.environ.copy()
    env["AWS_ACCESS_KEY_ID"] = access_key
    env["AWS_SECRET_ACCESS_KEY"] = secret_key
    env["AWS_DEFAULT_REGION"] = region

    # Configure AWS CLI settings for performance
    aws_config_commands = [
        ["aws", "configure", "set", "default.s3.multipart_threshold", str(CHUNK_SIZE_128MB)],
        ["aws", "configure", "set", "default.s3.multipart_chunksize", str(CHUNK_SIZE_128MB)],
        ["aws", "configure", "set", "default.s3.max_concurrent_requests", "10"],
        ["aws", "configure", "set", "default.s3.use_accelerate_endpoint", "false"],
    ]

    for cmd in aws_config_commands:
        try:
            subprocess.run(cmd, env=env, capture_output=True, check=True)
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to set AWS config {' '.join(cmd[3:])}: {e}")

    return env


def parse_aws_cli_speeds(output_text):
    """Parse AWS CLI output to extract transfer speeds."""
    import re

    speeds = []

    # Look for patterns like "(12.2 MiB/s)" or "(1.5 GB/s)"
    speed_pattern = r"\(([0-9.]+)\s*(B/s|KB/s|KiB/s|MB/s|MiB/s|GB/s|GiB/s)\)"
    matches = re.findall(speed_pattern, output_text)

    for speed_val, unit in matches:
        speed = float(speed_val)
        # Convert to bytes per second
        if unit == "KB/s":
            speed *= 1000
        elif unit == "KiB/s":
            speed *= 1024
        elif unit == "MB/s":
            speed *= 1000 * 1000
        elif unit == "MiB/s":
            speed *= 1024 * 1024
        elif unit == "GB/s":
            speed *= 1000 * 1000 * 1000
        elif unit == "GiB/s":
            speed *= 1024 * 1024 * 1024
        speeds.append(speed)

    return speeds


def upload_file_aws_cli(file_path, bucket, endpoint_url, env):
    """Upload a file using AWS CLI and return upload stats."""
    if not Path(file_path).exists():
        logger.warning(f"File {file_path} not found, skipping upload")
        return None, None, None, None

    file_size = get_file_size(file_path)
    logger.info(f"Starting upload of {file_path} ({format_size(file_size)}) to bucket {bucket}")

    start_time = time.time()

    cmd = ["aws", "s3", "cp", file_path, f"s3://{bucket}/{file_path}", "--endpoint-url", endpoint_url]

    try:
        # Use Popen to capture stderr in real-time (where progress is shown)
        process = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        stdout, stderr = process.communicate(timeout=1200)

        end_time = time.time()
        upload_time = end_time - start_time
        avg_upload_speed = file_size / upload_time if upload_time > 0 else 0

        if process.returncode == 0:
            # Parse speeds from stderr (where AWS CLI shows progress)
            speeds = parse_aws_cli_speeds(stderr)
            min_speed = min(speeds) if speeds else None
            max_speed = max(speeds) if speeds else None

            logger.info(f"Upload of {file_path} completed in {upload_time:.2f}s at {format_speed(avg_upload_speed)}")
            if min_speed and max_speed:
                logger.info(f"  Speed range: {format_speed(min_speed)} - {format_speed(max_speed)}")

            return upload_time, avg_upload_speed, min_speed, max_speed
        logger.error(f"AWS CLI upload failed for {file_path}: {stderr}")
        return None, None, None, None

    except subprocess.TimeoutExpired:
        logger.error(f"AWS CLI upload timeout for {file_path}")
        return None, None, None, None
    except Exception as e:
        logger.error(f"AWS CLI upload error for {file_path}: {e}")
        return None, None, None, None


def download_file_aws_cli(file_name, bucket, endpoint_url, env, download_dir):
    """Download a file using AWS CLI and return download stats."""
    download_path = download_dir / file_name
    download_path.parent.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting download of {file_name} from bucket {bucket}")

    start_time = time.time()

    cmd = ["aws", "s3", "cp", f"s3://{bucket}/{file_name}", str(download_path), "--endpoint-url", endpoint_url]

    try:
        # Use Popen to capture stderr in real-time (where progress is shown)
        process = subprocess.Popen(cmd, env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)

        stdout, stderr = process.communicate(timeout=1200)

        end_time = time.time()
        download_time = end_time - start_time

        if process.returncode == 0:
            if download_path.exists():
                downloaded_size = get_file_size(download_path)
                avg_download_speed = downloaded_size / download_time if download_time > 0 else 0

                # Parse speeds from stderr (where AWS CLI shows progress)
                speeds = parse_aws_cli_speeds(stderr)
                min_speed = min(speeds) if speeds else None
                max_speed = max(speeds) if speeds else None

                logger.info(
                    f"Successfully downloaded {file_name} in {download_time:.2f}s at {format_speed(avg_download_speed)}"
                )
                if min_speed and max_speed:
                    logger.info(f"  Speed range: {format_speed(min_speed)} - {format_speed(max_speed)}")

                return download_time, avg_download_speed, downloaded_size, min_speed, max_speed
            logger.error(f"Downloaded file {file_name} not found after AWS CLI command")
            return None, None, None, None, None
        logger.error(f"AWS CLI download failed for {file_name}: {stderr}")
        return None, None, None, None, None

    except subprocess.TimeoutExpired:
        logger.error(f"AWS CLI download timeout for {file_name}")
        return None, None, None, None, None
    except Exception as e:
        logger.error(f"AWS CLI download error for {file_name}: {e}")
        return None, None, None, None, None


def create_bucket_aws_cli(bucket, endpoint_url, env):
    """Create a bucket using AWS CLI."""
    logger.info(f"Creating bucket: {bucket}")

    cmd = ["aws", "s3api", "create-bucket", "--bucket", bucket, "--endpoint-url", endpoint_url]

    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=60)

        if result.returncode == 0:
            logger.info(f"Successfully created bucket: {bucket}")
            return True
        logger.error(f"Failed to create bucket {bucket}: {result.stderr}")
        return False

    except Exception as e:
        logger.error(f"Error creating bucket {bucket}: {e}")
        return False


def set_bucket_policy_public_aws_cli(bucket, endpoint_url, env):
    """Set bucket policy to make it public using AWS CLI."""
    logger.info(f"Setting bucket policy to make {bucket} public...")

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            {"Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": f"arn:aws:s3:::{bucket}/*"}
        ],
    }

    # Write policy to temporary file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(policy, f)
        policy_file = f.name

    cmd = [
        "aws",
        "s3api",
        "put-bucket-policy",
        "--bucket",
        bucket,
        "--policy",
        f"file://{policy_file}",
        "--endpoint-url",
        endpoint_url,
    ]

    try:
        result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=60)

        # Clean up temporary file
        os.unlink(policy_file)

        if result.returncode == 0:
            logger.info(f"Successfully set bucket policy for {bucket} - bucket is now public")
            return True
        logger.error(f"Failed to set bucket policy for {bucket}: {result.stderr}")
        return False

    except Exception as e:
        # Clean up temporary file on error
        try:
            os.unlink(policy_file)
        except:
            pass
        logger.error(f"Error setting bucket policy for {bucket}: {e}")
        return False


def wait_for_cid_aws_cli(file_name, bucket, endpoint_url, env, max_attempts=100, poll_interval=5):
    """Poll using AWS CLI list-objects-v2 until Owner.ID changes from 'pending' to a CID."""
    logger.info(f"Waiting for CID to be available for {file_name} in bucket {bucket}")

    for attempt in range(max_attempts):
        cmd = [
            "aws",
            "s3api",
            "list-objects-v2",
            "--bucket",
            bucket,
            "--prefix",
            file_name,
            "--max-keys",
            "1",
            "--endpoint-url",
            endpoint_url,
        ]

        try:
            result = subprocess.run(cmd, env=env, capture_output=True, text=True, timeout=30)

            if result.returncode == 0:
                response = json.loads(result.stdout)

                if "Contents" not in response or len(response["Contents"]) == 0:
                    logger.warning(
                        f"Object {file_name} not found in bucket listing, attempt {attempt + 1}/{max_attempts}"
                    )
                    time.sleep(poll_interval)
                    continue

                # Find exact match for our file
                obj = None
                for content in response["Contents"]:
                    if content["Key"] == file_name:
                        obj = content
                        break

                if not obj:
                    logger.warning(
                        f"Exact object {file_name} not found in bucket listing, attempt {attempt + 1}/{max_attempts}"
                    )
                    time.sleep(poll_interval)
                    continue

                owner_id = obj.get("Owner", {}).get("ID", "pending")

                if owner_id != "pending" and owner_id:
                    logger.info(f"CID is now available for {file_name}: {owner_id[:20]}...")
                    return True

                logger.debug(f"CID still pending for {file_name}, attempt {attempt + 1}/{max_attempts}")
            else:
                logger.error(
                    f"Error checking CID status for {file_name}, attempt {attempt + 1}/{max_attempts}: {result.stderr}"
                )

        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON response for {file_name}, attempt {attempt + 1}/{max_attempts}: {e}")
        except Exception as e:
            logger.error(f"Error checking CID status for {file_name}, attempt {attempt + 1}/{max_attempts}: {e}")

        if attempt < max_attempts - 1:  # Don't sleep on the last attempt
            time.sleep(poll_interval)

    logger.warning(f"CID polling timeout for {file_name} after {max_attempts} attempts")
    return False


def benchmark_s3_endpoint(results):
    """Test S3 endpoint performance using AWS CLI."""
    s3_bucket = f"s3-{bucket_name}"
    download_dir = Path("downloaded/s3")
    download_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting S3 endpoint benchmark with bucket {s3_bucket}")
    logger.info("Using 128MB chunks for AWS CLI transfers")

    # Setup AWS CLI environment and configuration
    env = setup_aws_cli_config(endpoint_url, s3_access_key, s3_seed_phrase)

    # Create bucket
    if not create_bucket_aws_cli(s3_bucket, endpoint_url, env):
        logger.error(f"Failed to create S3 bucket {s3_bucket}, skipping S3 tests")
        return

    # Set bucket policy to make it public (for unencrypted storage)
    set_bucket_policy_public_aws_cli(s3_bucket, endpoint_url, env)

    # Upload and download each file
    for file_name in files:
        logger.info(f"Testing file {file_name} on S3 endpoint")
        file_path = Path(file_name)

        if not file_path.exists():
            logger.warning(f"File {file_name} not found, recording as failed")
            if file_name not in results:
                results[file_name] = {}
            results[file_name]["s3"] = {
                "upload_time": None,
                "upload_speed": None,
                "download_time": None,
                "download_speed": None,
                "file_size": None,
            }
            continue

        file_size = get_file_size(file_path)

        # Initialize results for this file
        if file_name not in results:
            results[file_name] = {}
        results[file_name]["file_size"] = file_size

        # Upload test
        logger.info(f"Starting S3 upload test for {file_name}")
        upload_time, upload_speed_avg, upload_speed_min, upload_speed_max = upload_file_aws_cli(
            file_name, s3_bucket, endpoint_url, env
        )

        if upload_time is not None:
            logger.info(f"S3 upload successful for {file_name}")

            # Wait for CID to be available after upload
            logger.info(f"Waiting for CID to be available for {file_name}...")
            cid_available = wait_for_cid_aws_cli(file_name, s3_bucket, endpoint_url, env)
            if cid_available:
                logger.info(f"CID is now available for {file_name}")
            else:
                logger.warning(f"CID not available after polling timeout for {file_name}")
        else:
            logger.error(f"S3 upload failed for {file_name}")

        # Download test (only after CID is available)
        download_time = download_speed_avg = downloaded_size = download_speed_min = download_speed_max = None
        if upload_time is not None and cid_available:
            logger.info(f"Starting S3 download test for {file_name}")
            download_time, download_speed_avg, downloaded_size, download_speed_min, download_speed_max = (
                download_file_aws_cli(file_name, s3_bucket, endpoint_url, env, download_dir)
            )

        if download_time is not None:
            logger.info(f"S3 download successful for {file_name}")
        else:
            logger.error(f"S3 download failed for {file_name}")

        # Store results
        results[file_name]["s3"] = {
            "upload_time": upload_time,
            "upload_speed_avg": upload_speed_avg,
            "upload_speed_min": upload_speed_min,
            "upload_speed_max": upload_speed_max,
            "download_time": download_time,
            "download_speed_avg": download_speed_avg,
            "download_speed_min": download_speed_min,
            "download_speed_max": download_speed_max,
            "downloaded_size": downloaded_size,
        }

    logger.info(f"S3 endpoint benchmark completed for bucket {s3_bucket}")


def benchmark_r2_endpoint(results):
    """Test R2 endpoint performance using AWS CLI."""
    r2_bucket = f"r2-{bucket_name}"
    download_dir = Path("downloaded/r2")
    download_dir.mkdir(parents=True, exist_ok=True)

    logger.info(f"Starting R2 endpoint benchmark with bucket {r2_bucket}")
    logger.info("Using 128MB chunks for AWS CLI transfers")

    # Setup AWS CLI environment and configuration for R2
    env = setup_aws_cli_config(r2_endpoint, r2_access_key, r2_secret_key, "auto")

    # Create bucket
    if not create_bucket_aws_cli(r2_bucket, r2_endpoint, env):
        logger.error(f"Failed to create R2 bucket {r2_bucket}, skipping R2 tests")
        return

    # Upload and download each file
    for file_name in files:
        logger.info(f"Testing file {file_name} on R2 endpoint")
        file_path = Path(file_name)

        if not file_path.exists():
            logger.warning(f"File {file_name} not found, recording as failed")
            if file_name not in results:
                results[file_name] = {}
            results[file_name]["r2"] = {
                "upload_time": None,
                "upload_speed": None,
                "download_time": None,
                "download_speed": None,
            }
            continue

        file_size = get_file_size(file_path)

        # Initialize results for this file if not exists
        if file_name not in results:
            results[file_name] = {}
            results[file_name]["file_size"] = file_size

        # Upload test
        logger.info(f"Starting R2 upload test for {file_name}")
        upload_time, upload_speed_avg, upload_speed_min, upload_speed_max = upload_file_aws_cli(
            file_name, r2_bucket, r2_endpoint, env
        )

        if upload_time is not None:
            logger.info(f"R2 upload successful for {file_name}")
        else:
            logger.error(f"R2 upload failed for {file_name}")

        # Download test
        logger.info(f"Starting R2 download test for {file_name}")
        download_time, download_speed_avg, downloaded_size, download_speed_min, download_speed_max = (
            download_file_aws_cli(file_name, r2_bucket, r2_endpoint, env, download_dir)
        )

        if download_time is not None:
            logger.info(f"R2 download successful for {file_name}")
        else:
            logger.error(f"R2 download failed for {file_name}")

        # Store results
        results[file_name]["r2"] = {
            "upload_time": upload_time,
            "upload_speed_avg": upload_speed_avg,
            "upload_speed_min": upload_speed_min,
            "upload_speed_max": upload_speed_max,
            "download_time": download_time,
            "download_speed_avg": download_speed_avg,
            "download_speed_min": download_speed_min,
            "download_speed_max": download_speed_max,
            "downloaded_size": downloaded_size,
        }

    logger.info(f"R2 endpoint benchmark completed for bucket {r2_bucket}")


def save_results_to_csv(results):
    """Save results to CSV file."""
    timestamp = int(time.time())
    csv_filename = f"s3_vs_r2_speed_test_{timestamp}.csv"

    logger.info(f"Saving benchmark results to {csv_filename}")

    with open(csv_filename, "w", newline="") as csvfile:
        fieldnames = [
            "file_name",
            "file_size_mb",
            "s3_upload_time_s",
            "s3_upload_speed_avg_mbps",
            "s3_upload_speed_min_mbps",
            "s3_upload_speed_max_mbps",
            "s3_download_time_s",
            "s3_download_speed_avg_mbps",
            "s3_download_speed_min_mbps",
            "s3_download_speed_max_mbps",
            "r2_upload_time_s",
            "r2_upload_speed_avg_mbps",
            "r2_upload_speed_min_mbps",
            "r2_upload_speed_max_mbps",
            "r2_download_time_s",
            "r2_download_speed_avg_mbps",
            "r2_download_speed_min_mbps",
            "r2_download_speed_max_mbps",
        ]

        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()

        for file_name, file_results in results.items():
            if "file_size" not in file_results:
                continue

            file_size_mb = file_results["file_size"] / (1024 * 1024)

            row = {
                "file_name": file_name,
                "file_size_mb": f"{file_size_mb:.2f}",
            }

            # S3 results
            if "s3" in file_results:
                s3_data = file_results["s3"]
                row.update(
                    {
                        "s3_upload_time_s": f"{s3_data['upload_time']:.2f}" if s3_data["upload_time"] else "N/A",
                        "s3_upload_speed_avg_mbps": f"{(s3_data['upload_speed_avg'] / (1024 * 1024)):.2f}"
                        if s3_data["upload_speed_avg"]
                        else "N/A",
                        "s3_upload_speed_min_mbps": f"{(s3_data['upload_speed_min'] / (1024 * 1024)):.2f}"
                        if s3_data["upload_speed_min"]
                        else "N/A",
                        "s3_upload_speed_max_mbps": f"{(s3_data['upload_speed_max'] / (1024 * 1024)):.2f}"
                        if s3_data["upload_speed_max"]
                        else "N/A",
                        "s3_download_time_s": f"{s3_data['download_time']:.2f}" if s3_data["download_time"] else "N/A",
                        "s3_download_speed_avg_mbps": f"{(s3_data['download_speed_avg'] / (1024 * 1024)):.2f}"
                        if s3_data["download_speed_avg"]
                        else "N/A",
                        "s3_download_speed_min_mbps": f"{(s3_data['download_speed_min'] / (1024 * 1024)):.2f}"
                        if s3_data["download_speed_min"]
                        else "N/A",
                        "s3_download_speed_max_mbps": f"{(s3_data['download_speed_max'] / (1024 * 1024)):.2f}"
                        if s3_data["download_speed_max"]
                        else "N/A",
                    }
                )
            else:
                row.update(
                    {
                        "s3_upload_time_s": "N/A",
                        "s3_upload_speed_avg_mbps": "N/A",
                        "s3_upload_speed_min_mbps": "N/A",
                        "s3_upload_speed_max_mbps": "N/A",
                        "s3_download_time_s": "N/A",
                        "s3_download_speed_avg_mbps": "N/A",
                        "s3_download_speed_min_mbps": "N/A",
                        "s3_download_speed_max_mbps": "N/A",
                    }
                )

            # R2 results
            if "r2" in file_results:
                r2_data = file_results["r2"]
                row.update(
                    {
                        "r2_upload_time_s": f"{r2_data['upload_time']:.2f}" if r2_data["upload_time"] else "N/A",
                        "r2_upload_speed_avg_mbps": f"{(r2_data['upload_speed_avg'] / (1024 * 1024)):.2f}"
                        if r2_data["upload_speed_avg"]
                        else "N/A",
                        "r2_upload_speed_min_mbps": f"{(r2_data['upload_speed_min'] / (1024 * 1024)):.2f}"
                        if r2_data["upload_speed_min"]
                        else "N/A",
                        "r2_upload_speed_max_mbps": f"{(r2_data['upload_speed_max'] / (1024 * 1024)):.2f}"
                        if r2_data["upload_speed_max"]
                        else "N/A",
                        "r2_download_time_s": f"{r2_data['download_time']:.2f}" if r2_data["download_time"] else "N/A",
                        "r2_download_speed_avg_mbps": f"{(r2_data['download_speed_avg'] / (1024 * 1024)):.2f}"
                        if r2_data["download_speed_avg"]
                        else "N/A",
                        "r2_download_speed_min_mbps": f"{(r2_data['download_speed_min'] / (1024 * 1024)):.2f}"
                        if r2_data["download_speed_min"]
                        else "N/A",
                        "r2_download_speed_max_mbps": f"{(r2_data['download_speed_max'] / (1024 * 1024)):.2f}"
                        if r2_data["download_speed_max"]
                        else "N/A",
                    }
                )
            else:
                row.update(
                    {
                        "r2_upload_time_s": "N/A",
                        "r2_upload_speed_avg_mbps": "N/A",
                        "r2_upload_speed_min_mbps": "N/A",
                        "r2_upload_speed_max_mbps": "N/A",
                        "r2_download_time_s": "N/A",
                        "r2_download_speed_avg_mbps": "N/A",
                        "r2_download_speed_min_mbps": "N/A",
                        "r2_download_speed_max_mbps": "N/A",
                    }
                )

            writer.writerow(row)

    logger.info(f"Benchmark results saved to {csv_filename}")
    return csv_filename


def print_summary(results):
    """Print a summary comparison."""

    for file_results in results.values():
        if "file_size" not in file_results:
            continue

        if "s3" in file_results and "r2" in file_results:
            s3_data = file_results["s3"]
            r2_data = file_results["r2"]

            # Upload comparison
            if s3_data["upload_speed_avg"] and r2_data["upload_speed_avg"]:
                s3_up_mbps = s3_data["upload_speed_avg"] / (1024 * 1024)
                r2_up_mbps = r2_data["upload_speed_avg"] / (1024 * 1024)
                abs(s3_up_mbps - r2_up_mbps)

            # Download comparison
            if s3_data["download_speed_avg"] and r2_data["download_speed_avg"]:
                s3_down_mbps = s3_data["download_speed_avg"] / (1024 * 1024)
                r2_down_mbps = r2_data["download_speed_avg"] / (1024 * 1024)
                abs(s3_down_mbps - r2_down_mbps)


def main():
    logger.info("Starting S3 vs R2 speed benchmark using AWS CLI")

    # Check for required environment variables
    required_vars = ["HIPPIUS_ACC_1_SUBACCOUNT_UPLOAD", "R2_ACCESS_KEY", "R2_SECRET_KEY", "R2_ENDPOINT"]
    missing_vars = [var for var in required_vars if not os.environ.get(var)]

    if missing_vars:
        logger.error(f"Missing required environment variables: {missing_vars}")
        return

    logger.info("Environment variables validated successfully")
    logger.info(f"Testing files: {files}")
    logger.info("AWS CLI configuration: 128MB chunks, 10 concurrent requests")

    results = {}

    # Test S3 endpoint
    logger.info("Starting S3 endpoint tests")
    benchmark_s3_endpoint(results)

    # Test R2 endpoint
    logger.info("Starting R2 endpoint tests")
    benchmark_r2_endpoint(results)

    # Print summary
    logger.info("Generating summary")
    print_summary(results)

    # Save to CSV
    logger.info("Saving results to CSV")
    csv_file = save_results_to_csv(results)

    logger.info(f"Benchmark completed. Results saved to {csv_file}")


if __name__ == "__main__":
    main()
