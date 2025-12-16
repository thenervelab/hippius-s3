# S3 Endpoint Benchmark Tool

A standalone benchmark script for testing upload/download performance against S3-compatible endpoints.

## Features

- Tests upload and download speeds for multiple file sizes (5MB, 50MB, 500MB, 2GB, 5GB)
- MD5 hash verification to ensure data integrity
- Sequential testing (upload all files, then download all files)
- CSV results export with detailed metrics
- Configurable endpoint support (default: https://s3.hippius.com)
- Automatic bucket cleanup
- Supports **AWS CLI** (if installed) or **boto3** (automatic fallback)

## Installation

```bash
pip install -r requirements.txt
```

Or if you have the main project dependencies installed, you can run directly since boto3 is already available.

### Optional: AWS CLI

If you want to use the AWS CLI engine, install `aws` on the machine running the benchmark:

```bash
sudo apt-get update && sudo apt-get install -y awscli
```

## Setup

### Environment Variables (.env file)

Create a `.env` file in the `benchmark/` directory with your credentials:

```env
# Hippius S3 credentials
HIPPIUS_ENDPOINT=https://s3.hippius.com
HIPPIUS_ACCESS_KEY=your-hippius-access-key
HIPPIUS_SECRET_KEY=your-hippius-secret-key

# Cloudflare R2 credentials (optional)
R2_ENDPOINT=https://your-account.r2.cloudflarestorage.com
R2_ACCESS_KEY=your-r2-access-key
R2_SECRET_KEY=your-r2-secret-key
```

For custom endpoints, use standard AWS environment variables:

```bash
export AWS_ACCESS_KEY_ID="your-access-key"
export AWS_SECRET_ACCESS_KEY="your-secret-key"
```

## Usage

### Basic Usage (Hippius S3)

```bash
python benchmark.py --hippius
```

By default, the script uses `--engine auto`:

- Uses **AWS CLI** if the `aws` executable is present
- Otherwise falls back to **boto3**

### Test Cloudflare R2

```bash
python benchmark.py --r2
```

### Test Custom Endpoint

```bash
python benchmark.py --endpoint https://s3.amazonaws.com
```

### Choose transfer engine explicitly

```bash
# Force boto3 (recommended on minimal servers without awscli)
python benchmark.py --hippius --engine boto3

# Force AWS CLI (requires awscli installed)
python benchmark.py --hippius --engine awscli
```

The benchmark will:

1. Generate test files (5MB, 50MB, 500MB, 2GB, 5GB) in `test_files/` directory
2. Create a timestamped bucket `speedtest-{timestamp}`
3. Upload all files sequentially and measure performance
4. Download all files sequentially and measure performance
5. Verify data integrity with MD5 hashes
6. Save results to `<endpoint>-benchmark-{timestamp}.csv`
7. Clean up the bucket

### Keep Test Bucket

By default, the bucket is automatically deleted. To keep it:

```bash
python benchmark.py --keep-bucket
```

### Skip Hash Verification

To skip MD5 hash verification (faster, but no integrity check):

```bash
python benchmark.py --no-verify
```

### Regenerate Test Files

Force regeneration of test files even if they already exist:

```bash
python benchmark.py --regenerate
```

## Command-Line Options

| Option           | Description                    | Default        |
| ---------------- | ------------------------------ | -------------- |
| `--hippius`      | Use Hippius endpoint from .env | -              |
| `--r2`           | Use R2 endpoint from .env      | -              |
| `--endpoint URL` | Custom S3 endpoint URL         | -              |
| `--keep-bucket`  | Keep bucket after benchmark    | Auto-delete    |
| `--no-verify`    | Skip MD5 verification          | Verify enabled |
| `--regenerate`   | Force regenerate test files    | Use existing   |

**Note:** You must specify one of `--hippius`, `--r2`, or `--endpoint`.

## Output

### Console Output

The script provides real-time progress updates:

```
================================================================================
S3 Endpoint Benchmark
================================================================================
Endpoint: https://s3.hippius.com
Region: decentralized
Verify hashes: True
Keep bucket: False

Checking test files...
  5mb.bin already exists (5MB)
  50mb.bin already exists (50MB)
  ...

================================================================================
UPLOAD PHASE
================================================================================

Uploading 5mb.bin (5.00MB)...
  Upload time: 1.23s
  Upload speed: 4.07 MB/s

...

================================================================================
DOWNLOAD PHASE
================================================================================

Downloading 5mb.bin (5MB)...
  Download time: 0.89s
  Download speed: 5.62 MB/s
  Verifying integrity...

...

================================================================================
SUMMARY
================================================================================
Total data transferred: 7683.00 MB
Total upload time: 156.23s
Total download time: 132.45s
Average upload speed: 49.17 MB/s
Average download speed: 58.02 MB/s

Saving results to s3.hippius.com-benchmark-1234567890.csv...
  Results saved

Cleaning up bucket speedtest-1234567890...
  Bucket deleted

Benchmark completed successfully!
```

### CSV Output

Results are saved to `<endpoint>-benchmark-{timestamp}.csv`:

```csv
file_name,file_size_mb,upload_time_s,upload_speed_mbps,download_time_s,download_speed_mbps,hash_match,status
5mb.bin,5.00,1.23,4.07,0.89,5.62,true,ok
50mb.bin,50.00,12.34,4.05,8.91,5.61,true,ok
500mb.bin,500.00,98.76,5.06,87.65,5.70,true,ok
2gb.bin,2048.00,389.12,5.26,365.43,5.60,true,ok
5gb.bin,5120.00,1024.56,4.99,982.34,5.21,true,ok
```

## File Structure

```
benchmark/
├── benchmark.py          # Main benchmark script
├── requirements.txt      # Python dependencies
├── README.md            # This file
├── test_files/          # Generated test files (gitignored)
│   ├── 5mb.bin
│   ├── 50mb.bin
│   ├── 500mb.bin
│   ├── 2gb.bin
│   └── 5gb.bin
└── *.csv                # Benchmark results (gitignored)
```

## Troubleshooting

### Missing Credentials

If you see:

```
Error: Hippius credentials not found in .env file
```

Make sure you've created a `.env` file in the `benchmark/` directory with the required credentials (see Setup section).

For custom endpoints, ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables are set.

### Connection Errors

If you get connection errors, verify:

1. The endpoint URL is correct and accessible
2. Your network allows outbound HTTPS connections
3. The credentials are valid for the endpoint

### Large File Uploads Taking Long

The 2GB and 5GB files will take significant time depending on your network speed. This is expected. The script will show progress updates during the transfer.

### Hash Mismatch

If you see:

```
WARNING: Hash mismatch!
```

This indicates data corruption during transfer. This could be due to:

- Network issues
- Endpoint issues
- Storage issues

Try running the benchmark again. If the issue persists, there may be a problem with the endpoint.

## Performance Tips

1. **First Run**: The first run will generate all test files, which takes time. Subsequent runs reuse existing files.
2. **Skip Verification**: Use `--no-verify` if you only care about speed, not integrity.
3. **Network**: Run from a location with good network connectivity to the endpoint.
4. **Disk Space**: Ensure you have at least 15GB free space (7.7GB for test files + 7.7GB for downloads during benchmark).

## Examples

### Compare Multiple Endpoints

```bash
# Test Hippius
python benchmark.py --hippius

# Test Cloudflare R2
python benchmark.py --r2

# Test AWS S3 (requires AWS credentials in environment)
export AWS_ACCESS_KEY_ID="your-key"
export AWS_SECRET_ACCESS_KEY="your-secret"
python benchmark.py --endpoint https://s3.amazonaws.com
```

### Quick Test (No Verification)

```bash
python benchmark.py --hippius --no-verify
```

### Keep Bucket for Inspection

```bash
python benchmark.py --hippius --keep-bucket
# Bucket will remain for manual inspection
```

## Notes

- Test files are generated with random data using `os.urandom()`
- Timing excludes hash computation (measures pure network transfer time)
- All operations are sequential (one file at a time)
- The script uses boto3's `upload_file()` and `download_file()` methods for efficient streaming
- Temporary download files are stored in system temp directory and automatically cleaned up
