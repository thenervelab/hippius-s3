# S3 Endpoint Benchmark Tool

Benchmark scripts for testing upload/download performance against S3-compatible endpoints.

## Scripts

| Script | Purpose |
|--------|---------|
| `benchmark.py` | On-demand comprehensive benchmark (5MB - 5GB files) |
| `daily_bench.py` | Lightweight daily benchmark for trend tracking (1MB, 100MB, 1GB) |
| `plot_results.py` | Generate trend charts from daily benchmark CSV data |

## Features

- Tests upload and download speeds for multiple file sizes
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

Or if you have the main project dependencies installed, boto3 is already available.

### Optional: AWS CLI

```bash
sudo apt-get update && sudo apt-get install -y awscli
```

## Setup

### Environment Variables (.env file)

Create a `.env` file in the `benchmark/` directory:

```env
# Hippius S3 credentials (seed phrase auth or access key)
HIPPIUS_ENDPOINT=https://s3.hippius.com
HIPPIUS_ACCESS_KEY=your-hippius-access-key
HIPPIUS_SECRET_KEY=your-hippius-secret-key

# Cloudflare R2 credentials (optional)
R2_ENDPOINT=https://your-account.r2.cloudflarestorage.com
R2_ACCESS_KEY=your-r2-access-key
R2_SECRET_KEY=your-r2-secret-key
```

For Hippius credentials, either use `hip_*` access keys from https://console.hippius.com/dashboard/settings, or base64-encode your seed phrase as the access key with the plain seed as the secret.

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

## Daily Benchmark (CI/CD)

The daily benchmark runs automatically via GitHub Actions (`.github/workflows/production-daily-benchmark.yml`) at 6 AM UTC:

1. Runs `daily_bench.py` against production (https://s3.hippius.com)
2. Tests 1MB, 100MB, and 1GB uploads/downloads (3 iterations each)
3. Appends results to `daily.csv` in the `hippius-benchmarks` bucket
4. Generates trend charts via `plot_results.py`

Results are visible at: https://s3.hippius.com/hippius-benchmarks/daily.svg

## Output

### CSV Output

```csv
file_name,file_size_mb,upload_time_s,upload_speed_mbps,download_time_s,download_speed_mbps,hash_match,status
5mb.bin,5.00,1.23,4.07,0.89,5.62,true,ok
50mb.bin,50.00,12.34,4.05,8.91,5.61,true,ok
```

## File Structure

```
benchmark/
├── benchmark.py          # On-demand comprehensive benchmark
├── daily_bench.py        # Lightweight daily CI benchmark
├── plot_results.py       # Trend chart generation
├── requirements.txt      # Python dependencies
├── README.md
├── test_files/           # Generated test files (gitignored)
└── *.csv                 # Benchmark results (gitignored)
```

## Troubleshooting

### Missing Credentials

Make sure you've created a `.env` file in the `benchmark/` directory with the required credentials (see Setup section). For custom endpoints, ensure `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables are set.

### Connection Errors

Verify:
1. The endpoint URL is correct and accessible
2. Your network allows outbound HTTPS connections
3. The credentials are valid for the endpoint

### Large File Uploads Taking Long

The 2GB and 5GB files will take significant time depending on your network speed. This is expected.

### Hash Mismatch

Indicates data corruption during transfer. Try running again. If persistent, investigate the endpoint.

## Performance Tips

1. **First Run**: First run generates test files, which takes time. Subsequent runs reuse existing files.
2. **Skip Verification**: Use `--no-verify` if you only care about speed, not integrity.
3. **Disk Space**: Ensure at least 15GB free space (7.7GB for test files + 7.7GB for downloads).
