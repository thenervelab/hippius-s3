# Resumable S3 multipart uploads to Hippius

`aws s3 cp` doesn't persist the `uploadId`, so a network drop forces a full re-upload. Hippius speaks the full S3 resume protocol server-side — you just need a client that uses it. Two options.

---

## Option A — `s3cmd`

`pip install s3cmd`. Required flags against Hippius:

- `--host=s3.hippius.com --host-bucket=s3.hippius.com` — path-style (virtual-hosted is not supported).
- `--region=decentralized` — without it, s3cmd signs against `us-east-1` and every request 403s.
- `--multipart-chunk-size-mb=N` — must match on resume or parts won't line up.

```bash
# Upload
s3cmd put ./bigfile.bin s3://mybucket/bigfile.bin \
  --access_key=HIP_AK --secret_key=HIP_SK \
  --host=s3.hippius.com --host-bucket=s3.hippius.com \
  --region=decentralized --multipart-chunk-size-mb=16

# If interrupted: find the dangling UploadId
s3cmd multipart s3://mybucket <same flags>

# Resume
s3cmd put ./bigfile.bin s3://mybucket/bigfile.bin <same flags> \
  --upload-id=<ID_FROM_MULTIPART_CMD>

# Abandon
s3cmd abortmp s3://mybucket/bigfile.bin <ID> <same flags>
```

### Skip the flags with `~/.s3cfg`

```ini
[default]
host_base = s3.hippius.com
host_bucket = s3.hippius.com
bucket_location = decentralized
use_https = True
access_key = HIP_AK
secret_key = HIP_SK
multipart_chunk_size_mb = 16
put_continue = True
```

Then just `s3cmd put ./bigfile.bin s3://mybucket/bigfile.bin`.

Docs: [s3tools wiki](https://github.com/s3tools/s3cmd/wiki/How-to-configure-s3cmd-for-alternative-S3-compatible-services) · [s3cmd source](https://raw.githubusercontent.com/s3tools/s3cmd/master/s3cmd)

---

## Option B — `retryable-mpu.py`

Single file: [`scripts/retryable-mpu.py`](./retryable-mpu.py). Requires `boto3`.

```bash
export AWS_ACCESS_KEY_ID=HIP_AK
export AWS_SECRET_ACCESS_KEY=HIP_SK
export AWS_DEFAULT_REGION=decentralized
export S3_ENDPOINT_URL=https://s3.hippius.com

# Upload (or resume — same command)
./scripts/retryable-mpu.py --file ./bigfile.bin --bucket mybucket --key bigfile.bin

# Other actions
./scripts/retryable-mpu.py --bucket mybucket --list
./scripts/retryable-mpu.py --file ./bigfile.bin --bucket mybucket --key bigfile.bin --abort
./scripts/retryable-mpu.py --file ./bigfile.bin --bucket mybucket --key bigfile.bin --force-restart
```

State is persisted to `./bigfile.bin.mpu-state.json`, updated atomically after each part. `Ctrl+C` preserves the sidecar; a second `Ctrl+C` force-exits. If the source file's size or mtime changed since the sidecar was written, the script refuses to continue unless you pass `--force-restart`.

### Flags

```
--file FILE               Local file to upload
--bucket BUCKET           Destination bucket
--key KEY                 Destination key (default: basename of --file)
--endpoint-url URL        Default: $S3_ENDPOINT_URL, else AWS
--part-size-mb N          Default: 16
--workers N               Concurrent upload workers (default: 4)
--max-retries N           Per-part retry attempts (default: 5)
--force-restart           Abort existing MPU, discard sidecar, start fresh
--list                    List in-progress multipart uploads
--abort                   Abort an in-progress upload for --bucket/--key
--upload-id ID            Explicit uploadId for --abort
```
