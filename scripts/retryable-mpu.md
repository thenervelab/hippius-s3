# Resumable S3 multipart uploads to Hippius

If a network drop kills an `aws s3 cp` of a large file, the AWS CLI starts over from zero on retry — it doesn't persist the `uploadId`, so the in-progress multipart upload is orphaned and every already-uploaded part is re-uploaded from scratch.

Hippius S3 implements the server-side resume protocol correctly (`ListMultipartUploads`, `ListParts`, part re-upload on the same `uploadId`, `CompleteMultipartUpload` on a previously-initiated MPU). You just need a client that actually uses it. Two options below.

---

## Option A — `s3cmd` (third-party, no code)

`s3cmd` has had resumable MPU for years via `--continue-put` / `--upload-id`. Install with `pip install s3cmd` or your package manager.

> `s3cmd` has no `--endpoint-url` flag (that's AWS CLI / s5cmd syntax). Use `--host` + `--host-bucket` instead — [s3tools wiki](https://github.com/s3tools/s3cmd/wiki/How-to-configure-s3cmd-for-alternative-S3-compatible-services).

### Start an upload

Hippius does **not** support virtual-hosted-style addressing, so set `--host-bucket` equal to `--host` (this forces path-style). Also pass `--region=decentralized` — without it, s3cmd signs against `us-east-1`/`US` and the gateway rejects with `AuthorizationHeaderMalformed` / `InvalidAccessKeyId`.

```bash
s3cmd put ./bigfile.bin s3://mybucket/bigfile.bin \
  --access_key=HIP_AK --secret_key=HIP_SK \
  --host=s3.hippius.com --host-bucket=s3.hippius.com \
  --region=decentralized \
  --multipart-chunk-size-mb=16
```

If the network drops mid-upload, the MPU is left in-progress server-side.

### Find the dangling `UploadId`

```bash
s3cmd multipart s3://mybucket \
  --access_key=HIP_AK --secret_key=HIP_SK \
  --host=s3.hippius.com --host-bucket=s3.hippius.com \
  --region=decentralized
```

Output:
```
Initiated                         Path                            Id
2026-04-20T16:20:23.126633+00:00  s3://mybucket/bigfile.bin       ac33fb42-aa96-46d8-8336-2e9173e68373
```

### Resume — pin to that `UploadId`

```bash
s3cmd put ./bigfile.bin s3://mybucket/bigfile.bin \
  --access_key=HIP_AK --secret_key=HIP_SK \
  --host=s3.hippius.com --host-bucket=s3.hippius.com \
  --region=decentralized \
  --multipart-chunk-size-mb=16 \
  --upload-id=<ID_FROM_MULTIPART_CMD>
```

`s3cmd` calls `ListParts`, diffs against the local file (size + md5 per part), skips parts that match, and re-uploads mismatches.

### Abandon instead of resuming

```bash
s3cmd abortmp s3://mybucket/bigfile.bin <ID> \
  --access_key=HIP_AK --secret_key=HIP_SK \
  --host=s3.hippius.com --host-bucket=s3.hippius.com \
  --region=decentralized
```

### Gotchas

- **Path-style only on Hippius.** `--host-bucket` must equal `--host`; the `%(bucket)s.s3.hippius.com` virtual-hosted template will hang or fail.
- **`--region=decentralized` is mandatory.** Without it, s3cmd signs against `us-east-1`/`US` and every request 403s.
- **`--multipart-chunk-size-mb` must match** the original value on resume — otherwise parts won't line up.
- **Avoid passing flags through unquoted shell variables** (`$S3CMD_ARGS`). Shell splitting occasionally drops `--host=...`, making s3cmd fall back to signing against AWS. Use direct CLI args or put everything in `~/.s3cfg`.
- **Same local file required.** Resume keys off per-part md5+size; if the file changed, that part gets re-uploaded.
- **Aborts can be slow on the server** (observed 3–4 min end-to-end on `s3.hippius.com`). The request succeeds but don't be surprised if it blocks for a while.
- **Orphaned uploads use quota.** Clean up with `s3cmd multipart` → `s3cmd abortmp`.
- `--continue-put` without `--upload-id` picks one for you; if multiple are in-progress for the same key, prefer the explicit `--upload-id`.
- You can set `put_continue = True` in `~/.s3cfg` to make resume the default.

### Permanent config (skip the flags)

Save to `~/.s3cfg`:

```ini
[default]
host_base = s3.hippius.com
host_bucket = s3.hippius.com
bucket_location = decentralized
use_https = True
signature_v2 = False
access_key = HIP_AK
secret_key = HIP_SK
multipart_chunk_size_mb = 16
put_continue = True
```

Then just `s3cmd put ./bigfile.bin s3://mybucket/bigfile.bin`.

### Docs

- [s3cmd master source (authoritative — `--help` text verbatim)](https://raw.githubusercontent.com/s3tools/s3cmd/master/s3cmd)
- [s3tools.org — S3cmd Usage](https://s3tools.org/usage)
- [s3tools.org KB — Does s3cmd support multipart uploads?](https://s3tools.org/kb/item13.htm)

---

## Option B — `retryable-mpu.py` (bundled with this repo)

Single-file Python script: [`scripts/retryable-mpu.py`](./retryable-mpu.py). No config file, no `--host-bucket` template — give it a file, a bucket, and AWS creds in env, and it resumes automatically if it finds a sidecar state file or a matching in-progress MPU server-side.

### Install

Requires `boto3` (already in this repo's deps). For standalone use:

```bash
pip install boto3
```

### Start an upload

```bash
export AWS_ACCESS_KEY_ID=HIP_AK
export AWS_SECRET_ACCESS_KEY=HIP_SK
export S3_ENDPOINT_URL=https://s3.hippius.com

./scripts/retryable-mpu.py --file ./bigfile.bin --bucket mybucket --key bigfile.bin
```

The script writes `./bigfile.bin.mpu-state.json` next to the source file, updated atomically after every successful part.

### Resume after an interruption

Same command. No flags to remember:

```bash
./scripts/retryable-mpu.py --file ./bigfile.bin --bucket mybucket --key bigfile.bin
```

It loads the sidecar, calls `ListParts` to reconcile with the server (server is authoritative — mismatched parts get re-uploaded), and only uploads the missing parts. `Ctrl+C` preserves the sidecar so you can resume later; a second `Ctrl+C` force-exits.

### List in-progress uploads

```bash
./scripts/retryable-mpu.py --bucket mybucket --list
```

### Abort cleanly

```bash
./scripts/retryable-mpu.py --file ./bigfile.bin --bucket mybucket --key bigfile.bin --abort
```

### Force a fresh upload (discard sidecar + abort existing MPU)

```bash
./scripts/retryable-mpu.py --file ./bigfile.bin --bucket mybucket --key bigfile.bin --force-restart
```

### Full flag list

```
--file FILE               Local file to upload
--bucket BUCKET           Destination bucket
--key KEY                 Destination key (default: basename of --file)
--endpoint-url URL        Default: $S3_ENDPOINT_URL, else AWS
--part-size-mb N          Default: 16
--workers N               Concurrent upload workers (default: 4)
--max-retries N           Per-part retry attempts (default: 5)
--force-restart           Abort existing MPU, discard sidecar, start fresh
--list                    List in-progress multipart uploads and exit
--abort                   Abort an in-progress upload for --bucket/--key
--upload-id ID            Explicit uploadId for --abort
```

### Gotchas

- **Source file mtime/size is checked on resume.** If the file changed since the sidecar was written, the script refuses to continue — re-run with `--force-restart`.
- **No sidecar + server has an in-progress MPU for the same key** → the script adopts it and infers `part_size` from the lowest-numbered existing part. Works as long as all non-final parts were uploaded at the same size (which is the common case).
- **Orphaned uploads still cost quota.** Use `--list` then `--abort` to clean up.
