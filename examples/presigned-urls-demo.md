# Hippius S3 — Presigned URLs

Generate temporary, shareable download links for your files stored on Hippius decentralized storage.

**Key features:**

- Tested with MinIO, AWS SDK, boto3, and AWS CLI
- Video streaming support with range requests for seeking/scrubbing
- ACL support for orchestrating fine-grained permissions to access your files
- No single point of failure — data is replicated across the Hippius network
- Standard S3-compatible API — use any S3 client you already know

## Configuration Reference

| Parameter | Value | Notes |
|-----------|-------|-------|
| Endpoint | `s3.hippius.com` | Fixed |
| Region | `decentralized` | Required |
| Secure | `True` | Always HTTPS |
| Max presigned URL expiry | 7 days (604,800s) | Enforced server-side |

## Get Your Hippius Credentials

1. Sign up at [console.hippius.com](https://console.hippius.com)
2. Navigate to **Dashboard → Settings**
3. Click **Create Access Key**
4. Copy your **Access Key ID** (starts with `hip_`) and **Secret Key**

You can create:

- **Main keys** — full access to all your buckets
- **Sub keys** — require explicit ACL grants (recommended for applications)

## Configure Environment Variables

Set these variables before running any example:

```bash
export AWS_ACCESS_KEY_ID=hip_your_access_key_here
export AWS_SECRET_ACCESS_KEY=your_secret_key_here
export S3_BUCKET_NAME=your-bucket-name
```

| Variable | Description |
|----------|-------------|
| `AWS_ACCESS_KEY_ID` | Your Hippius access key (`hip_...`) |
| `AWS_SECRET_ACCESS_KEY` | Your access key secret |
| `S3_BUCKET_NAME` | Target bucket name (lowercase, no spaces) |

The endpoint (`https://s3.hippius.com`) and region (`decentralized`) are hardcoded in all examples.

## Quick Start: Upload + Presigned URL (MinIO)

The following Python example uploads a file and generates a presigned download URL using the MinIO SDK.

```bash
pip install minio
```

```python
import os
from datetime import timedelta

from minio import Minio

client = Minio(
    "s3.hippius.com",
    access_key=os.environ["AWS_ACCESS_KEY_ID"],
    secret_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    secure=True,
    region="decentralized",
)

bucket = os.environ["S3_BUCKET_NAME"]
client.make_bucket(bucket)

client.put_object(bucket, "video.mp4", data=open("video.mp4", "rb"), length=os.path.getsize("video.mp4"))

url = client.presigned_get_object(bucket, "video.mp4", expires=timedelta(hours=1))
print(f"Presigned URL (1h expiry): {url}")
```

Full runnable example: [`py/upload_and_presign.py`](py/upload_and_presign.py)

## Quick Start: Using boto3

```bash
pip install boto3
```

```python
import os

import boto3
from botocore.config import Config

s3 = boto3.client(
    "s3",
    endpoint_url="https://s3.hippius.com",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    region_name="decentralized",
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)

bucket = os.environ["S3_BUCKET_NAME"]
s3.create_bucket(Bucket=bucket)

s3.upload_file("video.mp4", bucket, "video.mp4")

url = s3.generate_presigned_url(
    "get_object",
    Params={"Bucket": bucket, "Key": "video.mp4"},
    ExpiresIn=3600,
)
print(f"Presigned URL (1h expiry): {url}")
```

Full runnable example: [`py/boto3_example.py`](py/boto3_example.py)

## Quick Start: Using AWS CLI

```bash
# Configure credentials
export AWS_ACCESS_KEY_ID=hip_your_access_key_here
export AWS_SECRET_ACCESS_KEY=your_secret_key_here
export AWS_DEFAULT_REGION=decentralized

# Create a bucket
aws s3 mb s3://my-bucket --endpoint-url https://s3.hippius.com

# Upload a file
aws s3 cp video.mp4 s3://my-bucket/video.mp4 --endpoint-url https://s3.hippius.com

# List objects
aws s3 ls s3://my-bucket/ --endpoint-url https://s3.hippius.com

# Generate a presigned URL (1 hour expiry)
aws s3 presign s3://my-bucket/video.mp4 --endpoint-url https://s3.hippius.com --expires-in 3600
```

## Quick Start: Using JavaScript

```bash
npm install @aws-sdk/client-s3 @aws-sdk/s3-request-presigner
```

```javascript
const { S3Client, CreateBucketCommand, PutObjectCommand, GetObjectCommand } = require("@aws-sdk/client-s3");
const { getSignedUrl } = require("@aws-sdk/s3-request-presigner");
const fs = require("fs");

const client = new S3Client({
  endpoint: "https://s3.hippius.com",
  region: "decentralized",
  credentials: {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  },
  forcePathStyle: true,
});

const bucket = process.env.S3_BUCKET_NAME;

async function main() {
  await client.send(new CreateBucketCommand({ Bucket: bucket }));

  const body = fs.readFileSync("video.mp4");
  await client.send(new PutObjectCommand({ Bucket: bucket, Key: "video.mp4", Body: body }));

  const url = await getSignedUrl(
    client,
    new GetObjectCommand({ Bucket: bucket, Key: "video.mp4" }),
    { expiresIn: 3600 }
  );
  console.log(`Presigned URL (1h expiry): ${url}`);
}

main();
```

Full runnable example: [`js/upload_and_presign.js`](js/upload_and_presign.js)

## Video Streaming with Range Requests

Hippius supports range requests, enabling seeking and scrubbing in video players. Upload a video file and generate a presigned URL — then use it directly in an HTML video tag:

```html
<video controls width="720">
  <source src="YOUR_PRESIGNED_URL_HERE" type="video/mp4" />
</video>
```

The presigned URL works with any player that supports range requests, including `<video>` tags, HLS.js, Video.js, and native mobile players.

Live demo: [s3.hippius.com/micky/index.html](https://s3.hippius.com/micky/index.html)

## Advanced: Async Python Client

For applications that need a reusable client with upload, download, delete, list, and presigned URL methods, see the full-featured client class:

[`py/async_hippius_client.py`](py/async_hippius_client.py)

```python
from async_hippius_client import HippiusClient

client = HippiusClient()
await client.ensure_bucket("my-bucket")
await client.upload("my-bucket", "photo.jpg", photo_bytes, content_type="image/jpeg")
url = await client.presigned_url("my-bucket", "photo.jpg", expires_seconds=86400)
```

## FAQ

| Question | Answer |
|----------|--------|
| How is this different from regular S3? | Hippius uses the decentralized Hippius network instead of centralized data centers. The API is S3-compatible, so you use the same tools. |
| What happens if a storage provider goes offline? | Data is replicated across the Hippius network. Downloads automatically fail over to other providers. |
| Can I use the AWS CLI? | Yes, pass `--endpoint-url https://s3.hippius.com` to any `aws s3` command. |
| Maximum file size? | ~5 TiB via multipart upload (512 MiB per part, up to 10,000 parts). |
| Is data encrypted? | Yes — server-side encryption with per-object key derivation and envelope encryption (KMS). |
| Maximum presigned URL expiry? | 7 days (604,800 seconds), enforced server-side. |
| What is Bittensor Subnet 75? | The incentive network that rewards storage miners for hosting your data reliably. |
| Rate limits? | 100 requests per minute per account. |

## Resources

- [Hippius Console](https://console.hippius.com) — manage buckets, keys, and billing
- [S3 Compatibility Matrix](../docs/s3-compatibility.md) — full list of supported S3 operations
- [S4 Atomic Append](../docs/s4.md) — Hippius extension for atomic append operations
- [Python Examples](py/) — MinIO, boto3, and async client examples
- [JavaScript Examples](js/) — AWS SDK for JavaScript examples
- [Video Demo](https://s3.hippius.com/micky/index.html) — live presigned URL demo with video streaming
