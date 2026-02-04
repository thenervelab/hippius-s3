# Hippius S3 — Python Examples

## Prerequisites

- Python 3.10+
- `pip install minio boto3`

## Environment Variables

Set these before running any example:

```bash
export AWS_ACCESS_KEY_ID=hip_your_access_key_here
export AWS_SECRET_ACCESS_KEY=your_secret_key_here
export AWS_ENDPOINT_URL_S3=https://s3.hippius.com
export AWS_DEFAULT_REGION=decentralized
export S3_BUCKET_NAME=your-bucket-name
```

Get your credentials at: https://console.hippius.com/dashboard/settings

## Examples

| File | Description |
|------|-------------|
| `upload_and_presign.py` | Upload a file and generate a presigned download URL (MinIO SDK) |
| `boto3_example.py` | Same flow using boto3 |
| `async_hippius_client.py` | Full-featured client class with upload, download, delete, list, presigned URLs |

## Running

```bash
python upload_and_presign.py
python boto3_example.py
python async_hippius_client.py
```
