# Hippius S3 -- Python Examples

## Prerequisites

- Python 3.10+
- `pip install minio boto3`

## Credentials

Two authentication methods:

**Seed phrase (SigV4)**: Base64-encode your 12-word seed phrase as the access key, use the plain seed phrase as the secret key.

**Access key**: Use `hip_*` access keys from https://console.hippius.com/dashboard/settings.

## Environment Variables

```bash
export AWS_ACCESS_KEY_ID=hip_your_access_key_here
export AWS_SECRET_ACCESS_KEY=your_secret_key_here
export AWS_ENDPOINT_URL_S3=https://s3.hippius.com
export AWS_DEFAULT_REGION=decentralized
export S3_BUCKET_NAME=your-bucket-name
```

## Examples

| File | Description |
|------|-------------|
| `upload_and_presign.py` | Upload a file and generate a presigned download URL (MinIO SDK) |
| `boto3_example.py` | Same upload/presign flow using boto3 (simplest, fewest deps) |
| `async_hippius_client.py` | Full-featured async client class with upload, download, delete, list, presigned URLs |

## Running

```bash
python upload_and_presign.py
python boto3_example.py
python async_hippius_client.py
```
