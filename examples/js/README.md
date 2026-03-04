# Hippius S3 -- JavaScript Examples

## Prerequisites

- Node.js 18+
- `npm install`

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
| `upload_and_presign.js` | Upload a file and generate a presigned download URL |

## Running

```bash
npm install
node upload_and_presign.js
```
