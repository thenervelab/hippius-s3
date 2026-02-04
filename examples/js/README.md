# Hippius S3 — JavaScript Examples

## Prerequisites

- Node.js 18+
- `npm install`

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
| `upload_and_presign.js` | Upload a file and generate a presigned download URL |

## Running

```bash
npm install
node upload_and_presign.js
```
