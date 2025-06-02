# Hippius S3

S3-compatible API gateway with IPFS storage and blockchain publishing

## Overview

Hippius S3 is a production-ready S3-compatible API that stores data on IPFS while automatically publishing to the Hippius blockchain marketplace. It provides standard S3 operations with AWS SigV4 authentication, server-side encryption, and seamless integration with existing S3 clients.

## Features

### ✅ Core S3 Operations
- **Bucket Management**: Create, list, delete, and check bucket existence
- **Object Operations**: Upload, download, list, delete, and copy objects
- **Multipart Uploads**: Complete support for large file uploads with part assembly
- **Metadata**: Custom metadata support with x-amz-meta-* headers
- **Tagging**: Bucket and object tagging with full CRUD operations

### ✅ Security & Authentication
- **AWS SigV4 HMAC**: Full AWS Signature Version 4 authentication
- **Seed Phrase Authentication**: Base64-encoded seed phrase credentials
- **Server-Side Encryption**: AES256 encryption via x-amz-server-side-encryption header
- **Account Credits**: Automatic credit verification for bucket creation
- **Rate Limiting**: 100 requests per minute per seed phrase to prevent abuse

### ✅ Blockchain Integration
- **IPFS Storage**: Automatic file storage and pinning via Hippius SDK
- **Blockchain Publishing**: Files automatically published to Hippius marketplace
- **Transaction Tracking**: Blockchain transaction hashes stored in metadata
- **Decentralized Access**: Files remain accessible via IPFS network

### ✅ S3 Client Compatibility
Works with standard S3 clients including:
- AWS CLI
- MinIO Client (minio-py)
- boto3
- s3cmd

## Installation

### Quick Start with Docker

```bash
git clone https://github.com/thenervelab/hippius-s3.git
cd hippius-s3
docker compose up -d
```

The service will be available at `http://localhost:8000` with PostgreSQL, Redis, and IPFS running in containers.

### Production Deployment

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Set up database
createdb hippius_s3
dbmate up

# 3. Configure environment
cp .env.example .env
# Edit .env with your settings

# 4. Run the service
uvicorn hippius_s3.main:app --host 0.0.0.0 --port 8000
```

## Configuration

### Environment Variables

```bash
# Database
DB_HOST=localhost
DB_PORT=5432
DB_USER=postgres
DB_PASSWORD=your_password
DB_NAME=hippius_s3

# Redis (for rate limiting and caching)
REDIS_URL=redis://localhost:6379/0

# IPFS
IPFS_STORE_URL=https://store.hippius.network
IPFS_GET_URL=https://get.hippius.network

# Blockchain
VALIDATOR_REGION=decentralized
```

### Encryption Key Storage

For automatic encryption key management per user, install the Hippius SDK with key storage support:

```bash
pip install hippius_sdk[keystore]
```

This enables PostgreSQL-backed encryption key storage and management. See the [Hippius SDK documentation](https://github.com/thenervelab/hippius-sdk/tree/main/hippius_sdk/db) for setup details.

## Usage Examples

### Using MinIO Client

```python
from minio import Minio
import base64

# Encode your seed phrase
seed_phrase = "your twelve word seed phrase here"
encoded_key = base64.b64encode(seed_phrase.encode('utf-8')).decode('utf-8')

# Create client
client = Minio(
    "s3.hippius.com",  # or localhost:8000
    access_key=encoded_key,
    secret_key=seed_phrase,
    secure=True,
    region="decentralized"
)

# Upload with encryption
client.put_object(
    "my-bucket",
    "encrypted-file.txt",
    data,
    length,
    metadata={"encrypted": "true"}
)
```

### Using AWS CLI

```bash
# Configure AWS CLI
aws configure set aws_access_key_id "base64-encoded-seed-phrase"
aws configure set aws_secret_access_key "your-seed-phrase"
aws configure set default.region "decentralized"
aws configure set default.s3.signature_version "s3v4"

# Upload file with encryption
aws s3 cp file.txt s3://my-bucket/ \
  --endpoint-url https://s3.hippius.com \
  --server-side-encryption AES256
```

## API Reference

The service implements S3-compatible endpoints:

- `GET /` - List buckets
- `PUT /{bucket}` - Create bucket
- `DELETE /{bucket}` - Delete bucket
- `PUT /{bucket}/{key}` - Upload object
- `GET /{bucket}/{key}` - Download object
- `DELETE /{bucket}/{key}` - Delete object
- `POST /{bucket}/{key}?uploads` - Initiate multipart upload
- `PUT /{bucket}/{key}?uploadId=X&partNumber=Y` - Upload part
- `POST /{bucket}/{key}?uploadId=X` - Complete multipart upload

All endpoints support standard S3 headers and return S3-compatible XML responses.

## Architecture

```
Client (MinIO/AWS CLI)
    ↓ [HTTPS + AWS SigV4]
HAProxy/Nginx
    ↓ [HTTP]
Hippius S3 API
    ↓ [s3_publish()]
Hippius SDK
    ↓
IPFS Network + Blockchain
```

### Components

- **FastAPI Application**: Modern async Python API framework
- **PostgreSQL**: Metadata storage with full schema migrations
- **Redis**: Rate limiting and caching layer
- **Hippius SDK**: IPFS and blockchain integration
- **HMAC Middleware**: AWS SigV4 signature verification
- **Credit Middleware**: Account verification for operations
- **Rate Limit Middleware**: Request throttling per seed phrase

## Development

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
ruff format .

# Type checking
mypy hippius_s3
```

## TODO Features

- [ ] **Access Control Lists (ACLs)** - Fine-grained permissions
- [ ] **Pre-signed URLs** - Temporary access without credentials
- [ ] **Object Versioning** - Multiple versions of objects
- [ ] **Lifecycle Management** - Automated object expiration
- [ ] **CORS Configuration** - Cross-origin request handling
- [ ] **Event Notifications** - Webhooks for object operations

## License

See [LICENSE](LICENSE) file.
