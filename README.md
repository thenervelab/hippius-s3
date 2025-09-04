# Hippius S3

A high-performance S3-compatible gateway for Hippius' decentralized IPFS storage network. This service provides AWS S3 API compatibility while storing data on IPFS with built-in authentication, rate limiting, and audit logging.

## Overview

Hippius S3 is a production-ready S3-compatible API that stores data on IPFS while automatically publishing to the Hippius blockchain marketplace. It provides standard S3 operations with HMAC-based authentication, comprehensive middleware stack, and seamless integration with existing S3 clients.

## Features

### ✅ Core S3 Operations
- **Bucket Management**: Create, list, delete, and check bucket existence
- **Object Operations**: Upload, download, list, delete, and copy objects
- **Multipart Uploads**: Complete support for large file uploads with part assembly
- **Metadata**: Custom metadata support with x-amz-meta-* headers
- **Tagging**: Bucket and object tagging with full CRUD operations

### ✅ Security & Authentication
- **HMAC Authentication**: Full HMAC signature verification with seed phrase credentials
- **Frontend/Backend HMAC**: Separate HMAC verification for different endpoints
- **Account Credits**: Automatic credit verification for all operations
- **Rate Limiting**: Configurable per-user rate limiting with Redis backend
- **IP-based Banning**: BanHammer service for IP-based protection
- **Input Validation**: AWS S3 compliance validation middleware

### ✅ Blockchain Integration
- **IPFS Storage**: Automatic file storage and pinning via Hippius SDK
- **Blockchain Publishing**: Files automatically published to Hippius marketplace
- **Transaction Tracking**: Blockchain transaction hashes stored in metadata
- **Decentralized Access**: Files remain accessible via IPFS network

### ✅ Production Features
- **Audit Logging**: Comprehensive audit trails for all operations
- **Performance Profiling**: Optional request profiling with Speedscope integration
- **Multi-tenant**: User-scoped buckets with isolated storage
- **Health Checks**: Built-in health checking for all dependencies
- **CORS Support**: Configurable cross-origin request handling

### ✅ S3 Client Compatibility
Works with standard S3 clients including:
- AWS CLI
- MinIO Client (minio-py)
- boto3
- s3cmd

## Getting Started

### Prerequisites

- Docker and Docker Compose
- Python 3.10+ (for local development)

### Quick Start with Docker

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd hippius-s3
   ```

2. **Create environment configuration**:
   ```bash
   cp .env.example .env
   # Edit .env with your specific values (database URL, HMAC secret, etc.)
   ```

3. **Start all services**:
   ```bash
   docker compose up -d
   ```

4. **Run database migrations**:
   ```bash
   docker compose exec api dbmate up
   ```

   ```bash
docker compose -f docker-compose.prod.yml up -d
   ```

## Configuration

### Required Environment Variables

Create a `.env` file with the required variables. See [`.env.example`](.env.example) for all configuration options and their descriptions.

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
    "http://localhost:8000",
    access_key=encoded_key,
    secret_key=seed_phrase,
    secure=False,
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

aws s3 mb s3://my-bucket/ --endpoint-url http://localhost:8000 --no-verify-ssl
aws s3 cp file.txt s3://my-bucket/ --endpoint-url http://localhost:8000 --no-verify-ssl
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
- [ ] **Lifecycle Management** - Automated object expiration

## License

See [LICENSE](LICENSE) file.
