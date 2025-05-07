# hippius-s3

S3 Gateway for Hippius' IPFS storage

Hippius-S3 is an S3-compatible API gateway that allows storing data in IPFS (InterPlanetary File System) while presenting a standard S3 interface to clients. This allows applications that work with Amazon S3 to seamlessly store their data on IPFS instead.

## Current Status

### Implemented Features
- **Bucket Operations**:
  - Create, list, and delete buckets
  - Bucket tags support
  - Bucket location queries

- **Object Operations**:
  - Upload and download objects
  - Object metadata handling
  - List objects in buckets with prefix filtering
  - Delete objects

- **Multipart Uploads**:
  - Initiate multipart uploads
  - Upload parts with proper ETag generation
  - Complete multipart uploads with part assembly
  - Abort uploads with proper cleanup
  - List ongoing multipart uploads

- **Core Components**:
  - FastAPI application with middleware and database connection pooling
  - PostgreSQL database integration with SQL queries and migrations
  - IPFS integration via Hippius SDK
  - S3-compatible XML response formatting
  - Configuration management with environment variables

### TODO: Missing Features
- [ ] **Pre-signed URLs**: Implementation of pre-signed URL generation and validation
- [ ] **Access Control Lists (ACLs)**: Fine-grained permission control for buckets and objects
- [ ] **Server-Side Encryption**: Support for encryption headers and parameters
- [ ] **Bucket Lifecycle Management**: Implementation of object expiration and transitions
- [ ] **Object Versioning**: Support for multiple versions of objects
- [ ] **Website Hosting**: Static website hosting capabilities
- [ ] **Cross-Origin Resource Sharing (CORS)**: Bucket-specific CORS configuration
- [ ] **Event Notifications**: Triggers for object operations
- [ ] **Testing**: Comprehensive test suite
- [ ] **Documentation**: OpenAPI/Swagger integration

## Setup

This project uses modern Python tooling:

- [uv](https://github.com/astral-sh/uv) for fast dependency management
- [ruff](https://github.com/astral-sh/ruff) for linting and formatting
- [mypy](https://github.com/python/mypy) for static type checking
- [pytest](https://github.com/pytest-dev/pytest) for testing
- [dbmate](https://github.com/amacneil/dbmate) for database migrations

### Installation

#### Local Development

```bash
# Install with development dependencies
uv pip install -e ".[dev]"

# Install dbmate (macOS)
brew install dbmate

# Or install dbmate (Linux)
curl -fsSL -o ~/bin/dbmate https://github.com/amacneil/dbmate/releases/latest/download/dbmate-linux-amd64
chmod +x ~/bin/dbmate

# Initialize the database (requires PostgreSQL running)
dbmate up
```

#### Docker

The project includes a Docker setup with PostgreSQL, IPFS node, and the FastAPI application:

```bash
# Start all services
docker-compose up

# Start in background
docker-compose up -d

# View logs
docker-compose logs -f

# Stop services
docker-compose down
```

The Docker setup automatically:
- Installs dbmate in the API container
- Applies database migrations on startup
- Connects to PostgreSQL and IPFS services

### Development

```bash
# Run the FastAPI application
uvicorn hippius_s3.main:app --reload

# Database operations
dbmate up     # Apply all pending migrations
dbmate new migration_name  # Create a new migration file

# Run tests
pytest

# Run linter
ruff check .

# Format code
ruff format .

# Type check
mypy hippius_s3 tests
```

## Project Structure

```
hippius_s3/
├── api/                # API endpoints organized by domain
│   └── s3/             # S3 gateway endpoints
│       ├── endpoints.py # Basic S3 operations
│       ├── multipart.py # Multipart upload support
│       └── schemas.py   # API schemas
├── sql/
│   ├── migrations/     # Database migrations (managed by dbmate)
│   └── queries/        # SQL query files
├── config.py           # Application configuration
├── dependencies.py     # FastAPI dependency injection
├── ipfs_service.py     # IPFS integration service
├── main.py             # Application entry point
└── utils.py            # Utility functions
```

### Implementation Details

- **Database Schema**: PostgreSQL database with tables for buckets, objects, multipart uploads, and parts
- **IPFS Integration**: Uses the Hippius SDK to interact with IPFS for storing and retrieving data
- **S3 Compatibility**: Implements XML response formatting for S3 clients with proper error codes
- **API Structure**: FastAPI-based endpoints with proper response models and validation
- **Configuration**: Environment variable-based configuration with sane defaults

## API Endpoints

### S3 Core Operations

- `POST /s3/buckets` - Create a new bucket
- `GET /s3/buckets` - List all buckets
- `DELETE /s3/buckets/{bucket_name}` - Delete a bucket
- `POST /s3/objects` - Upload an object to a bucket
- `GET /s3/objects/{bucket_name}` - List objects in a bucket
- `GET /s3/objects/{bucket_name}/{object_key}` - Get object metadata
- `GET /s3/download/{bucket_name}/{object_key}` - Download an object
- `DELETE /s3/objects/{bucket_name}/{object_key}` - Delete an object

### Multipart Uploads

- `POST /s3/multipart/uploads` - Create a new multipart upload
- `POST /s3/multipart/uploads/{upload_id}/parts/{part_number}` - Upload a part
- `GET /s3/multipart/uploads/{upload_id}/parts` - List parts for a multipart upload
- `POST /s3/multipart/uploads/{upload_id}/complete` - Complete a multipart upload
- `DELETE /s3/multipart/uploads/{upload_id}` - Abort a multipart upload

### Pre-signed URLs (Not Yet Implemented)

- [ ] `POST /s3/presigned/url` - Generate a pre-signed URL for temporary access
- [ ] `GET /s3/presigned/validate` - Validate a pre-signed URL signature (internal only)

## Usage Examples

### Creating a bucket

```bash
curl -X POST "http://localhost:8000/s3/buckets" \
  -H "Content-Type: application/json" \
  -d '{"bucket_name": "my-bucket", "is_public": false}'
```

### Uploading an object

```bash
curl -X POST "http://localhost:8000/s3/objects" \
  -H "Content-Type: application/json" \
  -d '{
    "bucket_name": "my-bucket",
    "object_key": "hello.txt",
    "content_type": "text/plain",
    "file_size": 14,
    "file_data": "SGVsbG8sIHdvcmxkIQo=",
    "metadata": {"author": "John Doe"}
  }'
```

### Downloading an object

```bash
curl -X GET "http://localhost:8000/s3/download/my-bucket/hello.txt" -o downloaded_file.txt
```

### Using multipart uploads for large files

1. Create a multipart upload:
```bash
curl -X POST "http://localhost:8000/s3/multipart/uploads" \
  -H "Content-Type: application/json" \
  -d '{
    "bucket_name": "my-bucket",
    "object_key": "large-file.zip",
    "content_type": "application/zip"
  }'
```

2. Upload parts (repeat for each part):
```bash
curl -X POST "http://localhost:8000/s3/multipart/uploads/{upload_id}/parts/1" \
  -H "Content-Type: application/json" \
  -d '{
    "part_number": 1,
    "file_size": 1024000,
    "file_data": "base64-encoded-part-data"
  }'
```

3. Complete the multipart upload:
```bash
curl -X POST "http://localhost:8000/s3/multipart/uploads/{upload_id}/complete" \
  -H "Content-Type: application/json" \
  -d '{
    "parts": [
      {"PartNumber": 1, "ETag": "etag1"},
      {"PartNumber": 2, "ETag": "etag2"}
    ]
  }'
```

### Generating a pre-signed URL (Not Yet Implemented)

```bash
# This functionality is not yet implemented
curl -X POST "http://localhost:8000/s3/presigned/url" \
  -H "Content-Type: application/json" \
  -d '{
    "bucket_name": "my-bucket",
    "object_key": "hello.txt",
    "expiration": 3600,
    "http_method": "GET"
  }'
```

## Roadmap

The following features are planned for future development:

1. Implement pre-signed URLs functionality for secure temporary access to objects
2. Add Access Control Lists (ACLs) for fine-grained permission control
3. Implement server-side encryption options
4. Add support for bucket lifecycle management
5. Develop a comprehensive test suite with high coverage
6. Integrate OpenAPI/Swagger documentation
7. Add object versioning support
8. Implement CORS configuration for buckets
9. Add event notifications for object operations
10. Implement static website hosting capabilities

## Compatibility with S3 Clients

This project aims to be compatible with common S3 clients like:

- [AWS CLI](https://aws.amazon.com/cli/)
- [MinIO Client](https://docs.min.io/docs/minio-client-complete-guide.html)
- [s3cmd](https://s3tools.org/s3cmd)
- [boto3](https://boto3.amazonaws.com/v1/documentation/api/latest/index.html) (Python AWS SDK)

The `/examples` directory contains sample code for interacting with the S3 API using these clients.

## License

See [LICENSE](LICENSE) file.
