from datetime import datetime
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from pydantic import BaseModel
from pydantic import Field


class BucketCreate(BaseModel):
    """Bucket creation request schema."""

    bucket_name: str = Field(..., description="Name of the bucket")
    is_public: bool = Field(False, description="Whether the bucket is publicly accessible")


class Bucket(BaseModel):
    """Bucket schema."""

    bucket_id: str
    bucket_name: str
    created_at: datetime
    is_public: bool


class FileUploadRequest(BaseModel):
    """File upload request schema."""

    bucket_name: str = Field(..., description="Name of the bucket")
    object_key: str = Field(..., description="Object key (path within bucket)")
    content_type: str = Field(..., description="MIME type of the file")
    file_size: int = Field(..., description="Size of the file in bytes")
    file_data: str = Field(..., description="Base64 encoded file data")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Optional metadata for the file")


class ObjectInfo(BaseModel):
    """Object information schema."""

    object_id: str
    bucket_id: str
    bucket_name: str
    object_key: str
    ipfs_cid: str
    size_bytes: int
    content_type: str
    created_at: datetime
    metadata: Optional[Dict[str, Any]] = None


class ListObjectsResponse(BaseModel):
    """List objects response schema."""

    objects: List[ObjectInfo]
    bucket_name: str


class DeleteObjectResponse(BaseModel):
    """Delete object response schema."""

    deleted: bool
    object_key: str


class DeleteBucketResponse(BaseModel):
    """Delete bucket response schema."""

    deleted: bool
    bucket_name: str
