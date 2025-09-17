from __future__ import annotations

from fastapi import Response


async def handle_get_bucket_location(bucket_name: str) -> Response:
    """
    Get the region/location of a bucket (GET /{bucket_name}?location).

    This is used by the MinIO client to determine the bucket's region.
    """
    # Always return a valid XML response for bucket location queries
    # Even for buckets that don't exist - this is what Minio client expects
    # For S3 compatibility, we use us-east-1 as the default region
    try:
        # This is what most S3 clients expect for the LocationConstraint response
        # Create a hardcoded response to ensure it's correctly formatted
        xml = '<?xml version="1.0" encoding="UTF-8"?>'
        xml += '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>'

        return Response(
            content=xml.encode("utf-8"),
            media_type="application/xml",
            headers={"Content-Type": "application/xml"},
        )
    except Exception:
        # Even on error, return a valid XML response
        xml = '<?xml version="1.0" encoding="UTF-8"?>'
        xml += '<LocationConstraint xmlns="http://s3.amazonaws.com/doc/2006-03-01/">us-east-1</LocationConstraint>'

        return Response(
            content=xml.encode("utf-8"),
            media_type="application/xml",
            headers={"Content-Type": "application/xml"},
        )
