"""S3-compatible error responses for the gateway."""

from html import escape

from fastapi import Response


def s3_error_response(
    code: str,
    message: str,
    status_code: int,
    **kwargs: str,
) -> Response:
    """
    Generate an S3-compatible XML error response.

    Args:
        code: S3 error code (e.g., "AccessDenied", "InvalidAccessKeyId")
        message: Human-readable error message
        status_code: HTTP status code
        **kwargs: Additional fields to include in the error XML (e.g., BucketName, Key)

    Returns:
        FastAPI Response with XML error content
    """
    xml_parts = [
        '<?xml version="1.0" encoding="UTF-8"?>',
        "<Error>",
        f"  <Code>{escape(code)}</Code>",
        f"  <Message>{escape(message)}</Message>",
    ]

    for key, value in kwargs.items():
        if value:
            xml_parts.append(f"  <{escape(key)}>{escape(value)}</{escape(key)}>")

    xml_parts.append("</Error>")

    xml_content = "\n".join(xml_parts)

    return Response(
        content=xml_content,
        status_code=status_code,
        media_type="application/xml",
    )
