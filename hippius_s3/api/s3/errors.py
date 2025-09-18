"""Standard error responses for S3 API."""

import uuid

from fastapi import Response
from lxml import etree as ET


class S3Error(Exception):
    """Exception class for S3-specific errors."""

    def __init__(self, code: str, status_code: int = 400, message: str = ""):
        self.code = code
        self.status_code = status_code

        # Use a default message based on the error code if none provided
        if not message:
            if code == "NoSuchBucket":
                message = "The specified bucket does not exist"
            elif code == "NoSuchKey":
                message = "The specified key does not exist"
            else:
                message = f"S3 Error: {code}"

        self.message = message
        super().__init__(message)


def s3_error_response(
    code: str,
    message: str,
    request_id: str = "",
    status_code: int = 400,
    *,
    extra_headers: dict[str, str] | None = None,
    **kwargs: str,
) -> Response:
    """Generate a standardized S3 error response in XML format.

    Args:
        code: The S3 error code (e.g., "NoSuchBucket", "InvalidRequest")
        message: The error message
        request_id: A unique ID for the request (generated if not provided)
        status_code: The HTTP status code
        **kwargs: Additional key-value pairs to include in the error response

    Returns:
        A properly formatted S3 XML error response
    """
    # Generate request ID if not provided
    if request_id == "":
        request_id = str(uuid.uuid4())

    # Create XML using lxml. Avoid XML namespace for maximum boto3 compatibility
    # (boto3 error parser expects <Error><Code>...</Code>...</Error> without a namespace)
    root = ET.Element("Error")

    code_elem = ET.SubElement(root, "Code")
    code_elem.text = code

    message_elem = ET.SubElement(root, "Message")
    message_elem.text = message

    req_id_elem = ET.SubElement(root, "RequestId")
    req_id_elem.text = request_id

    host_id_elem = ET.SubElement(root, "HostId")
    host_id_elem.text = "hippius-s3"

    # Add any additional elements
    for key, value in kwargs.items():
        elem = ET.SubElement(root, key)
        elem.text = str(value)

    # Generate proper XML with declaration
    xml_content = ET.tostring(
        root,
        encoding="UTF-8",
        xml_declaration=True,
        pretty_print=True,
    )

    # Set standard S3 headers for error responses
    headers = {
        "Content-Type": "application/xml; charset=utf-8",
        "x-amz-request-id": request_id,
        "Content-Length": str(len(xml_content)),
        # Help SDKs parse error type/message even when they don't parse XML body
        "x-amz-error-code": code,
        "x-amz-error-message": message,
    }

    if extra_headers:
        headers.update({k: str(v) for k, v in extra_headers.items()})

    return Response(content=xml_content, media_type="application/xml", status_code=status_code, headers=headers)
