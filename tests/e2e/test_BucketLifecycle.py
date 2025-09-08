"""E2E tests for Bucket Lifecycle using boto3 where possible.

Local server uses path-style + SigV4 via our custom setup; boto3 will handle signing.
"""

from typing import Any
from typing import Callable

import pytest
from botocore.awsrequest import AWSRequest  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]


def test_get_bucket_lifecycle_not_configured_returns_404(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("lifecycle-get")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    # Use boto3; AWS and our server both raise NoSuchLifecycleConfiguration when not set
    from botocore.exceptions import ClientError

    with pytest.raises(ClientError) as excinfo:
        boto3_client.get_bucket_lifecycle_configuration(Bucket=bucket_name)
    code = excinfo.value.response.get("Error", {}).get("Code") or excinfo.value.response.get("Code")
    assert code == "NoSuchLifecycleConfiguration"


def test_put_bucket_lifecycle_accepts_configuration(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("lifecycle-put")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    # PUT lifecycle via boto3 (it will serialize to XML)
    lc = {
        "Rules": [
            {
                "ID": "default-rule",
                "Status": "Enabled",
                "Filter": {"Prefix": "tmp/"},
                "Expiration": {"Days": 7},
            }
        ]
    }
    resp = boto3_client.put_bucket_lifecycle_configuration(Bucket=bucket_name, LifecycleConfiguration=lc)
    status = resp.get("ResponseMetadata", {}).get("HTTPStatusCode")
    assert status in (200, 204)


def test_put_bucket_lifecycle_malformed_xml(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("lifecycle-malformed")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    # Use a botocore before-sign hook to replace the serialized body with malformed XML
    bad_xml = "<LifecycleConfiguration><Rule><ID>no-end"

    def _malform_body(request: AWSRequest, **kwargs: Any) -> None:
        request.headers["Content-Type"] = "application/xml"
        request.data = bad_xml
        request.headers["Content-Length"] = str(len(bad_xml))

    boto3_client.meta.events.register(
        "before-sign.s3.PutBucketLifecycleConfiguration",
        _malform_body,
    )

    try:
        with pytest.raises(ClientError) as excinfo:
            # Provide any valid shape; the hook will replace the body after serialization
            boto3_client.put_bucket_lifecycle_configuration(
                Bucket=bucket_name,
                LifecycleConfiguration={
                    "Rules": [
                        {
                            "ID": "x",
                            "Status": "Enabled",
                            "Filter": {"Prefix": ""},
                            "Expiration": {"Days": 1},
                        }
                    ]
                },
            )
        assert excinfo.value.response.get("ResponseMetadata", {}).get("HTTPStatusCode") == 400
    finally:
        boto3_client.meta.events.unregister(
            "before-sign.s3.PutBucketLifecycleConfiguration",
            _malform_body,
        )
