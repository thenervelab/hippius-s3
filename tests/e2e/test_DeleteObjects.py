"""E2E tests for DeleteObjects (POST /{bucket}?delete)."""

from typing import Any
from typing import Callable

from botocore.exceptions import ClientError


def _xml_delete_body(keys: list[str], quiet: bool = False) -> str:
    ns = "http://s3.amazonaws.com/doc/2006-03-01/"
    quiet_xml = "<Quiet>true</Quiet>" if quiet else ""
    objects_xml = "".join([f"<Object><Key>{k}</Key></Object>" for k in keys])
    return f"""<?xml version=\"1.0\" encoding=\"UTF-8\"?>
<Delete xmlns=\"{ns}\">{quiet_xml}{objects_xml}</Delete>"""


def test_delete_objects_happy_path(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("del-objects")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)

    # Put two keys
    boto3_client.put_object(Bucket=bucket_name, Key="a.txt", Body=b"aaa", ContentType="text/plain")
    boto3_client.put_object(Bucket=bucket_name, Key="b.txt", Body=b"bbb", ContentType="text/plain")

    # Delete both
    # Use boto3's native delete_objects call
    resp = boto3_client.delete_objects(
        Bucket=bucket_name,
        Delete={
            "Objects": [{"Key": "a.txt"}, {"Key": "b.txt"}],
            "Quiet": False,
        },
    )

    assert "Deleted" in resp
    deleted_keys = sorted([d["Key"] for d in resp.get("Deleted", [])])
    assert deleted_keys == ["a.txt", "b.txt"]
    assert "Errors" not in resp or not resp["Errors"]

    # Confirm deletion
    for key in ["a.txt", "b.txt"]:
        try:
            boto3_client.head_object(Bucket=bucket_name, Key=key)
            raise AssertionError("Expected 404 after delete")
        except ClientError as e:
            assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (404,)


def test_delete_objects_idempotent_and_missing(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("del-objects-idem")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.put_object(Bucket=bucket_name, Key="x.txt", Body=b"x", ContentType="text/plain")

    # First delete existing and non-existing
    resp1 = boto3_client.delete_objects(
        Bucket=bucket_name, Delete={"Objects": [{"Key": "x.txt"}, {"Key": "missing.txt"}], "Quiet": False}
    )
    assert sorted([d["Key"] for d in resp1.get("Deleted", [])]) == ["missing.txt", "x.txt"]
    assert not resp1.get("Errors")

    # Second delete again should still show Deleted for both (idempotent)
    resp2 = boto3_client.delete_objects(
        Bucket=bucket_name, Delete={"Objects": [{"Key": "x.txt"}, {"Key": "missing.txt"}], "Quiet": False}
    )
    assert sorted([d["Key"] for d in resp2.get("Deleted", [])]) == ["missing.txt", "x.txt"]
    assert not resp2.get("Errors")


def test_delete_objects_quiet_mode(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket_name = unique_bucket_name("del-objects-quiet")
    cleanup_buckets(bucket_name)

    boto3_client.create_bucket(Bucket=bucket_name)
    boto3_client.put_object(Bucket=bucket_name, Key="q.txt", Body=b"q", ContentType="text/plain")

    resp = boto3_client.delete_objects(
        Bucket=bucket_name, Delete={"Objects": [{"Key": "q.txt"}, {"Key": "missing.txt"}], "Quiet": True}
    )
    # In quiet mode, boto3's parsing returns potentially empty Deleted list
    assert "Errors" not in resp or not resp["Errors"]

    # Ensure object gone
    try:
        boto3_client.head_object(Bucket=bucket_name, Key="q.txt")
        raise AssertionError("Expected 404")
    except ClientError as e:
        assert e.response["ResponseMetadata"]["HTTPStatusCode"] in (404,)
