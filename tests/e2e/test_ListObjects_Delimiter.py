"""E2E: ListObjectsV2 delimiter + pagination contract over a wide/deep keyspace.

Gate for LS-1 (push the distinct-common-prefix rollup into SQL). The CommonPrefixes / Contents /
KeyCount / IsTruncated / NextContinuationToken semantics are a strict client contract; any SQL
rollup must reproduce them exactly. The existing ListObjects e2e only covers a flat prefix.
"""

from typing import Any
from typing import Callable


def _seed(client: Any, bucket: str) -> None:
    # Several sibling "folders", each with multiple keys, plus a couple of top-level keys.
    keys = []
    for folder in ("logs", "data", "images"):
        for i in range(5):
            keys.append(f"{folder}/{i:02d}.txt")
    keys += ["root-a.txt", "root-b.txt"]
    for k in keys:
        client.put_object(Bucket=bucket, Key=k, Body=b"x", ContentType="application/octet-stream")


def test_delimiter_rolls_up_common_prefixes(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("list-delim")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    _seed(boto3_client, bucket)

    resp = boto3_client.list_objects_v2(Bucket=bucket, Delimiter="/")
    common = {p["Prefix"] for p in resp.get("CommonPrefixes", [])}
    contents = {o["Key"] for o in resp.get("Contents", [])}

    assert common == {"logs/", "data/", "images/"}, "each folder collapses to exactly one CommonPrefix"
    assert contents == {"root-a.txt", "root-b.txt"}, "only top-level keys appear as Contents"
    # KeyCount counts Contents + CommonPrefixes (AWS semantics).
    assert resp["KeyCount"] == len(common) + len(contents)


def test_delimiter_with_prefix_lists_one_level(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("list-delim-prefix")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    _seed(boto3_client, bucket)

    resp = boto3_client.list_objects_v2(Bucket=bucket, Prefix="logs/", Delimiter="/")
    keys = {o["Key"] for o in resp.get("Contents", [])}
    assert keys == {f"logs/{i:02d}.txt" for i in range(5)}
    assert not resp.get("CommonPrefixes"), "a fully-listed folder has no nested CommonPrefixes"


def test_pagination_is_stable_and_complete(
    docker_services: Any,
    boto3_client: Any,
    unique_bucket_name: Callable[[str], str],
    cleanup_buckets: Callable[[str], None],
) -> None:
    bucket = unique_bucket_name("list-paged")
    cleanup_buckets(bucket)
    boto3_client.create_bucket(Bucket=bucket)
    _seed(boto3_client, bucket)  # 17 keys total

    seen: list[str] = []
    token: str | None = None
    pages = 0
    while True:
        kwargs = {"Bucket": bucket, "MaxKeys": 4}
        if token:
            kwargs["ContinuationToken"] = token
        resp = boto3_client.list_objects_v2(**kwargs)
        seen.extend(o["Key"] for o in resp.get("Contents", []))
        pages += 1
        if not resp.get("IsTruncated"):
            break
        token = resp["NextContinuationToken"]
        assert pages < 20, "pagination did not terminate"

    assert sorted(seen) == seen, "keys must come back in sorted order across pages"
    assert len(seen) == len(set(seen)) == 17, "every key returned exactly once across pages"
