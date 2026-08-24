from __future__ import annotations

import logging
from typing import Any

from fastapi import Response

from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.buckets.list_objects_endpoint import DEFAULT_MAX_KEYS
from hippius_s3.api.s3.buckets.list_objects_endpoint import MAX_KEYS_LIMIT
from hippius_s3.api.s3.buckets.list_objects_endpoint import _maybe_url_encode
from hippius_s3.api.s3.buckets.list_objects_endpoint import _prefix_resume
from hippius_s3.api.s3.common import format_s3_timestamp
from hippius_s3.api.s3.common import parse_version_id
from hippius_s3.dependencies import RequestContext
from hippius_s3.utils import get_query
from hippius_s3.xml_helpers import add_subelement
from hippius_s3.xml_helpers import create_element
from hippius_s3.xml_helpers import to_xml_bytes


logger = logging.getLogger(__name__)

S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _invalid_arg(message: str) -> Response:
    return errors.s3_error_response(code="InvalidArgument", message=message, status_code=400)


async def handle_list_object_versions(
    bucket_name: str,
    ctx: RequestContext,
    pool: Any,
    *,
    prefix: str | None,
    key_marker: str | None,
    version_id_marker: str | None,
    max_keys: str | None,
    encoding_type: str | None,
    delimiter: str | None,
) -> Response:
    # FastAPI hands "?prefix=" through as "" — treat as absent.
    prefix = prefix or None
    delimiter = delimiter or None
    key_marker = key_marker or None

    if max_keys is None or max_keys == "":
        target = DEFAULT_MAX_KEYS
    else:
        try:
            target = int(max_keys)
        except ValueError:
            return _invalid_arg("max-keys must be an integer")
        if target < 0:
            return _invalid_arg("max-keys must be non-negative")
        target = min(target, MAX_KEYS_LIMIT)

    # "null" is accepted here for the same reason it is on GET/DELETE: clients echo back the
    # version id AWS reports for pre-versioning objects.
    try:
        version_marker = parse_version_id(version_id_marker)
    except (ValueError, TypeError):
        return _invalid_arg("version-id-marker must be a version id returned by a previous response")

    if encoding_type and encoding_type.lower() != "url":
        return _invalid_arg("encoding-type must be 'url'")

    bucket = await pool.fetchrow(get_query("get_bucket_by_name"), bucket_name)
    if not bucket:
        return errors.s3_error_response(
            code="NoSuchBucket",
            message=f"The specified bucket {bucket_name} does not exist",
            status_code=404,
            BucketName=bucket_name,
        )

    bucket_owner = bucket["main_account_id"] or ctx.main_account_id
    # A bucket that never enabled versioning reports one entry per key, matching AWS's shape for
    # an unversioned bucket, rather than exposing the overwrite history we happen to retain.
    current_only = not bucket["versioning_status"]

    entries, is_truncated, next_key, next_version = await _collect_page(
        pool,
        bucket["bucket_id"],
        prefix=prefix,
        delimiter=delimiter,
        key_marker=key_marker,
        version_marker=version_marker,
        target=target,
        current_only=current_only,
    )

    # Scalars first, then interleaved Version/DeleteMarker, then CommonPrefixes — matching AWS's
    # own sample responses. (Its Response Syntax block orders these differently from its
    # examples; every SDK parses by element name, so the sample order is the safer one to copy.)
    root = create_element("ListVersionsResult", xmlns=S3_NS)
    add_subelement(root, "Name", bucket_name)
    add_subelement(root, "Prefix", _maybe_url_encode(prefix or "", encoding_type))
    add_subelement(root, "KeyMarker", _maybe_url_encode(key_marker or "", encoding_type))
    add_subelement(root, "VersionIdMarker", version_id_marker or "")
    if is_truncated and next_key is not None:
        add_subelement(root, "NextKeyMarker", _maybe_url_encode(next_key, encoding_type))
        add_subelement(root, "NextVersionIdMarker", str(next_version))
    add_subelement(root, "MaxKeys", str(target))
    if delimiter:
        add_subelement(root, "Delimiter", _maybe_url_encode(delimiter, encoding_type))
    add_subelement(root, "IsTruncated", "true" if is_truncated else "false")
    if encoding_type:
        add_subelement(root, "EncodingType", encoding_type)

    common_prefixes: list[str] = []
    for kind, payload in entries:
        if kind == "prefix":
            common_prefixes.append(payload)
            continue
        _append_entry(root, kind, payload, bucket_owner, encoding_type)

    for common_prefix in common_prefixes:
        cp = add_subelement(root, "CommonPrefixes")
        add_subelement(cp, "Prefix", _maybe_url_encode(common_prefix, encoding_type))

    return Response(
        content=to_xml_bytes(root, pretty_print=False),
        media_type="application/xml",
        status_code=200,
    )


def _append_entry(
    root: Any,
    kind: str,
    row: Any,
    bucket_owner: str,
    encoding_type: str | None,
) -> None:
    is_marker = kind == "marker"
    entry = add_subelement(root, "DeleteMarker" if is_marker else "Version")
    add_subelement(entry, "Key", _maybe_url_encode(row["object_key"], encoding_type))
    add_subelement(entry, "VersionId", str(row["object_version"]))
    add_subelement(
        entry,
        "IsLatest",
        "true" if int(row["object_version"]) == int(row["current_object_version"]) else "false",
    )
    add_subelement(entry, "LastModified", format_s3_timestamp(row["last_modified"]))
    # A delete marker holds no data, so AWS omits ETag/Size/StorageClass on it entirely.
    if not is_marker:
        add_subelement(entry, "ETag", f'"{row["md5_hash"] or ""}"')
        add_subelement(entry, "Size", str(row["size_bytes"]))
        add_subelement(entry, "StorageClass", "STANDARD")
    owner = add_subelement(entry, "Owner")
    # Same contract as ListObjects: the console reads Owner.ID as the Arion file hash, so a
    # version that has a plaintext BLAKE3 surfaces it and one that doesn't falls back to the
    # account id. A delete marker has no content, so it always carries the account id — the two
    # listings must not disagree about the same version.
    file_hash = "" if is_marker else (row["body_blake3"] or "")
    add_subelement(owner, "ID", file_hash or bucket_owner)
    add_subelement(owner, "DisplayName", bucket_owner)


async def _collect_page(
    pool: Any,
    bucket_id: Any,
    *,
    prefix: str | None,
    delimiter: str | None,
    key_marker: str | None,
    version_marker: int | None,
    target: int,
    current_only: bool,
) -> tuple[list[tuple[str, Any]], bool, str | None, int | None]:
    """Walk versions in (key asc, version desc) order, rolling delimiter groups into prefixes.

    Returns the page entries, whether more exist, and the (key, version) of the first entry NOT
    returned — which becomes NextKeyMarker/NextVersionIdMarker.

    Each version counts as one result against max-keys, and each common prefix counts as one. When
    a group collapses, the cursor jumps past its whole key range via ``_prefix_resume`` so a folder
    of N objects costs one extra round trip rather than N rows of output.
    """
    if target == 0:
        # AWS returns an empty, non-truncated page for max-keys=0 (degenerate probe).
        return [], False, None, None

    plen = len(prefix or "")
    dlen = len(delimiter or "")
    batch_limit = target + 1
    entries: list[tuple[str, Any]] = []
    seen_prefixes: set[str] = set()

    prefix_upper = _prefix_resume(prefix) if prefix else None
    if prefix_upper == prefix:
        prefix_upper = None

    cursor_key = key_marker
    cursor_version = version_marker

    while True:
        batch = await pool.fetch(
            get_query("list_object_versions"),
            bucket_id,
            prefix,
            cursor_key,
            cursor_version,
            batch_limit,
            prefix_upper,
            current_only,
        )
        if not batch:
            return entries, False, None, None

        for row in batch:
            key = row["object_key"]
            di = key.find(delimiter, plen) if delimiter else -1

            common_prefix = key[: di + dlen] if di != -1 else None
            # A key already folded into an emitted prefix is not a result, so it must not count
            # toward max-keys — check this before the truncation test.
            if common_prefix is not None and common_prefix in seen_prefixes:
                continue

            if len(entries) >= target:
                # The first entry we did NOT return becomes the caller's Next*Marker pair.
                return entries, True, key, int(row["object_version"])

            if common_prefix is not None:
                seen_prefixes.add(common_prefix)
                entries.append(("prefix", common_prefix))
                # Skip the whole collapsed group in one index descent on the next batch.
                cursor_key = _prefix_resume(common_prefix)
                cursor_version = None
                continue

            entries.append(("marker" if row["is_delete_marker"] else "version", row))
            cursor_key = key
            cursor_version = int(row["object_version"]) - 1

        if len(batch) < batch_limit:
            return entries, False, None, None
