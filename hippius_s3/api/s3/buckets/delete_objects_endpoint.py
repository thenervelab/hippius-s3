from __future__ import annotations

import logging
from typing import Any
from typing import NamedTuple

from fastapi import Request
from fastapi import Response
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.objects.delete_object_endpoint import delete_object_version
from hippius_s3.api.s3.objects.delete_object_endpoint import enqueue_object_unpin
from hippius_s3.api.s3.objects.delete_object_endpoint import insert_delete_marker
from hippius_s3.api.s3.objects.delete_object_endpoint import parse_version_id_or_error
from hippius_s3.config import get_config
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.utils import get_query
from hippius_s3.xml_helpers import parse_untrusted_xml


logger = logging.getLogger(__name__)
config = get_config()


class _DeletedEntry(NamedTuple):
    """One <Deleted> row. AWS splits the two ids: a version that was removed reports VersionId, a
    delete marker that was created or removed reports DeleteMarkerVersionId, and removing a marker
    reports both (with the same value). `DeleteMarker` is emitted iff the second id is set."""

    key: str
    version_id: str | None
    delete_marker_version_id: str | None


def parse_delete_request(root: Any) -> tuple[bool, list[tuple[str, str]]]:
    """Parse a DeleteObjects body into (quiet, [(key, version_id), ...]).

    S3 clients disagree on whether to namespace this body: botocore/aws-cli send
    xmlns="http://s3.amazonaws.com/doc/2006-03-01/", minio-go (mc, and anything
    built on it) sends bare elements. Real S3 accepts both, so match on
    local-name() instead of a namespace-qualified path — a namespace-qualified
    XPath silently yields zero <Object> nodes for the bare form, which turns a
    delete into a 200 OK that deletes nothing.
    """
    quiet_nodes = root.xpath("./*[local-name()='Quiet']")
    quiet = bool(quiet_nodes) and str(quiet_nodes[0].text or "").strip().lower() == "true"

    objects: list[tuple[str, str]] = []
    for obj in root.xpath(".//*[local-name()='Object']"):
        key_nodes = obj.xpath("./*[local-name()='Key']")
        version_nodes = obj.xpath("./*[local-name()='VersionId']")
        key = str(key_nodes[0].text) if key_nodes and key_nodes[0].text else ""
        version_id = str(version_nodes[0].text) if version_nodes and version_nodes[0].text else ""
        objects.append((key, version_id))

    return quiet, objects


async def handle_delete_objects(bucket_name: str, request: Request, db: Any, redis_client: Any) -> Response:
    """Implements S3 DeleteObjects: POST /{bucket}?delete

    - Accepts XML body with up to 1000 <Object><Key>...</Key></Object> entries
    - "Quiet" flag suppresses <Deleted> entries when true
    - Non-existent keys are treated as successfully deleted (idempotent)
    - A per-key VersionId deletes exactly that version; without one, a versioning-enabled bucket
      gets a delete marker and any other bucket is soft-deleted whole
    """
    try:
        # AuthN/AuthZ context
        user = await UserRepository(db).ensure_by_main_account(request.state.main_account_id)
        bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, user["main_account_id"])
        if not bucket:
            return errors.s3_error_response(
                code="NoSuchBucket",
                message=f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        # Parse XML body
        body = await request.body()
        if not body:
            return errors.s3_error_response(
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema.",
                status_code=400,
            )

        try:
            root = parse_untrusted_xml(body)
        except ValueError:
            logger.exception("Malformed XML for DeleteObjects")
            return errors.s3_error_response(
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema.",
                status_code=400,
            )

        # Quiet flag + collect objects
        quiet, object_entries = parse_delete_request(root)
        if len(object_entries) > 1000:
            return errors.s3_error_response(
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema.",
                status_code=400,
            )

        bucket_id = bucket["bucket_id"]
        deleted_keys: list[_DeletedEntry] = []
        errors_list: list[dict[str, str]] = []

        ray_id = getattr(request.state, "ray_id", None)
        versioning_enabled = bucket.get("versioning_status") == "Enabled"

        for key, raw_version in object_entries:
            if not key:
                # Skip invalid entries
                errors_list.append({"Key": "", "Code": "MalformedXML", "Message": "Invalid Delete Object entry"})
                continue

            version_id, invalid = parse_version_id_or_error(raw_version or None)
            if invalid is not None:
                errors_list.append(
                    {"Key": key, "Code": "InvalidArgument", "Message": f"Invalid version ID: {raw_version}"}
                )
                continue

            # Per-key isolation: one bad key must yield one <Error> entry, not fail the whole
            # 1000-key batch. The plain soft-delete below already worked this way.
            try:
                if version_id is not None:
                    resp = await delete_object_version(bucket_id, key, version_id, request, db)
                elif versioning_enabled:
                    resp = await insert_delete_marker(bucket_id, key, db)
            except Exception:
                logger.exception("Delete failed for key %s", key)
                errors_list.append({"Key": key, "Code": "InternalError", "Message": "Failed to delete"})
                continue

            if version_id is not None:
                removed_a_marker = resp.headers.get("x-amz-delete-marker") == "true"
                deleted_keys.append(
                    _DeletedEntry(
                        key=key,
                        version_id=str(version_id),
                        # Removing a marker reports the SAME id in both fields, per AWS.
                        delete_marker_version_id=str(version_id) if removed_a_marker else None,
                    )
                )
                continue

            if versioning_enabled:
                deleted_keys.append(
                    _DeletedEntry(
                        key=key,
                        # A created marker reports only DeleteMarkerVersionId — AWS emits no
                        # VersionId here, and clients read the marker's id from that field to
                        # undo the delete later.
                        version_id=None,
                        delete_marker_version_id=resp.headers.get("x-amz-version-id"),
                    )
                )
                continue

            # Soft-delete the object
            try:
                deleted = await db.fetchrow(
                    get_query("soft_delete_object"),
                    bucket_id,
                    key,
                )
            except Exception:
                logger.exception("Soft-delete query failed for key %s", key)
                deleted = None

            if deleted:
                # NULL version = every version — see enqueue_object_unpin for why the fan-out is
                # deferred to the unpinner rather than expanded here (this loop runs up to 1000
                # times per request).
                await enqueue_object_unpin(
                    db,
                    object_id=str(deleted["object_id"]),
                    object_version=None,
                    address=request.state.main_account_id,
                    ray_id=ray_id,
                )

            # S3 semantics: even if not found, include as Deleted (unless Quiet)
            deleted_keys.append(_DeletedEntry(key=key, version_id=None, delete_marker_version_id=None))

        # Build XML response
        resp_root = ET.Element(
            "DeleteResult",
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/",
        )

        if not quiet:
            for entry in deleted_keys:
                d = ET.SubElement(resp_root, "Deleted")
                ET.SubElement(d, "Key").text = entry.key
                if entry.version_id:
                    ET.SubElement(d, "VersionId").text = entry.version_id
                if entry.delete_marker_version_id:
                    ET.SubElement(d, "DeleteMarker").text = "true"
                    ET.SubElement(d, "DeleteMarkerVersionId").text = entry.delete_marker_version_id

        for err in errors_list:
            e = ET.SubElement(resp_root, "Error")
            ET.SubElement(e, "Key").text = err.get("Key", "")
            ET.SubElement(e, "Code").text = err.get("Code", "")
            ET.SubElement(e, "Message").text = err.get("Message", "")

        xml_content = ET.tostring(resp_root, encoding="UTF-8", xml_declaration=True, pretty_print=False)
        return Response(content=xml_content, media_type="application/xml", status_code=200)

    except Exception:
        logger.exception("Error in DeleteObjects")
        return errors.s3_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )
