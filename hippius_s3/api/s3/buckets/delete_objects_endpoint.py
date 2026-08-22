from __future__ import annotations

import logging
from typing import Any

from fastapi import Request
from fastapi import Response
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.objects.delete_object_endpoint import delete_object_version
from hippius_s3.api.s3.objects.delete_object_endpoint import enqueue_unpins_for_versions
from hippius_s3.api.s3.objects.delete_object_endpoint import insert_delete_marker
from hippius_s3.api.s3.objects.delete_object_endpoint import parse_version_id
from hippius_s3.config import get_config
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.utils import get_query
from hippius_s3.xml_helpers import parse_untrusted_xml


logger = logging.getLogger(__name__)
config = get_config()


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
        # Each entry is a key, the version id to report back (None when unversioned), and whether
        # the delete produced a marker.
        deleted_keys: list[tuple[str, str | None, bool]] = []
        errors_list: list[dict[str, str]] = []

        ray_id = getattr(request.state, "ray_id", None)
        versioning_enabled = bucket.get("versioning_status") == "Enabled"

        for key, raw_version in object_entries:
            if not key:
                # Skip invalid entries
                errors_list.append({"Key": "", "Code": "MalformedXML", "Message": "Invalid Delete Object entry"})
                continue

            version_id, invalid = parse_version_id(raw_version or None)
            if invalid is not None:
                errors_list.append(
                    {"Key": key, "Code": "InvalidArgument", "Message": f"Invalid version ID: {raw_version}"}
                )
                continue

            if version_id is not None:
                resp = await delete_object_version(bucket_id, key, version_id, request, db)
                deleted_keys.append((key, str(version_id), resp.headers.get("x-amz-delete-marker") == "true"))
                continue

            if versioning_enabled:
                resp = await insert_delete_marker(bucket_id, key, db)
                deleted_keys.append((key, resp.headers.get("x-amz-version-id"), True))
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
                object_id = str(deleted["object_id"])
                # Every version holding a backend copy, not just the current one — see
                # enqueue_unpins_for_versions for why this is resolved here rather than deferred.
                rows = await db.fetch(get_query("list_object_versions_for_unpin"), object_id)
                versions = [int(r["object_version"]) for r in rows] or [int(deleted["current_object_version"])]
                await enqueue_unpins_for_versions(
                    db,
                    object_id=object_id,
                    versions=versions,
                    address=request.state.main_account_id,
                    ray_id=ray_id,
                )

            # S3 semantics: even if not found, include as Deleted (unless Quiet)
            deleted_keys.append((key, None, False))

        # Build XML response
        resp_root = ET.Element(
            "DeleteResult",
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/",
        )

        if not quiet:
            for key, version_id, was_marker in deleted_keys:
                d = ET.SubElement(resp_root, "Deleted")
                ET.SubElement(d, "Key").text = key
                if version_id:
                    ET.SubElement(d, "VersionId").text = version_id
                if was_marker:
                    ET.SubElement(d, "DeleteMarker").text = "true"

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
