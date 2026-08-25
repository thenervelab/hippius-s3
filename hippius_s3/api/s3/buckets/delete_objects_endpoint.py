from __future__ import annotations

import logging
from typing import Any

import asyncpg
from fastapi import Request
from fastapi import Response
from lxml import etree as ET  # ty: ignore[unresolved-import]

from hippius_s3.api.s3 import errors
from hippius_s3.api.s3.object_names import drop_s3_name
from hippius_s3.backend_routing import resolve_object_backends
from hippius_s3.config import get_config
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_unpin_request
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
    - Versioning is not supported: keys with VersionId yield per-key <Error NotImplemented>
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
        deleted_keys: list[str] = []
        errors_list: list[dict[str, str]] = []

        for key, version_id in object_entries:
            if not key:
                # Skip invalid entries
                errors_list.append({"Key": "", "Code": "MalformedXML", "Message": "Invalid Delete Object entry"})
                continue

            if version_id:
                errors_list.append({"Key": key, "Code": "NotImplemented", "Message": "Versioning not supported"})
                continue

            try:
                kind = await drop_s3_name(db, str(bucket_id), key)
            except asyncpg.UniqueViolationError:
                logger.exception("drop_s3_name name conflict for key %s", key)
                errors_list.append(
                    {
                        "Key": key,
                        "Code": "InternalError",
                        "Message": "Name conflict while deleting",
                    }
                )
                continue
            except Exception:
                logger.exception("drop_s3_name failed for key %s", key)
                errors_list.append(
                    {
                        "Key": key,
                        "Code": "InternalError",
                        "Message": "Delete failed",
                    }
                )
                continue

            if kind in {"alias", "promoted"}:
                deleted_keys.append(key)
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
                ray_id = getattr(request.state, "ray_id", None)
                object_id = str(deleted["object_id"])
                object_version = int(deleted["current_object_version"])
                db_backends = await resolve_object_backends(db, object_id, object_version)
                unpin_payload = UnpinChainRequest(
                    address=request.state.main_account_id,
                    object_id=object_id,
                    object_version=object_version,
                    ray_id=ray_id,
                    delete_backends=db_backends if db_backends else None,
                )
                await enqueue_unpin_request(payload=unpin_payload)

            # S3 semantics: even if not found, include as Deleted (unless Quiet)
            deleted_keys.append(key)

        # Build XML response
        resp_root = ET.Element(
            "DeleteResult",
            xmlns="http://s3.amazonaws.com/doc/2006-03-01/",
        )

        if not quiet:
            for key in deleted_keys:
                d = ET.SubElement(resp_root, "Deleted")
                ET.SubElement(d, "Key").text = key

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
