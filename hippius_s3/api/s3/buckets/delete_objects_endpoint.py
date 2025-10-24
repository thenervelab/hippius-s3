from __future__ import annotations

import logging
from typing import Any

from fastapi import Request
from fastapi import Response
from lxml import etree as ET

from hippius_s3.api.s3 import errors
from hippius_s3.config import get_config
from hippius_s3.queue import UnpinChainRequest
from hippius_s3.queue import enqueue_unpin_request
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)
config = get_config()


async def handle_delete_objects(bucket_name: str, request: Request, db: Any, redis_client: Any) -> Response:
    """Implements S3 DeleteObjects: POST /{bucket}?delete

    - Accepts XML body with up to 1000 <Object><Key>...</Key></Object> entries
    - "Quiet" flag suppresses <Deleted> entries when true
    - Non-existent keys are treated as successfully deleted (idempotent)
    - Versioning is not supported: keys with VersionId yield per-key <Error NotImplemented>
    """
    try:
        # AuthN/AuthZ context
        user = await UserRepository(db).ensure_by_main_account(request.state.account.main_account)
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
            root = ET.fromstring(body)
        except Exception:
            logger.exception("Malformed XML for DeleteObjects")
            return errors.s3_error_response(
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema.",
                status_code=400,
            )

        ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}

        # Quiet flag
        quiet_nodes = root.xpath("./s3:Quiet", namespaces=ns)  # type: ignore[attr-defined]
        quiet = False
        if quiet_nodes and quiet_nodes[0].text:
            quiet = str(quiet_nodes[0].text).strip().lower() == "true"

        # Collect objects
        object_elems = root.xpath(".//s3:Object", namespaces=ns)  # type: ignore[attr-defined]
        if len(object_elems) > 1000:
            return errors.s3_error_response(
                "MalformedXML",
                "The XML you provided was not well-formed or did not validate against our published schema.",
                status_code=400,
            )

        bucket_id = bucket["bucket_id"]
        deleted_keys: list[str] = []
        errors_list: list[dict[str, str]] = []

        for obj in object_elems:
            key_nodes = obj.xpath("./s3:Key", namespaces=ns)  # type: ignore[attr-defined]
            version_nodes = obj.xpath("./s3:VersionId", namespaces=ns)  # type: ignore[attr-defined]
            key = str(key_nodes[0].text) if key_nodes and key_nodes[0].text else ""
            version_id = str(version_nodes[0].text) if version_nodes and version_nodes[0].text else ""

            if not key:
                # Skip invalid entries
                errors_list.append({"Key": "", "Code": "MalformedXML", "Message": "Invalid Delete Object entry"})
                continue

            if version_id:
                errors_list.append({"Key": key, "Code": "NotImplemented", "Message": "Versioning not supported"})
                continue

            # Perform permission-aware delete; non-existent counts as success
            try:
                deleted_object = await db.fetchrow(get_query("delete_object"), bucket_id, key, user["main_account_id"])
            except Exception:
                logger.exception("Delete query failed for key %s", key)
                deleted_object = None

            all_cids = deleted_object.get("all_cids") or [] if deleted_object else []
            if all_cids:
                for cid in all_cids:
                    await enqueue_unpin_request(
                        payload=UnpinChainRequest(
                            substrate_url=config.substrate_url,
                            ipfs_node=config.ipfs_store_url,
                            address=request.state.account.main_account,
                            subaccount=request.state.account.main_account,
                            subaccount_seed_phrase=request.state.seed_phrase,
                            bucket_name=bucket_name,
                            object_key=key,
                            should_encrypt=not bucket["is_public"],
                            cid=cid,
                            object_id=str(deleted_object["object_id"])
                            if deleted_object and deleted_object.get("object_id")
                            else "",
                            object_version=int(
                                deleted_object.get("object_version")
                                or deleted_object.get("current_object_version")
                                or 1
                            )
                            if deleted_object
                            else 1,
                        ),
                        redis_client=redis_client,
                    )

            # S3 semantics: even if not found, include as Deleted (unless Quiet)
            deleted_keys.append(key)

        # Build XML response
        resp_root = ET.Element("DeleteResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")

        if not quiet:
            for key in deleted_keys:
                d = ET.SubElement(resp_root, "Deleted")
                ET.SubElement(d, "Key").text = key

        for err in errors_list:
            e = ET.SubElement(resp_root, "Error")
            ET.SubElement(e, "Key").text = err.get("Key", "")
            ET.SubElement(e, "Code").text = err.get("Code", "")
            ET.SubElement(e, "Message").text = err.get("Message", "")

        xml_content = ET.tostring(resp_root, encoding="UTF-8", xml_declaration=True, pretty_print=True)
        return Response(content=xml_content, media_type="application/xml", status_code=200)

    except Exception:
        logger.exception("Error in DeleteObjects")
        return errors.s3_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )
