from __future__ import annotations

import contextlib
import json
import logging
from typing import Any

from fastapi import Request
from fastapi import Response
from lxml import etree as ET

from hippius_s3.api.s3 import errors
from hippius_s3.repositories.buckets import BucketRepository
from hippius_s3.repositories.objects import ObjectRepository
from hippius_s3.repositories.users import UserRepository
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


async def get_object_tags(
    bucket_name: str,
    object_key: str,
    db: Any,
    _: str,
    main_account_id: str,
) -> Response:
    try:
        await UserRepository(db).ensure_by_main_account(main_account_id)
        bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, main_account_id)
        if not bucket:
            return errors.s3_error_response(
                code="NoSuchBucket",
                message=f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        try:
            object_info = await ObjectRepository(db).get_by_path(bucket["bucket_id"], object_key)
        except Exception:
            object_info = None
        if not object_info:
            return errors.s3_error_response(
                code="NoSuchKey",
                message=f"The specified key {object_key} does not exist",
                status_code=404,
                Key=object_key,
            )
        # Normalize record to dict for safe .get access
        if not isinstance(object_info, dict):
            with contextlib.suppress(Exception):
                object_info = dict(object_info)
        metadata = object_info.get("metadata", {})
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        tags = metadata.get("tags", {})
        if not isinstance(tags, dict):
            tags = {}

        root = ET.Element("Tagging", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
        tag_set = ET.SubElement(root, "TagSet")
        for key, value in tags.items():
            tag = ET.SubElement(tag_set, "Tag")
            ET.SubElement(tag, "Key").text = key
            ET.SubElement(tag, "Value").text = str(value)

        xml_content = ET.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)
        return Response(content=xml_content, media_type="application/xml")

    except Exception:
        logger.exception("Error getting object tags")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )


async def set_object_tags(
    bucket_name: str,
    object_key: str,
    request: Request,
    db: Any,
    _: str,
    main_account_id: str,
) -> Response:
    try:
        await UserRepository(db).ensure_by_main_account(main_account_id)
        bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, main_account_id)
        if not bucket:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        try:
            object_info = await ObjectRepository(db).get_by_path(bucket["bucket_id"], object_key)
        except Exception:
            object_info = None
        if not object_info:
            return errors.s3_error_response(
                "NoSuchKey",
                f"The specified key {object_key} does not exist",
                status_code=404,
                Key=object_key,
            )
        if not isinstance(object_info, dict):
            with contextlib.suppress(Exception):
                object_info = dict(object_info)
        xml_data = await request.body()
        if not xml_data:
            tag_dict: dict[str, str] = {}
        else:
            try:
                root = ET.fromstring(xml_data)
                ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
                tag_dict = {}
                for tag_elem in root.xpath(".//s3:Tag", namespaces=ns):  # type: ignore[attr-defined]
                    key_nodes = tag_elem.xpath("./s3:Key", namespaces=ns)  # type: ignore[attr-defined]
                    value_nodes = tag_elem.xpath("./s3:Value", namespaces=ns)  # type: ignore[attr-defined]
                    if key_nodes and value_nodes and key_nodes[0].text and value_nodes[0].text:
                        tag_dict[str(key_nodes[0].text)] = str(value_nodes[0].text)
            except Exception:
                logger.exception("Error parsing XML tags")
                return errors.s3_error_response(
                    "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    status_code=400,
                )

        metadata = object_info.get("metadata", {})
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        metadata["tags"] = tag_dict

        object_id = object_info["object_id"]
        object_version = object_info.get("object_version") or object_info.get("ov.object_version")
        await db.fetchrow(get_query("update_object_metadata"), json.dumps(metadata), object_id, int(object_version))
        return Response(status_code=200)

    except Exception:
        logger.exception("Error setting object tags")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )


async def delete_object_tags(
    bucket_name: str,
    object_key: str,
    db: Any,
    _: str,
    main_account_id: str,
) -> Response:
    try:
        user = await UserRepository(db).ensure_by_main_account(main_account_id)
        bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, user["main_account_id"])
        if not bucket:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        try:
            object_info = await ObjectRepository(db).get_by_path(bucket["bucket_id"], object_key)
        except Exception:
            object_info = None
        if not object_info:
            return errors.s3_error_response(
                "NoSuchKey",
                f"The specified key {object_key} does not exist",
                status_code=404,
                Key=object_key,
            )
        if not isinstance(object_info, dict):
            with contextlib.suppress(Exception):
                object_info = dict(object_info)
        metadata = object_info.get("metadata", {})
        if isinstance(metadata, str):
            metadata = json.loads(metadata)
        if "tags" in metadata:
            metadata["tags"] = {}

        object_id = object_info["object_id"]
        object_version = object_info.get("object_version") or object_info.get("ov.object_version")
        await db.fetchrow(get_query("update_object_metadata"), json.dumps(metadata), object_id, int(object_version))
        return Response(status_code=204)

    except Exception:
        logger.exception("Error deleting object tags")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )
