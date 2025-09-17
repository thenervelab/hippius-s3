from __future__ import annotations

import json
import logging
from typing import Any

from fastapi import Request
from fastapi import Response
from lxml import etree as ET

from hippius_s3.api.s3 import errors
from hippius_s3.repositories.buckets import BucketRepository


logger = logging.getLogger(__name__)


async def get_bucket_tags(bucket_name: str, db: Any, main_account_id: str) -> Response:
    try:
        bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, main_account_id)
        if not bucket:
            return errors.s3_error_response(
                code="NoSuchBucket",
                message=f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        tags = bucket.get("tags", {})
        if isinstance(tags, str):
            try:
                tags = json.loads(tags)
            except Exception:
                tags = {}
        if not tags:
            return errors.s3_error_response(
                code="NoSuchTagSet",
                message="The TagSet does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        root = ET.Element("Tagging", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
        tag_set = ET.SubElement(root, "TagSet")
        for k, v in tags.items():
            tag = ET.SubElement(tag_set, "Tag")
            ET.SubElement(tag, "Key").text = k
            ET.SubElement(tag, "Value").text = str(v)
        xml_content = ET.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)
        return Response(content=xml_content, media_type="application/xml", status_code=200)
    except Exception:
        logger.exception("Error getting bucket tags")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )


async def set_bucket_tags(bucket_name: str, request: Request, db: Any, main_account_id: str) -> Response:
    try:
        bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, main_account_id)
        if not bucket:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )
        body = await request.body()
        tags: dict[str, str] = {}
        if body:
            try:
                root = ET.fromstring(body)
                ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
                for tag_elem in root.xpath(".//s3:Tag", namespaces=ns):  # type: ignore[attr-defined]
                    key_nodes = tag_elem.xpath("./s3:Key", namespaces=ns)  # type: ignore[attr-defined]
                    value_nodes = tag_elem.xpath("./s3:Value", namespaces=ns)  # type: ignore[attr-defined]
                    if key_nodes and value_nodes and key_nodes[0].text and value_nodes[0].text:
                        tags[str(key_nodes[0].text)] = str(value_nodes[0].text)
            except Exception:
                return errors.s3_error_response(
                    "MalformedXML",
                    "The XML you provided was not well-formed or did not validate against our published schema.",
                    status_code=400,
                )
        await db.fetchrow("UPDATE buckets SET tags = $1 WHERE bucket_id = $2", json.dumps(tags), bucket["bucket_id"])
        return Response(status_code=200)
    except Exception:
        logger.exception("Error setting bucket tags via S3 protocol")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )


async def delete_bucket_tags(bucket_name: str, db: Any, main_account_id: str) -> Response:
    try:
        bucket = await BucketRepository(db).get_by_name_and_owner(bucket_name, main_account_id)
        if not bucket:
            return errors.s3_error_response(
                "NoSuchBucket",
                f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )
        await db.fetchrow("UPDATE buckets SET tags = $1 WHERE bucket_id = $2", json.dumps({}), bucket["bucket_id"])
        return Response(status_code=204)
    except Exception:
        logger.exception("Error deleting bucket tags")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )
