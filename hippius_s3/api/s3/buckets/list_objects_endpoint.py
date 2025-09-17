from __future__ import annotations

import logging
from datetime import datetime
from typing import Any

from fastapi import Response
from lxml import etree as ET

from hippius_s3.api.s3 import errors
from hippius_s3.dependencies import RequestContext
from hippius_s3.utils import get_query


logger = logging.getLogger(__name__)


def _format_s3_timestamp(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


async def handle_list_objects(bucket_name: str, ctx: RequestContext, db: Any, prefix: str | None) -> Response:
    try:
        # Get bucket for this main account
        bucket = await db.fetchrow(
            get_query("get_bucket_by_name_and_owner"),
            bucket_name,
            ctx.main_account_id,
        )
        if not bucket:
            return errors.s3_error_response(
                code="NoSuchBucket",
                message=f"The specified bucket {bucket_name} does not exist",
                status_code=404,
                BucketName=bucket_name,
            )

        bucket_id = bucket["bucket_id"]
        # list-objects supports optional Prefix filtering

        results = await db.fetch(get_query("list_objects"), bucket_id, prefix)
        # Defensive filter if backend query ignored prefix
        if prefix:
            results = [r for r in results if str(r["object_key"]).startswith(prefix)]

        root = ET.Element("ListBucketResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
        ET.SubElement(root, "Name").text = bucket_name
        ET.SubElement(root, "Prefix").text = prefix or ""
        ET.SubElement(root, "Marker").text = ""
        ET.SubElement(root, "MaxKeys").text = "1000"
        ET.SubElement(root, "IsTruncated").text = "false"

        for obj in results:
            content = ET.SubElement(root, "Contents")
            ET.SubElement(content, "Key").text = obj["object_key"]
            ET.SubElement(content, "LastModified").text = _format_s3_timestamp(obj["created_at"])
            ET.SubElement(content, "ETag").text = obj["md5_hash"]
            ET.SubElement(content, "Size").text = str(obj["size_bytes"])
            storage_class = ET.SubElement(content, "StorageClass")
            storage_class.text = "multipart" if obj.get("multipart") else "standard"
            owner = ET.SubElement(content, "Owner")
            ET.SubElement(owner, "ID").text = (obj.get("ipfs_cid") or "").strip() or "pending"
            ET.SubElement(owner, "DisplayName").text = ctx.main_account_id

        xml_content = ET.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)

        # Custom headers (optional diagnostics)
        total_objects = len(results)
        objects_with_cid = sum(1 for obj in results if obj.get("ipfs_cid"))
        status_counts: dict[str, int] = {}
        for obj in results:
            status = obj.get("status", "unknown")
            status_counts[status] = status_counts.get(status, 0) + 1

        headers = {
            "x-hippius-total-objects": str(total_objects),
            "x-hippius-objects-with-cid": str(objects_with_cid),
            "x-hippius-status-counts": ",".join(f"{k}:{v}" for k, v in status_counts.items()),
        }

        return Response(content=xml_content, media_type="application/xml", status_code=200, headers=headers)
    except Exception:
        logger.exception("Error listing objects")
        return errors.s3_error_response(
            "InternalError",
            "We encountered an internal error. Please try again.",
            status_code=500,
        )
