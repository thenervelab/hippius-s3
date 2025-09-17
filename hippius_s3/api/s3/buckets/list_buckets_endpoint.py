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


async def handle_list_buckets(ctx: RequestContext, db: Any) -> Response:
    try:
        results = await db.fetch(get_query("list_user_buckets"), ctx.main_account_id)

        root = ET.Element("ListAllMyBucketsResult", xmlns="http://s3.amazonaws.com/doc/2006-03-01/")
        owner = ET.SubElement(root, "Owner")
        ET.SubElement(owner, "ID").text = "hippius-s3-ipfs-gateway"
        ET.SubElement(owner, "DisplayName").text = "hippius-s3"
        buckets = ET.SubElement(root, "Buckets")
        for row in results:
            bucket = ET.SubElement(buckets, "Bucket")
            ET.SubElement(bucket, "Name").text = row["bucket_name"]
            ET.SubElement(bucket, "CreationDate").text = _format_s3_timestamp(row["created_at"])
        xml_content = ET.tostring(root, encoding="UTF-8", xml_declaration=True, pretty_print=True)
        return Response(content=xml_content, media_type="application/xml")
    except Exception:
        logger.exception("Error listing buckets via S3 protocol")
        return errors.s3_error_response(
            "InternalError", "We encountered an internal error. Please try again.", status_code=500
        )
