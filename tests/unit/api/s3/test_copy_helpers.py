from __future__ import annotations

from datetime import datetime
from datetime import timezone
from unittest.mock import AsyncMock
from unittest.mock import Mock

import pytest

from hippius_s3.api.s3.copy_helpers import build_copy_success_response
from hippius_s3.api.s3.copy_helpers import is_multipart_object
from hippius_s3.api.s3.copy_helpers import parse_copy_source
from hippius_s3.api.s3.copy_helpers import parse_object_metadata
from hippius_s3.api.s3.copy_helpers import resolve_chunk_size


def test_parse_copy_source_valid():
    result = parse_copy_source("/my-bucket/my/key.txt")
    assert result == ("my-bucket", "my/key.txt")


def test_parse_copy_source_url_encoded():
    result = parse_copy_source("/bucket/key%20with%20spaces.txt")
    assert result == ("bucket", "key with spaces.txt")


def test_parse_copy_source_with_query_params():
    result = parse_copy_source("/bucket/key.txt?versionId=123")
    assert result == ("bucket", "key.txt")


def test_parse_copy_source_with_leading_slash():
    result = parse_copy_source("/bucket/folder/key.txt")
    assert result == ("bucket", "folder/key.txt")


def test_parse_copy_source_without_leading_slash():
    result = parse_copy_source("bucket/key.txt")
    assert result == ("bucket", "key.txt")


def test_parse_copy_source_missing():
    from hippius_s3.api.s3.errors import S3Error

    with pytest.raises(S3Error) as exc_info:
        parse_copy_source(None)
    assert exc_info.value.code == "InvalidArgument"


def test_parse_copy_source_invalid_format():
    from hippius_s3.api.s3.errors import S3Error

    with pytest.raises(S3Error) as exc_info:
        parse_copy_source("/just-bucket")
    assert exc_info.value.code == "InvalidArgument"


def test_parse_object_metadata_json_string():
    result = parse_object_metadata('{"multipart": true, "key": "value"}')
    assert result == {"multipart": True, "key": "value"}


def test_parse_object_metadata_dict():
    input_dict = {"key": "value", "number": 42}
    result = parse_object_metadata(input_dict)
    assert result == input_dict


def test_parse_object_metadata_none():
    result = parse_object_metadata(None)
    assert result == {}


def test_parse_object_metadata_invalid_json():
    result = parse_object_metadata("not valid json")
    assert result == {}


def test_parse_object_metadata_json_array():
    result = parse_object_metadata('["array", "not", "dict"]')
    assert result == {}


def test_is_multipart_object_from_column_true():
    obj_row = {"multipart": True, "metadata": "{}"}
    assert is_multipart_object(obj_row) is True


def test_is_multipart_object_from_column_false():
    obj_row = {"multipart": False, "metadata": '{"multipart": true}'}
    assert is_multipart_object(obj_row) is False


def test_is_multipart_object_from_metadata():
    obj_row = {"multipart": None, "metadata": '{"multipart": true}'}
    assert is_multipart_object(obj_row) is True


def test_is_multipart_object_from_metadata_false():
    obj_row = {"multipart": None, "metadata": '{"multipart": false}'}
    assert is_multipart_object(obj_row) is False


def test_is_multipart_object_no_metadata():
    obj_row = {"multipart": None, "metadata": None}
    assert is_multipart_object(obj_row) is False


def test_resolve_chunk_size_from_row():
    obj_row = {"enc_chunk_size_bytes": 524288}
    config = Mock(object_chunk_size_bytes=262144)
    assert resolve_chunk_size(obj_row, config) == 524288


def test_resolve_chunk_size_from_config():
    obj_row = {"enc_chunk_size_bytes": None}
    config = Mock(object_chunk_size_bytes=262144)
    assert resolve_chunk_size(obj_row, config) == 262144


def test_resolve_chunk_size_missing_from_row():
    obj_row = {}
    config = Mock(object_chunk_size_bytes=131072)
    assert resolve_chunk_size(obj_row, config) == 131072


def test_build_copy_success_response():
    etag = "abc123def456"
    last_modified = datetime(2025, 1, 25, 12, 0, 0, tzinfo=timezone.utc)

    response = build_copy_success_response(etag, last_modified)

    assert response.status_code == 200
    assert response.media_type == "application/xml"
    assert response.headers["ETag"] == '"abc123def456"'
    assert b"<CopyObjectResult>" in response.body
    assert b"<ETag>abc123def456</ETag>" in response.body
    assert b"<LastModified>2025-01-25T12:00:00.000Z</LastModified>" in response.body


def test_build_copy_success_response_empty_etag():
    etag = ""
    last_modified = datetime(2025, 1, 25, 12, 0, 0, tzinfo=timezone.utc)

    response = build_copy_success_response(etag, last_modified)

    assert response.status_code == 200
    assert response.headers["ETag"] == '""'
    assert b"<ETag></ETag>" in response.body


@pytest.mark.asyncio
async def test_should_use_v5_fast_path_eligible():
    from hippius_s3.api.s3.copy_helpers import should_use_v5_fast_path

    db = AsyncMock()
    db.fetch.return_value = [
        (0, "QmAbc123", 1024, 1000),
        (1, "QmDef456", 1024, 1000),
    ]

    src_obj_row = {
        "object_id": "obj-123",
        "object_version": 1,
    }

    eligible, chunk_rows, reason = await should_use_v5_fast_path(
        db=db,
        src_obj_row=src_obj_row,
        existing_dest=None,
        src_storage_version=5,
        src_multipart=False,
    )

    assert eligible is True
    assert len(chunk_rows) == 2
    assert reason == "fast_path_eligible"


@pytest.mark.asyncio
async def test_should_use_v5_fast_path_storage_version_too_old():
    from hippius_s3.api.s3.copy_helpers import should_use_v5_fast_path

    db = AsyncMock()
    src_obj_row = {"object_id": "obj-123", "object_version": 1}

    eligible, chunk_rows, reason = await should_use_v5_fast_path(
        db=db,
        src_obj_row=src_obj_row,
        existing_dest=None,
        src_storage_version=4,
        src_multipart=False,
    )

    assert eligible is False
    assert chunk_rows is None
    assert reason == "storage_version < 5"


@pytest.mark.asyncio
async def test_should_use_v5_fast_path_multipart_not_supported():
    from hippius_s3.api.s3.copy_helpers import should_use_v5_fast_path

    db = AsyncMock()
    src_obj_row = {"object_id": "obj-123", "object_version": 1}

    eligible, chunk_rows, reason = await should_use_v5_fast_path(
        db=db,
        src_obj_row=src_obj_row,
        existing_dest=None,
        src_storage_version=5,
        src_multipart=True,
    )

    assert eligible is False
    assert chunk_rows is None
    assert reason == "multipart not supported"


@pytest.mark.asyncio
async def test_should_use_v5_fast_path_destination_exists():
    from hippius_s3.api.s3.copy_helpers import should_use_v5_fast_path

    db = AsyncMock()
    src_obj_row = {"object_id": "obj-123", "object_version": 1}
    existing_dest = {"object_id": "obj-456"}

    eligible, chunk_rows, reason = await should_use_v5_fast_path(
        db=db,
        src_obj_row=src_obj_row,
        existing_dest=existing_dest,
        src_storage_version=5,
        src_multipart=False,
    )

    assert eligible is False
    assert chunk_rows is None
    assert reason == "destination exists (new version), AAD mismatch"


@pytest.mark.asyncio
async def test_should_use_v5_fast_path_missing_chunks():
    from hippius_s3.api.s3.copy_helpers import should_use_v5_fast_path

    db = AsyncMock()
    db.fetch.return_value = []

    src_obj_row = {"object_id": "obj-123", "object_version": 1}

    eligible, chunk_rows, reason = await should_use_v5_fast_path(
        db=db,
        src_obj_row=src_obj_row,
        existing_dest=None,
        src_storage_version=5,
        src_multipart=False,
    )

    assert eligible is False
    assert chunk_rows is None
    assert reason == "missing part_chunks metadata"


@pytest.mark.asyncio
async def test_should_use_v5_fast_path_pending_cids():
    from hippius_s3.api.s3.copy_helpers import should_use_v5_fast_path

    db = AsyncMock()
    db.fetch.return_value = [
        (0, "QmAbc123", 1024, 1000),
        (1, "pending", 1024, 1000),
        (2, "QmDef456", 1024, 1000),
    ]

    src_obj_row = {"object_id": "obj-123", "object_version": 1}

    eligible, chunk_rows, reason = await should_use_v5_fast_path(
        db=db,
        src_obj_row=src_obj_row,
        existing_dest=None,
        src_storage_version=5,
        src_multipart=False,
    )

    assert eligible is False
    assert chunk_rows is None
    assert "missing chunk CID(s) indices=[1]" in reason
