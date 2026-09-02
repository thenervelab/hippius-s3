"""Object Lock on the two write paths that were not honouring it: CopyObject and multipart.

Both were silent. A copy into a compliance bucket landed completely unprotected with a 200 and no
signal; a multipart upload carrying lock headers answered 501 while the same headers on a simple PUT
were honoured, so "lock this object" worked or failed purely on file size.

The theme in every test below is that a durability control must not depend on which internal path
the write happened to take. A client cannot see the difference between a fast-path copy, a streaming
copy and a multipart upload, so none of them may differ in whether the object comes back retained.
"""

from __future__ import annotations

import datetime
import os
from typing import Any
from typing import Callable

import pytest
from botocore.exceptions import ClientError


def _error(exc: ClientError) -> tuple[int, str]:
    return (
        exc.response["ResponseMetadata"]["HTTPStatusCode"],
        exc.response.get("Error", {}).get("Code", ""),
    )


def _until(days: int = 30) -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(days=days)


def _locked_bucket(client: Any, name: str, *, default_days: int | None = None) -> None:
    client.create_bucket(Bucket=name, ObjectLockEnabledForBucket=True)
    if default_days is not None:
        client.put_object_lock_configuration(
            Bucket=name,
            ObjectLockConfiguration={
                "ObjectLockEnabled": "Enabled",
                "Rule": {"DefaultRetention": {"Mode": "GOVERNANCE", "Days": default_days}},
            },
        )


def _drop(client: Any, bucket: str, key: str) -> None:
    """Remove a version that may be under GOVERNANCE, so a test bucket stays cleanable."""
    for v in client.list_object_versions(Bucket=bucket, Prefix=key).get("Versions", []):
        try:
            client.delete_object(
                Bucket=bucket, Key=key, VersionId=v["VersionId"], BypassGovernanceRetention=True
            )
        except ClientError:
            pass


# --------------------------------------------------------------------------------------------
# CopyObject
# --------------------------------------------------------------------------------------------


class TestCopyObjectLock:
    def test_copy_inherits_the_destination_bucket_default_retention(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """The silent gap: copying INTO a compliance bucket is how data usually arrives there."""
        src = unique_bucket_name("cp-src")
        dst = unique_bucket_name("cp-dst")
        cleanup_buckets(src)
        cleanup_buckets(dst)
        boto3_client.create_bucket(Bucket=src)
        _locked_bucket(boto3_client, dst, default_days=7)

        boto3_client.put_object(Bucket=src, Key="a.txt", Body=b"payload")
        boto3_client.copy_object(Bucket=dst, Key="a.txt", CopySource=f"{src}/a.txt")

        head = boto3_client.head_object(Bucket=dst, Key="a.txt")
        assert head.get("ObjectLockMode") == "GOVERNANCE", (
            "a copy into a bucket with a default retention landed unprotected"
        )
        assert head.get("ObjectLockRetainUntilDate") is not None
        _drop(boto3_client, dst, "a.txt")

    def test_copy_honours_explicit_lock_headers(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        src = unique_bucket_name("cp-hsrc")
        dst = unique_bucket_name("cp-hdst")
        cleanup_buckets(src)
        cleanup_buckets(dst)
        boto3_client.create_bucket(Bucket=src)
        _locked_bucket(boto3_client, dst)

        boto3_client.put_object(Bucket=src, Key="b.txt", Body=b"payload")
        boto3_client.copy_object(
            Bucket=dst,
            Key="b.txt",
            CopySource=f"{src}/b.txt",
            ObjectLockMode="COMPLIANCE",
            ObjectLockRetainUntilDate=_until(),
        )
        head = boto3_client.head_object(Bucket=dst, Key="b.txt")
        assert head.get("ObjectLockMode") == "COMPLIANCE"

    def test_explicit_headers_override_the_destination_default(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Same precedence as PUT: the request wins over the bucket."""
        src = unique_bucket_name("cp-osrc")
        dst = unique_bucket_name("cp-odst")
        cleanup_buckets(src)
        cleanup_buckets(dst)
        boto3_client.create_bucket(Bucket=src)
        _locked_bucket(boto3_client, dst, default_days=7)

        boto3_client.put_object(Bucket=src, Key="c.txt", Body=b"payload")
        boto3_client.copy_object(
            Bucket=dst,
            Key="c.txt",
            CopySource=f"{src}/c.txt",
            ObjectLockMode="COMPLIANCE",
            ObjectLockRetainUntilDate=_until(),
        )
        assert boto3_client.head_object(Bucket=dst, Key="c.txt").get("ObjectLockMode") == "COMPLIANCE"

    def test_same_bucket_locked_copy_is_a_real_copy_not_an_alias(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """The subtle one. A same-bucket copy is normally optimised into an ALIAS — a second name on
        one object_id, with no version of its own. A per-version lock has nothing to attach to
        there, and writing one would lock the SOURCE too: an object the caller never named becomes
        undeletable. So lock intent has to disqualify the alias optimisation.

        Asserted from the outside, the way a client would notice: the destination is retained and
        the source is NOT.
        """
        b = unique_bucket_name("cp-alias")
        cleanup_buckets(b)
        _locked_bucket(boto3_client, b)
        boto3_client.put_object(Bucket=b, Key="orig.txt", Body=b"payload")

        boto3_client.copy_object(
            Bucket=b,
            Key="copy.txt",
            CopySource=f"{b}/orig.txt",
            ObjectLockMode="GOVERNANCE",
            ObjectLockRetainUntilDate=_until(),
        )

        assert boto3_client.head_object(Bucket=b, Key="copy.txt").get("ObjectLockMode") == "GOVERNANCE"
        assert "ObjectLockMode" not in boto3_client.head_object(Bucket=b, Key="orig.txt"), (
            "locking a same-bucket copy also locked its SOURCE — the alias was not disqualified"
        )
        _drop(boto3_client, b, "copy.txt")

    def test_unlocked_same_bucket_copy_still_takes_the_alias_path(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """The optimisation must survive for the overwhelmingly common unlocked case."""
        b = unique_bucket_name("cp-noalias")
        cleanup_buckets(b)
        boto3_client.create_bucket(Bucket=b)
        boto3_client.put_object(Bucket=b, Key="o.txt", Body=b"payload")
        boto3_client.copy_object(Bucket=b, Key="c.txt", CopySource=f"{b}/o.txt")
        assert boto3_client.get_object(Bucket=b, Key="c.txt")["Body"].read() == b"payload"

    def test_copy_into_a_bucket_without_object_lock_is_refused(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Same opt-in rule as PUT — and refused, not dropped."""
        src = unique_bucket_name("cp-nsrc")
        dst = unique_bucket_name("cp-ndst")
        cleanup_buckets(src)
        cleanup_buckets(dst)
        boto3_client.create_bucket(Bucket=src)
        boto3_client.create_bucket(Bucket=dst)
        boto3_client.put_object(Bucket=src, Key="d.txt", Body=b"payload")

        with pytest.raises(ClientError) as excinfo:
            boto3_client.copy_object(
                Bucket=dst,
                Key="d.txt",
                CopySource=f"{src}/d.txt",
                ObjectLockMode="COMPLIANCE",
                ObjectLockRetainUntilDate=_until(),
            )
        status, code = _error(excinfo.value)
        assert status == 400 and code == "InvalidRequest"

    def test_refused_copy_leaves_no_destination_object(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """A 4xx must not leave a side effect — the same rule PutObject needed fixing for."""
        src = unique_bucket_name("cp-ssrc")
        dst = unique_bucket_name("cp-sdst")
        cleanup_buckets(src)
        cleanup_buckets(dst)
        boto3_client.create_bucket(Bucket=src)
        boto3_client.create_bucket(Bucket=dst)
        boto3_client.put_object(Bucket=src, Key="e.txt", Body=b"payload")
        boto3_client.put_object(Bucket=dst, Key="e.txt", Body=b"ORIGINAL")

        with pytest.raises(ClientError):
            boto3_client.copy_object(
                Bucket=dst,
                Key="e.txt",
                CopySource=f"{src}/e.txt",
                ObjectLockMode="COMPLIANCE",
                ObjectLockRetainUntilDate=_until(),
            )
        assert boto3_client.get_object(Bucket=dst, Key="e.txt")["Body"].read() == b"ORIGINAL", (
            "a refused copy overwrote the destination anyway"
        )

    def test_a_locked_copy_actually_resists_deletion(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """The header round-trip is the symptom; refusing the delete is the property that matters."""
        src = unique_bucket_name("cp-esrc")
        dst = unique_bucket_name("cp-edst")
        cleanup_buckets(src)
        cleanup_buckets(dst)
        boto3_client.create_bucket(Bucket=src)
        _locked_bucket(boto3_client, dst)
        boto3_client.put_object(Bucket=src, Key="f.txt", Body=b"payload")
        boto3_client.copy_object(
            Bucket=dst,
            Key="f.txt",
            CopySource=f"{src}/f.txt",
            ObjectLockMode="GOVERNANCE",
            ObjectLockRetainUntilDate=_until(),
        )

        vid = boto3_client.list_object_versions(Bucket=dst, Prefix="f.txt")["Versions"][0]["VersionId"]
        with pytest.raises(ClientError) as excinfo:
            boto3_client.delete_object(Bucket=dst, Key="f.txt", VersionId=vid)
        assert _error(excinfo.value)[1] == "AccessDenied"
        _drop(boto3_client, dst, "f.txt")


# --------------------------------------------------------------------------------------------
# Multipart
# --------------------------------------------------------------------------------------------


class TestMultipartObjectLock:
    def _mpu(self, client: Any, bucket: str, key: str, **extra: Any) -> None:
        created = client.create_multipart_upload(Bucket=bucket, Key=key, **extra)
        part = client.upload_part(
            Bucket=bucket, Key=key, PartNumber=1, UploadId=created["UploadId"], Body=b"x" * (5 * 1024 * 1024)
        )
        client.complete_multipart_upload(
            Bucket=bucket,
            Key=key,
            UploadId=created["UploadId"],
            MultipartUpload={"Parts": [{"ETag": part["ETag"], "PartNumber": 1}]},
        )

    def test_multipart_honours_explicit_lock_headers(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Previously 501. The lock is fixed at initiate and must survive to the completed version."""
        b = unique_bucket_name("mpu-hdr")
        cleanup_buckets(b)
        _locked_bucket(boto3_client, b)
        self._mpu(
            boto3_client, b, "big",
            ObjectLockMode="COMPLIANCE", ObjectLockRetainUntilDate=_until(),
        )
        head = boto3_client.head_object(Bucket=b, Key="big")
        assert head.get("ObjectLockMode") == "COMPLIANCE"
        assert head.get("ObjectLockRetainUntilDate") is not None

    def test_multipart_legal_hold_header(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """A hold is an independent lock and needs no retention alongside it."""
        b = unique_bucket_name("mpu-hold")
        cleanup_buckets(b)
        _locked_bucket(boto3_client, b)
        self._mpu(boto3_client, b, "held", ObjectLockLegalHoldStatus="ON")
        assert (
            boto3_client.get_object_legal_hold(Bucket=b, Key="held")["LegalHold"]["Status"] == "ON"
        )
        boto3_client.put_object_legal_hold(Bucket=b, Key="held", LegalHold={"Status": "OFF"})

    def test_multipart_explicit_headers_override_the_bucket_default(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        b = unique_bucket_name("mpu-ovr")
        cleanup_buckets(b)
        _locked_bucket(boto3_client, b, default_days=7)
        self._mpu(
            boto3_client, b, "ovr",
            ObjectLockMode="COMPLIANCE", ObjectLockRetainUntilDate=_until(),
        )
        assert boto3_client.head_object(Bucket=b, Key="ovr").get("ObjectLockMode") == "COMPLIANCE"

    def test_multipart_lock_is_refused_on_a_bucket_without_object_lock(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Refused at INITIATE, before any part is uploaded — not after 5 GB of transfer."""
        b = unique_bucket_name("mpu-noopt")
        cleanup_buckets(b)
        boto3_client.create_bucket(Bucket=b)
        with pytest.raises(ClientError) as excinfo:
            boto3_client.create_multipart_upload(
                Bucket=b, Key="k",
                ObjectLockMode="COMPLIANCE", ObjectLockRetainUntilDate=_until(),
            )
        status, code = _error(excinfo.value)
        assert status == 400 and code == "InvalidRequest"

    def test_multipart_malformed_lock_header_is_refused_at_initiate(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """Mode without a retain-until date is incomplete on this path exactly as it is on PUT."""
        b = unique_bucket_name("mpu-bad")
        cleanup_buckets(b)
        _locked_bucket(boto3_client, b)
        with pytest.raises(ClientError) as excinfo:
            boto3_client.create_multipart_upload(Bucket=b, Key="k", ObjectLockMode="COMPLIANCE")
        assert _error(excinfo.value)[0] == 400

    def test_multipart_size_no_longer_changes_the_outcome(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
        tmp_path: Any,
    ) -> None:
        """The user-visible symptom: boto3 routes ExtraArgs to create_multipart_upload above the
        transfer threshold, so the identical upload_file call used to succeed under it and 501 over
        it. Both sizes must now come back retained."""
        from boto3.s3.transfer import TransferConfig

        b = unique_bucket_name("mpu-size")
        cleanup_buckets(b)
        _locked_bucket(boto3_client, b)
        cfg = TransferConfig(multipart_threshold=5 * 1024 * 1024, multipart_chunksize=5 * 1024 * 1024)
        extra = {"ObjectLockMode": "GOVERNANCE", "ObjectLockRetainUntilDate": _until()}

        for name, size in (("under", 1 * 1024 * 1024), ("over", 11 * 1024 * 1024)):
            path = tmp_path / name
            path.write_bytes(os.urandom(size))
            boto3_client.upload_file(str(path), b, name, ExtraArgs=dict(extra), Config=cfg)
            assert boto3_client.head_object(Bucket=b, Key=name).get("ObjectLockMode") == "GOVERNANCE", (
                f"{name}-threshold upload landed unprotected — the outcome still depends on size"
            )
            _drop(boto3_client, b, name)

    def test_multipart_without_lock_intent_is_unaffected(
        self,
        docker_services: Any,
        boto3_client: Any,
        unique_bucket_name: Callable[[str], str],
        cleanup_buckets: Callable[[str], None],
    ) -> None:
        """The common path must cost nothing and must not acquire a lock by accident."""
        b = unique_bucket_name("mpu-plain")
        cleanup_buckets(b)
        boto3_client.create_bucket(Bucket=b)
        self._mpu(boto3_client, b, "plain")
        assert "ObjectLockMode" not in boto3_client.head_object(Bucket=b, Key="plain")
        boto3_client.delete_object(Bucket=b, Key="plain")
