#!/usr/bin/env python3
from __future__ import annotations

import concurrent.futures
import hashlib
import os
import sys
import threading
import time
import urllib.request
from collections import Counter
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from typing import Any

import boto3
from botocore.config import Config as BotoConfig


NODE_HEADER = "x-hippius-node"
NO_NODE = "no X-Hippius-Node header"
SINGLE_PART_BYTES = 64 * 1024
MPU_PART_BYTES = 5 * 1024 * 1024
MPU_PARTS = 4
MPU_GETS = 3
HEAD_BUCKET_CALLS = 3
DRILL_GETS = 30
DRILL_TTFB_LIMIT_SECONDS = 5.0
PRESIGN_EXPIRES_SECONDS = 300


@dataclass(frozen=True)
class ProbeConfig:
    endpoint: str
    access_key: str
    secret_key: str
    region: str
    keys: int
    gets: int
    lists: int
    keep_bucket: bool
    drill: bool


@dataclass
class CheckResult:
    name: str
    passed: bool
    details: list[str] = field(default_factory=list)

    def line(self) -> str:
        verdict = "PASS" if self.passed else "FAIL"
        return f"{verdict} {self.name}: {'; '.join(self.details)}"


@dataclass(frozen=True)
class KeyObservation:
    key: str
    put_node: str | None
    get_nodes: tuple[str | None, ...]
    head_node: str | None
    body_ok: bool = True


@dataclass(frozen=True)
class DrillSample:
    status: int
    ttfb_seconds: float
    node: str | None
    error: str = ""


def _truthy(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def load_config(env: Mapping[str, str]) -> ProbeConfig | None:
    endpoint = env.get("HIPPIUS_ROUTING_ENDPOINT", "")
    access_key = env.get("AWS_ACCESS_KEY_ID", "")
    secret_key = env.get("AWS_SECRET_ACCESS_KEY", "")
    if not (endpoint and access_key and secret_key):
        return None
    return ProbeConfig(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        region=env.get("AWS_DEFAULT_REGION", "decentralized"),
        keys=int(env.get("HIPPIUS_PROBE_KEYS", "20")),
        gets=int(env.get("HIPPIUS_PROBE_GETS", "5")),
        lists=int(env.get("HIPPIUS_PROBE_LISTS", "50")),
        keep_bucket=_truthy(env.get("HIPPIUS_PROBE_KEEP_BUCKET", "")),
        drill=_truthy(env.get("HIPPIUS_PROBE_DRILL", "")),
    )


def node_from_parsed(parsed: Mapping[str, Any]) -> str | None:
    headers = parsed.get("ResponseMetadata", {}).get("HTTPHeaders", {})
    return headers.get(NODE_HEADER) or None


def node_from_headers(headers: Any) -> str | None:
    return headers.get("X-Hippius-Node") or None


def label(node: str | None) -> str:
    return node if node else NO_NODE


def distribution(nodes: Iterable[str | None]) -> str:
    counts = Counter(label(n) for n in nodes)
    return ", ".join(f"{node}={count}" for node, count in sorted(counts.items()))


def evaluate_single_part(observations: Sequence[KeyObservation]) -> CheckResult:
    mismatches: list[str] = []
    for obs in observations:
        reads = [*obs.get_nodes, obs.head_node]
        misplaced = obs.put_node is None or any(node != obs.put_node for node in reads)
        if misplaced or not obs.body_ok:
            reason = "" if obs.body_ok else " body-mismatch"
            mismatches.append(
                f"{obs.key} put={label(obs.put_node)} gets=[{', '.join(label(n) for n in obs.get_nodes)}] "
                f"head={label(obs.head_node)}{reason}"
            )
    gets = len(observations[0].get_nodes) if observations else 0
    details = [
        f"{len(observations)} keys x {gets} gets + head",
        f"put nodes: {distribution(obs.put_node for obs in observations)}",
    ]
    details.extend(mismatches or ["every read landed on its PUT node"])
    return CheckResult("single-part agreement", not mismatches, details)


def evaluate_colocation(name: str, ops: Mapping[str, str | None], extra: Sequence[str] = ()) -> CheckResult:
    nodes = set(ops.values())
    strays = [f"{op} -> {label(node)}" for op, node in ops.items() if node is None]
    passed = len(nodes) == 1 and None not in nodes
    if passed:
        details = [f"{len(ops)} ops all on {label(next(iter(nodes)))}"]
    else:
        details = [f"{op}={label(node)}" for op, node in ops.items()]
        details.extend(strays)
    details.extend(extra)
    return CheckResult(name, passed, details)


def evaluate_spread(name: str, nodes: Sequence[str | None], extra: Sequence[str] = ()) -> CheckResult:
    missing = sum(1 for node in nodes if node is None)
    distinct = {node for node in nodes if node is not None}
    passed = missing == 0 and len(distinct) > 1
    details = [f"{len(nodes)} calls over {len(distinct)} nodes: {distribution(nodes)}"]
    if missing:
        details.append(f"{missing} responses with {NO_NODE}")
    if len(distinct) <= 1:
        details.append("expected more than one node for bucket-level calls")
    details.extend(extra)
    return CheckResult(name, passed, details)


def evaluate_drill(samples: Sequence[DrillSample], put_node: str | None) -> CheckResult:
    served = [s for s in samples if s.node is not None]
    owner = Counter(s.node for s in served).most_common(1)[0][0] if served else None
    failures: list[str] = []
    for i, s in enumerate(samples):
        problems = []
        if not 200 <= s.status < 300:
            problems.append(f"status={s.status}{' ' + s.error if s.error else ''}")
        if s.ttfb_seconds >= DRILL_TTFB_LIMIT_SECONDS:
            problems.append(f"ttfb={s.ttfb_seconds:.2f}s")
        if s.node != owner:
            problems.append(f"node={label(s.node)}")
        if problems:
            failures.append(f"get#{i} " + " ".join(problems))
    ttfbs = [s.ttfb_seconds for s in samples]
    details = [
        f"{len(samples)} concurrent gets, owner={label(owner)}, put node={label(put_node)}"
        + (" (differs: expected for a drained-then-restored owner)" if put_node != owner else ""),
        f"ttfb max={max(ttfbs):.2f}s" if ttfbs else "no samples",
        f"nodes: {distribution(s.node for s in samples)}",
    ]
    details.extend(failures)
    return CheckResult("misplaced-object drill", bool(samples) and not failures, details)


def sample_from_error(exc: BaseException, ttfb_seconds: float) -> DrillSample:
    response = getattr(exc, "response", None)
    if isinstance(response, Mapping):
        status = int(response.get("ResponseMetadata", {}).get("HTTPStatusCode", 0))
        return DrillSample(status, ttfb_seconds, node_from_parsed(response), type(exc).__name__)
    return DrillSample(0, ttfb_seconds, None, f"{type(exc).__name__}: {exc}")


def exit_code(results: Sequence[CheckResult]) -> int:
    return 0 if all(r.passed for r in results) else 1


def make_client(cfg: ProbeConfig) -> Any:
    return boto3.client(
        "s3",
        endpoint_url=cfg.endpoint,
        aws_access_key_id=cfg.access_key,
        aws_secret_access_key=cfg.secret_key,
        region_name=cfg.region,
        config=BotoConfig(
            signature_version="s3v4",
            s3={"addressing_style": "path", "payload_signing_enabled": False},
            connect_timeout=10,
            read_timeout=120,
            retries={"max_attempts": 3, "mode": "standard"},
        ),
    )


class Probe:
    def __init__(self, client: Any, cfg: ProbeConfig, bucket: str) -> None:
        self.client = client
        self.cfg = cfg
        self.bucket = bucket
        self._captured = threading.local()
        client.meta.events.register("after-call.s3", self._after_call)

    def _after_call(self, parsed: Mapping[str, Any], **_: Any) -> None:
        self._captured.node = node_from_parsed(parsed)

    def call(self, op: str, **kwargs: Any) -> tuple[str | None, dict[str, Any]]:
        self._captured.node = None
        response = getattr(self.client, op)(Bucket=self.bucket, **kwargs)
        return self._captured.node, response

    def put(self, key: str, body: bytes) -> tuple[str | None, str | None]:
        node, response = self.call("put_object", Key=key, Body=body)
        version_id = response.get("VersionId")
        return node, version_id

    def get(self, key: str, **kwargs: Any) -> tuple[str | None, bytes]:
        node, response = self.call("get_object", Key=key, **kwargs)
        with response["Body"] as body:
            return node, body.read()

    def get_ttfb(self, key: str) -> DrillSample:
        started = time.monotonic()
        node, response = self.call("get_object", Key=key)
        with response["Body"] as body:
            chunks = body.iter_chunks(chunk_size=8192)
            next(chunks, b"")
            ttfb = time.monotonic() - started
            for _ in chunks:
                pass
        return DrillSample(int(response["ResponseMetadata"]["HTTPStatusCode"]), ttfb, node)

    def check_single_part(self) -> CheckResult:
        observations: list[KeyObservation] = []
        for i in range(self.cfg.keys):
            key = f"single/{i:03d}.bin"
            body = os.urandom(SINGLE_PART_BYTES)
            put_node, _ = self.put(key, body)
            get_nodes: list[str | None] = []
            body_ok = True
            for _ in range(self.cfg.gets):
                node, data = self.get(key)
                get_nodes.append(node)
                body_ok = body_ok and data == body
            head_node, _ = self.call("head_object", Key=key)
            observations.append(KeyObservation(key, put_node, tuple(get_nodes), head_node, body_ok))
        return evaluate_single_part(observations)

    def check_multipart(self) -> CheckResult:
        key = "mpu/object.bin"
        ops: dict[str, str | None] = {}
        node, created = self.call("create_multipart_upload", Key=key)
        ops["create"] = node
        upload_id = created["UploadId"]
        digest = hashlib.md5()
        parts = []
        for pn in range(1, MPU_PARTS + 1):
            data = os.urandom(MPU_PART_BYTES)
            digest.update(data)
            node, uploaded = self.call("upload_part", Key=key, PartNumber=pn, UploadId=upload_id, Body=data)
            ops[f"part{pn}"] = node
            parts.append({"PartNumber": pn, "ETag": uploaded["ETag"]})
        ops["list_parts"], _ = self.call("list_parts", Key=key, UploadId=upload_id)
        ops["complete"], _ = self.call(
            "complete_multipart_upload", Key=key, UploadId=upload_id, MultipartUpload={"Parts": parts}
        )
        extra: list[str] = []
        for i in range(MPU_GETS):
            node, data = self.get(key)
            ops[f"get{i + 1}"] = node
            if hashlib.md5(data).digest() != digest.digest():
                extra.append(f"get{i + 1} body-mismatch")
        result = evaluate_colocation("multipart co-location", ops, extra)
        result.passed = result.passed and not extra
        return result

    def check_multipart_abort(self) -> CheckResult:
        key = "mpu/aborted.bin"
        ops: dict[str, str | None] = {}
        ops["create"], created = self.call("create_multipart_upload", Key=key)
        upload_id = created["UploadId"]
        ops["part1"], _ = self.call(
            "upload_part", Key=key, PartNumber=1, UploadId=upload_id, Body=os.urandom(MPU_PART_BYTES)
        )
        ops["abort"], _ = self.call("abort_multipart_upload", Key=key, UploadId=upload_id)
        return evaluate_colocation("multipart abort", ops)

    def check_bucket_spread(self) -> CheckResult:
        list_nodes = [self.call("list_objects_v2")[0] for _ in range(self.cfg.lists)]
        head_nodes = [self.call("head_bucket")[0] for _ in range(HEAD_BUCKET_CALLS)]
        extra = [f"head_bucket: {distribution(head_nodes)}"]
        result = evaluate_spread("bucket-level spread", list_nodes, extra)
        result.passed = result.passed and None not in head_nodes
        return result

    def check_read_variants(self) -> CheckResult:
        key = "variants/one.bin"
        body = os.urandom(SINGLE_PART_BYTES)
        ops: dict[str, str | None] = {}
        ops["put"], version_id = self.put(key, body)
        extra: list[str] = []
        node, data = self.get(key, Range="bytes=0-1023")
        ops["range"] = node
        if data != body[:1024]:
            extra.append("range body-mismatch")
        if version_id is None:
            extra.append("PUT returned no VersionId, versioned GET skipped")
        else:
            ops["versioned"], data = self.get(key, VersionId=version_id)
            if data != body:
                extra.append("versioned body-mismatch")
        url = self.client.generate_presigned_url(
            "get_object", Params={"Bucket": self.bucket, "Key": key}, ExpiresIn=PRESIGN_EXPIRES_SECONDS
        )
        with urllib.request.urlopen(url, timeout=120) as response:
            ops["presigned"] = node_from_headers(response.headers)
            if response.read() != body:
                extra.append("presigned body-mismatch")
        result = evaluate_colocation("read variants (range/versioned/presigned)", ops, extra)
        result.passed = result.passed and not any(e.endswith("body-mismatch") for e in extra)
        return result

    def check_drill(self) -> CheckResult:
        key = "drill/misplaced.bin"
        print(
            "DRILL: drain one node at the edge (weight 0 on the hashed backend and its round-robin twin, "
            "on every load balancer), then press Enter",
            flush=True,
        )
        input()
        put_node, _ = self.put(key, os.urandom(SINGLE_PART_BYTES))
        print(f"DRILL: PUT {key} landed on {label(put_node)}; restore the node, then press Enter", flush=True)
        input()
        with concurrent.futures.ThreadPoolExecutor(max_workers=DRILL_GETS) as pool:
            started = time.monotonic()
            futures = [pool.submit(self.get_ttfb, key) for _ in range(DRILL_GETS)]
            concurrent.futures.wait(futures)
        samples = []
        for fut in futures:
            exc = fut.exception()
            samples.append(fut.result() if exc is None else sample_from_error(exc, time.monotonic() - started))
        return evaluate_drill(samples, put_node)

    def cleanup(self) -> None:
        # The server hands back a VersionId on PUT even on an unversioned bucket, but refuses
        # DeleteObject with that VersionId there, so always delete by key.
        for entry in self.call("list_objects_v2")[1].get("Contents", []):
            self.call("delete_object", Key=entry["Key"])
        self.call("delete_bucket")


def run(cfg: ProbeConfig, client: Any, bucket: str) -> list[CheckResult]:
    probe = Probe(client, cfg, bucket)
    probe.call("create_bucket")
    print(f"bucket {bucket} on {cfg.endpoint}", flush=True)
    checks: list[Callable[[], CheckResult]] = [
        probe.check_single_part,
        probe.check_multipart,
        probe.check_multipart_abort,
        probe.check_bucket_spread,
        probe.check_read_variants,
    ]
    if cfg.drill:
        checks.append(probe.check_drill)
    results = []
    for check in checks:
        result = check()
        print(result.line(), flush=True)
        results.append(result)
    if cfg.keep_bucket:
        print(f"keeping bucket {bucket}", flush=True)
    else:
        probe.cleanup()
    return results


def main() -> int:
    cfg = load_config(os.environ)
    if cfg is None:
        print("locality probe skipped: HIPPIUS_ROUTING_ENDPOINT, AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY required")
        return 0
    results = run(cfg, make_client(cfg), f"locality-probe-{int(time.time())}")
    failed = sum(1 for r in results if not r.passed)
    print(f"summary: {len(results) - failed}/{len(results)} checks passed", flush=True)
    return exit_code(results)


if __name__ == "__main__":
    sys.exit(main())
