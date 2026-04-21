#!/usr/bin/env python3
import argparse
import concurrent.futures
import contextlib
import json
import os
import signal
import sys
import tempfile
import threading
import time
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import boto3
from botocore.config import Config as BotoConfig
from botocore.exceptions import BotoCoreError
from botocore.exceptions import ClientError


STATE_VERSION = 1
STATE_SUFFIX = ".mpu-state.json"
STOP = threading.Event()


@dataclass
class PartEntry:
    etag: str
    size: int


@dataclass
class State:
    upload_id: str
    endpoint_url: str
    bucket: str
    key: str
    source_path: str
    source_size: int
    source_mtime: float
    part_size: int
    parts: Dict[str, PartEntry] = field(default_factory=dict)
    version: int = STATE_VERSION

    def to_json(self) -> Dict[str, Any]:
        return {
            "version": self.version,
            "upload_id": self.upload_id,
            "endpoint_url": self.endpoint_url,
            "bucket": self.bucket,
            "key": self.key,
            "source_path": self.source_path,
            "source_size": self.source_size,
            "source_mtime": self.source_mtime,
            "part_size": self.part_size,
            "parts": {pn: asdict(p) for pn, p in self.parts.items()},
        }


def state_path_for(file_path: str) -> str:
    return file_path + STATE_SUFFIX


def load_state(file_path: str) -> Optional[State]:
    sp = state_path_for(file_path)
    if not os.path.exists(sp):
        return None
    try:
        with open(sp, "r") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[warn] corrupt sidecar at {sp} ({e}); ignoring.", file=sys.stderr)
        return None
    parts = {str(pn): PartEntry(**p) for pn, p in data.get("parts", {}).items()}
    return State(
        upload_id=data["upload_id"],
        endpoint_url=data.get("endpoint_url", ""),
        bucket=data["bucket"],
        key=data["key"],
        source_path=data["source_path"],
        source_size=int(data["source_size"]),
        source_mtime=float(data["source_mtime"]),
        part_size=int(data["part_size"]),
        parts=parts,
        version=int(data.get("version", STATE_VERSION)),
    )


def save_state_atomic(state: State, file_path: str) -> None:
    sp = state_path_for(file_path)
    dir_ = os.path.dirname(os.path.abspath(sp)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".mpu-state.", dir=dir_)
    with os.fdopen(fd, "w") as f:
        json.dump(state.to_json(), f, indent=2, sort_keys=True)
    os.replace(tmp, sp)


def clear_state(file_path: str) -> None:
    sp = state_path_for(file_path)
    with contextlib.suppress(FileNotFoundError):
        os.remove(sp)


def make_s3_client(endpoint_url: str) -> Any:
    cfg = BotoConfig(
        retries={"max_attempts": 5, "mode": "adaptive"},
        s3={"addressing_style": "path"},
        connect_timeout=20,
        read_timeout=300,
    )
    return boto3.session.Session().client("s3", endpoint_url=(endpoint_url or None), config=cfg)


def find_existing_mpu(s3: Any, bucket: str, key: str) -> Optional[str]:
    resp = s3.list_multipart_uploads(Bucket=bucket, Prefix=key)
    matches = [u for u in resp.get("Uploads", []) if u.get("Key") == key]
    if len(matches) == 1:
        return str(matches[0]["UploadId"])
    if len(matches) > 1:
        print(
            f"[warn] {len(matches)} in-progress uploads for s3://{bucket}/{key}; "
            f"not adopting automatically — pass --force-restart to abort them, or use --list to inspect.",
            file=sys.stderr,
        )
    return None


def server_parts(s3: Any, bucket: str, key: str, upload_id: str) -> Optional[Dict[int, PartEntry]]:
    parts: Dict[int, PartEntry] = {}
    marker = 0
    while True:
        try:
            resp = s3.list_parts(Bucket=bucket, Key=key, UploadId=upload_id, PartNumberMarker=marker)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("NoSuchUpload", "404"):
                return None
            raise
        for p in resp.get("Parts", []):
            pn = int(p["PartNumber"])
            parts[pn] = PartEntry(etag=str(p["ETag"]).strip('"'), size=int(p["Size"]))
        if not resp.get("IsTruncated"):
            break
        marker = int(resp.get("NextPartNumberMarker", 0))
    return parts


def num_parts_for(total_size: int, part_size: int) -> int:
    if total_size == 0:
        return 0
    q, r = divmod(total_size, part_size)
    return q + (1 if r else 0)


def upload_one_part(
    s3: Any,
    state: State,
    part_number: int,
    file_path: str,
    max_retries: int,
    lock: threading.Lock,
) -> PartEntry:
    offset = (part_number - 1) * state.part_size
    size = min(state.part_size, state.source_size - offset)
    if size <= 0:
        raise ValueError(f"Invalid part_number {part_number} for file size {state.source_size}")

    with open(file_path, "rb") as f:
        f.seek(offset)
        body = f.read(size)

    attempt = 0
    delay = 1.0
    while True:
        if STOP.is_set():
            raise KeyboardInterrupt("stopped")
        attempt += 1
        try:
            resp = s3.upload_part(
                Bucket=state.bucket,
                Key=state.key,
                PartNumber=part_number,
                UploadId=state.upload_id,
                Body=body,
            )
            etag = str(resp["ETag"]).strip('"')
            entry = PartEntry(etag=etag, size=size)
            with lock:
                state.parts[str(part_number)] = entry
                save_state_atomic(state, file_path)
            return entry
        except (ClientError, BotoCoreError, OSError) as e:
            if attempt > max_retries:
                raise
            print(
                f"[warn] part {part_number} attempt {attempt} failed: {e}; retrying in {delay:.1f}s",
                file=sys.stderr,
            )
            time.sleep(delay)
            delay = min(delay * 2, 30.0)


def do_upload(args: argparse.Namespace, s3: Any) -> int:
    file_path = args.file
    if not os.path.isfile(file_path):
        print(f"[error] file not found: {file_path}", file=sys.stderr)
        return 2
    source_size = os.path.getsize(file_path)
    source_mtime = os.path.getmtime(file_path)

    if source_size == 0:
        print("[info] file is empty; using PutObject (MPU requires non-zero parts)")
        s3.put_object(Bucket=args.bucket, Key=args.key, Body=b"")
        clear_state(file_path)
        return 0

    requested_part_size = args.part_size_mb * 1024 * 1024
    state = load_state(file_path)

    if state and args.force_restart:
        print(f"[info] --force-restart: aborting old MPU and clearing sidecar for {file_path}")
        with contextlib.suppress(ClientError):
            s3.abort_multipart_upload(Bucket=state.bucket, Key=state.key, UploadId=state.upload_id)
        clear_state(file_path)
        state = None

    if state is not None:
        if state.source_size != source_size or abs(state.source_mtime - source_mtime) > 1e-6:
            print(
                f"[error] source file changed since sidecar was written "
                f"(size {state.source_size} -> {source_size}, mtime {state.source_mtime} -> {source_mtime}). "
                f"Re-run with --force-restart to discard the sidecar and start fresh.",
                file=sys.stderr,
            )
            return 3
        if state.bucket != args.bucket or state.key != args.key:
            print(
                f"[error] sidecar points to s3://{state.bucket}/{state.key} but you asked for "
                f"s3://{args.bucket}/{args.key}. Refusing to continue.",
                file=sys.stderr,
            )
            return 3

    if state is None:
        existing = find_existing_mpu(s3, args.bucket, args.key)
        part_size = requested_part_size
        if existing:
            upload_id = existing
            print(f"[info] adopting existing MPU upload_id={upload_id}")
            peek = server_parts(s3, args.bucket, args.key, upload_id) or {}
            if peek:
                first_pn = min(peek.keys())
                inferred = peek[first_pn].size
                if inferred != requested_part_size:
                    print(
                        f"[info] inferred part_size={inferred} from server part {first_pn}; "
                        f"overrides --part-size-mb={args.part_size_mb}"
                    )
                part_size = inferred
        else:
            resp = s3.create_multipart_upload(Bucket=args.bucket, Key=args.key)
            upload_id = str(resp["UploadId"])
            print(f"[info] created new MPU upload_id={upload_id}")
        state = State(
            upload_id=upload_id,
            endpoint_url=args.endpoint_url or "",
            bucket=args.bucket,
            key=args.key,
            source_path=os.path.abspath(file_path),
            source_size=source_size,
            source_mtime=source_mtime,
            part_size=part_size,
        )
        save_state_atomic(state, file_path)

    server = server_parts(s3, state.bucket, state.key, state.upload_id)
    if server is None:
        print("[info] server no longer knows this uploadId; starting fresh")
        clear_state(file_path)
        return do_upload(args, s3)

    state.parts = {str(pn): PartEntry(etag=p.etag, size=p.size) for pn, p in server.items()}
    save_state_atomic(state, file_path)

    total_parts = num_parts_for(state.source_size, state.part_size)
    done = {int(pn) for pn in state.parts}
    missing = [pn for pn in range(1, total_parts + 1) if pn not in done]
    print(
        f"[info] s3://{state.bucket}/{state.key} "
        f"{len(done)}/{total_parts} parts uploaded; {len(missing)} remaining "
        f"(part_size={state.part_size // (1024 * 1024)}MB, workers={args.workers})"
    )

    lock = threading.Lock()
    errors: List[BaseException] = []

    def _work(pn: int) -> None:
        if STOP.is_set():
            return
        try:
            entry = upload_one_part(s3, state, pn, file_path, args.max_retries, lock)
            print(f"[info] part {pn}/{total_parts} ok ({entry.size} bytes, etag={entry.etag})")
        except KeyboardInterrupt:
            pass
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = [pool.submit(_work, pn) for pn in missing]
        for fut in concurrent.futures.as_completed(futures):
            fut.result()

    if STOP.is_set():
        print(
            "[info] interrupted; sidecar preserved. Run the same command to resume.",
            file=sys.stderr,
        )
        return 130

    if errors:
        print(
            f"[error] {len(errors)} part(s) failed; sidecar preserved. Last error: {errors[-1]}",
            file=sys.stderr,
        )
        return 1

    parts_list = [{"PartNumber": int(pn), "ETag": f'"{p.etag}"'} for pn, p in state.parts.items()]
    parts_list.sort(key=lambda p: int(p["PartNumber"]))
    resp = s3.complete_multipart_upload(
        Bucket=state.bucket,
        Key=state.key,
        UploadId=state.upload_id,
        MultipartUpload={"Parts": parts_list},
    )
    etag = resp.get("ETag", "")
    print(f"[info] complete: s3://{state.bucket}/{state.key} etag={etag}")
    clear_state(file_path)
    return 0


def do_list(args: argparse.Namespace, s3: Any) -> int:
    resp = s3.list_multipart_uploads(Bucket=args.bucket, Prefix=args.key or "")
    uploads = resp.get("Uploads", [])
    if not uploads:
        print("(no in-progress multipart uploads)")
        return 0
    for u in uploads:
        print(f"{u.get('UploadId')}  {u.get('Key')}  initiated={u.get('Initiated')}")
    return 0


def do_abort(args: argparse.Namespace, s3: Any) -> int:
    if not args.key:
        print("[error] --abort requires --key", file=sys.stderr)
        return 2
    upload_id = args.upload_id or find_existing_mpu(s3, args.bucket, args.key)
    if not upload_id:
        print(f"[error] no in-progress upload for s3://{args.bucket}/{args.key}", file=sys.stderr)
        return 1
    s3.abort_multipart_upload(Bucket=args.bucket, Key=args.key, UploadId=upload_id)
    print(f"[info] aborted s3://{args.bucket}/{args.key} upload_id={upload_id}")
    if args.file:
        clear_state(args.file)
    return 0


def _on_signal(_signum: int, _frame: Any) -> None:
    if STOP.is_set():
        sys.exit(130)
    print(
        "[info] interrupt received; letting in-flight parts finish... (Ctrl+C again to force)",
        file=sys.stderr,
    )
    STOP.set()


def main() -> int:
    ap = argparse.ArgumentParser(
        description=(
            "Resumable S3 multipart uploader for Hippius (or any S3). "
            "Persists state to <file>.mpu-state.json and resumes across process restarts. "
            "Reads AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION from env."
        ),
    )
    ap.add_argument("--file", help="Local file to upload (required for upload mode)")
    ap.add_argument("--bucket", required=True, help="Destination bucket")
    ap.add_argument("--key", default="", help="Destination key (default: basename of --file)")
    ap.add_argument(
        "--endpoint-url",
        default=os.environ.get("S3_ENDPOINT_URL", ""),
        help="S3 endpoint URL (default: $S3_ENDPOINT_URL, else AWS)",
    )
    ap.add_argument("--part-size-mb", type=int, default=16, help="Part size in MB (default: 16)")
    ap.add_argument("--workers", type=int, default=4, help="Concurrent upload workers (default: 4)")
    ap.add_argument("--max-retries", type=int, default=5, help="Per-part retry attempts (default: 5)")
    ap.add_argument("--force-restart", action="store_true", help="Abort existing MPU, discard sidecar, start fresh")
    ap.add_argument("--list", action="store_true", help="List in-progress multipart uploads and exit")
    ap.add_argument("--abort", action="store_true", help="Abort an in-progress upload for --bucket/--key")
    ap.add_argument("--upload-id", default="", help="Explicit uploadId for --abort")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    if args.file and not args.key:
        args.key = os.path.basename(args.file)

    signal.signal(signal.SIGINT, _on_signal)
    signal.signal(signal.SIGTERM, _on_signal)

    s3 = make_s3_client(args.endpoint_url)

    if args.list:
        return do_list(args, s3)
    if args.abort:
        return do_abort(args, s3)
    if not args.file:
        ap.error("--file is required unless --list or --abort is used")
    if not args.key:
        ap.error("--key could not be derived; pass --key explicitly")
    return do_upload(args, s3)


if __name__ == "__main__":
    sys.exit(main())
