#!/usr/bin/env python3
"""Upload/download throughput benchmark against a hippius-s3 endpoint.

Separate from run.py on purpose: run.py is the PASS/FAIL readiness gate, this is a measurement.
It emits no verdict — only numbers plus the client-side counters needed to tell a gateway-bound
result from a client-bound one.

Usage:
    source .aws.cli.env && python stress-test/throughput.py --endpoint https://s3.hippius.com
"""

from __future__ import annotations

import argparse
import concurrent.futures
import dataclasses
import hashlib
import json
import pathlib
import statistics
import sys
import time


sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent))

from boto3.s3.transfer import TransferConfig  # noqa: E402
from harness import config  # noqa: E402
from harness import s3util  # noqa: E402


MiB = 1024 * 1024
GiB = 1024 * MiB

# Multipart chunk size. Fixed and explicit so a rung's numbers are comparable across sizes —
# the hippius CLI default (64 MiB) would silently make the 64 MiB rung single-part and the
# 256 MiB rung 4-part, which is a different code path, not a different data point.
MPU_CHUNK = 8 * MiB

# One shared read-only block that every SyntheticFile repeats. The client box has 3 GB of RAM and
# 2.9 GB of free disk, so a 1 GiB object can be neither buffered nor staged — the data has to be
# manufactured at read() time or the benchmark OOMs before it measures anything.
_BLOCK_SIZE = 8 * MiB


def _make_block() -> bytes:
    out = bytearray()
    counter = 0
    while len(out) < _BLOCK_SIZE:
        out += hashlib.sha256(b"hippius-throughput" + counter.to_bytes(8, "big")).digest()
        counter += 1
    return bytes(out[:_BLOCK_SIZE])


_BLOCK = _make_block()


class SyntheticFile:
    """A seekable file-like of arbitrary size backed by a repeating in-memory block.

    boto3's transfer manager seeks and re-reads parts, so this has to honour seek/tell rather than
    being a plain generator. Memory stays flat at _BLOCK_SIZE no matter how large `size` is.
    """

    def __init__(self, size: int, salt: bytes) -> None:
        self._size = size
        self._pos = 0
        # Vary the head per object so two objects are never byte-identical, in case anything in the
        # path ever content-addresses plaintext. Cheap; the tail is the shared block.
        self._head = hashlib.sha256(salt).digest() * 4  # 128 B

    def read(self, n: int = -1) -> bytes:
        if n is None or n < 0:
            n = self._size - self._pos
        n = min(n, self._size - self._pos)
        if n <= 0:
            return b""
        out = bytearray(n)
        written = 0
        while written < n:
            abs_pos = self._pos + written
            if abs_pos < len(self._head):
                src, off = self._head, abs_pos
            else:
                src, off = _BLOCK, (abs_pos - len(self._head)) % _BLOCK_SIZE
            take = min(n - written, len(src) - off)
            out[written:written + take] = src[off:off + take]
            written += take
        self._pos += n
        return bytes(out)

    def seek(self, offset: int, whence: int = 0) -> int:
        base = {0: 0, 1: self._pos, 2: self._size}[whence]
        self._pos = max(0, min(self._size, base + offset))
        return self._pos

    def tell(self) -> int:
        return self._pos

    def seekable(self) -> bool:
        return True


# ---------------------------------------------------------------- client-side counters
def _cpu_jiffies() -> tuple[int, int]:
    """(busy, total) from /proc/stat. A rung whose numbers are capped by the client's own CPU is
    measuring Python's TLS stack, not the gateway — without this the two are indistinguishable."""
    parts = pathlib.Path("/proc/stat").read_text().splitlines()[0].split()[1:]
    vals = [int(p) for p in parts]
    idle = vals[3] + (vals[4] if len(vals) > 4 else 0)
    total = sum(vals)
    return total - idle, total


def _nic_bytes(iface: str) -> tuple[int, int]:
    base = pathlib.Path(f"/sys/class/net/{iface}/statistics")
    try:
        return (int((base / "rx_bytes").read_text()), int((base / "tx_bytes").read_text()))
    except OSError:
        return (0, 0)


def _default_iface() -> str:
    for p in sorted(pathlib.Path("/sys/class/net").iterdir()):
        if p.name != "lo":
            return p.name
    return "lo"


@dataclasses.dataclass
class Rung:
    phase: str
    size_label: str
    size_bytes: int
    concurrency: int
    objects: int
    wall_s: float
    ok: int
    errors: list[str]
    per_obj_s: list[float]
    ttfb_s: list[float]
    cpu_pct: float
    wire_mb: float

    @property
    def agg_mbps(self) -> float:
        return (self.ok * self.size_bytes / 1e6 / self.wall_s) if self.wall_s else 0.0

    @property
    def per_obj_mbps(self) -> float:
        if not self.per_obj_s:
            return 0.0
        return self.size_bytes / 1e6 / statistics.median(self.per_obj_s)


def _pct(vals: list[float], q: float) -> float:
    if not vals:
        return 0.0
    s = sorted(vals)
    return s[min(len(s) - 1, int(q * len(s)))]


def _run_rung(phase: str, size_label: str, size: int, concurrency: int, keys: list[str],
              fn, iface: str) -> Rung:
    per_obj_s: list[float] = []
    ttfb_s: list[float] = []
    errors: list[str] = []
    cpu0, tot0 = _cpu_jiffies()
    rx0, tx0 = _nic_bytes(iface)
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as ex:
        for res in ex.map(fn, keys):
            kind, elapsed, ttfb, err = res
            if kind == "ok":
                per_obj_s.append(elapsed)
                if ttfb is not None:
                    ttfb_s.append(ttfb)
            else:
                errors.append(err)
    wall = time.time() - t0
    cpu1, tot1 = _cpu_jiffies()
    rx1, tx1 = _nic_bytes(iface)
    dtot = max(1, tot1 - tot0)
    wire = ((tx1 - tx0) if phase == "upload" else (rx1 - rx0)) / 1e6
    r = Rung(phase, size_label, size, concurrency, len(keys), wall, len(per_obj_s), errors,
             per_obj_s, ttfb_s, 100.0 * (cpu1 - cpu0) / dtot, wire)
    ttfb_note = f" ttfb_p50={_pct(ttfb_s, 0.5)*1000:.0f}ms" if ttfb_s else ""
    print(f"    {phase:8s} {size_label:>7s} C={concurrency:<3d} n={len(keys):<3d} "
          f"{r.agg_mbps:7.1f} MB/s agg  {r.per_obj_mbps:6.1f} MB/s/obj  "
          f"cpu={r.cpu_pct:5.1f}%{ttfb_note}"
          + (f"  ERRORS={len(errors)}" if errors else ""))
    return r


def main() -> int:
    ap = argparse.ArgumentParser(description="hippius-s3 upload/download throughput benchmark")
    ap.add_argument("--endpoint", default="https://s3.hippius.com")
    ap.add_argument("--keep", action="store_true", help="leave the benchmark bucket in place")
    ap.add_argument("--prefix", default="throughput", help="bucket name prefix")
    ap.add_argument("--smoke", action="store_true", help="tiny plan — validates the harness, not the gateway")
    args = ap.parse_args()

    cfg = config.load()
    cfg.endpoint_url = args.endpoint
    client = s3util.make_client(cfg)
    iface = _default_iface()
    ncpu = len(
        [ln for ln in pathlib.Path("/proc/cpuinfo").read_text().splitlines() if ln.startswith("processor")])

    bucket = f"{args.prefix}-{int(time.time())}"
    print(f"== throughput benchmark ==\n   target: {cfg.endpoint_url}\n   bucket: {bucket}\n"
          f"   client: {ncpu} vCPU, iface {iface}, mpu chunk {MPU_CHUNK // MiB} MiB\n")
    s3util.ensure_bucket(client, bucket)

    # (size_label, size, upload concurrency, object count) — the corpus is written once and re-read
    # by the download rungs, so total bytes WRITTEN is just the sum of these.
    upload_plan = [
        ("64MiB", 64 * MiB, 1, 8),
        ("64MiB", 64 * MiB, 4, 16),
        ("64MiB", 64 * MiB, 8, 16),
        ("64MiB", 64 * MiB, 16, 16),
        ("256MiB", 256 * MiB, 1, 4),
        ("256MiB", 256 * MiB, 8, 8),
        ("1GiB", GiB, 1, 2),
    ]
    if args.smoke:
        upload_plan = [("8MiB", 8 * MiB, 1, 2), ("8MiB", 8 * MiB, 4, 4), ("32MiB", 32 * MiB, 2, 2)]
    written = sum(s * n for _, s, _, n in upload_plan)
    print(f"   plan: {written / 1e9:.1f} GB written across {sum(n for *_, n in upload_plan)} objects\n")

    rungs: list[Rung] = []
    corpus: dict[str, list[str]] = {}

    def upload_one(size: int, intra: int):
        tc = TransferConfig(multipart_threshold=MPU_CHUNK, multipart_chunksize=MPU_CHUNK,
                            max_concurrency=intra, use_threads=intra > 1)

        def _do(key: str):
            t0 = time.time()
            try:
                client.upload_fileobj(SyntheticFile(size, key.encode()), bucket, key, Config=tc)
            except Exception as e:  # noqa: BLE001 — one object's failure must not kill the rung
                return ("err", time.time() - t0, None, f"{type(e).__name__}: {str(e)[:120]}")
            return ("ok", time.time() - t0, None, "")
        return _do

    def download_one(key: str):
        t0 = time.time()
        try:
            body = client.get_object(Bucket=bucket, Key=key)["Body"]
            first = body.read(1)
            ttfb = time.time() - t0
            n = len(first)
            while True:
                chunk = body.read(MiB)  # read-and-discard: constant memory, so 1 GiB objects are fine
                if not chunk:
                    break
                n += len(chunk)
            body.close()
        except Exception as e:  # noqa: BLE001
            return ("err", time.time() - t0, None, f"{type(e).__name__}: {str(e)[:120]}")
        return ("ok", time.time() - t0, ttfb, "")

    print("-- upload --")
    for label, size, conc, count in upload_plan:
        keys = [f"{label}/c{conc}/obj-{i}" for i in range(count)]
        # intra=1: every object is a single stream, so `conc` is whole-object parallelism and nothing
        # else. Mixing in per-object transfer threads would conflate two kinds of concurrency.
        rungs.append(_run_rung("upload", label, size, conc, keys, upload_one(size, 1), iface))
        if len(keys) > len(corpus.get(label, [])):
            corpus[label] = keys

    # Downloads re-read the corpus, so they add zero written bytes. NOTE: these objects were written
    # seconds ago and are still in the ingest SSD cache — this measures the WARM read path. A cold
    # read (drained copy via CephFS/Arion) is a different, slower number and needs cluster access to
    # force. Reported as such; do not quote these as cold-read figures.
    download_plan = [
        ("64MiB", 64 * MiB, 1), ("64MiB", 64 * MiB, 4), ("64MiB", 64 * MiB, 8), ("64MiB", 64 * MiB, 16),
        ("256MiB", 256 * MiB, 1), ("256MiB", 256 * MiB, 8),
        ("1GiB", GiB, 1), ("1GiB", GiB, 2),
    ]
    if args.smoke:
        download_plan = [("8MiB", 8 * MiB, 1), ("8MiB", 8 * MiB, 4), ("32MiB", 32 * MiB, 2)]
    print("\n-- download (WARM: objects just written, likely ingest-cache hits) --")
    for label, size, conc in download_plan:
        avail = corpus.get(label, [])
        if not avail:
            continue
        # At least 4 objects even at C=1, so the per-object median isn't a single sample.
        keys = avail[:min(len(avail), max(conc, 4))]
        rungs.append(_run_rung("download", label, size, conc, keys, download_one, iface))

    out_dir = pathlib.Path(__file__).resolve().parent / "results"
    out_dir.mkdir(exist_ok=True)
    stamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())

    lines = [f"# Throughput benchmark — {cfg.endpoint_url}", "",
             f"Client: {ncpu} vCPU. Bucket `{bucket}`. MPU chunk {MPU_CHUNK // MiB} MiB. "
             f"{written / 1e9:.1f} GB written.", "",
             "| Phase | Size | Conc | Objs | Wall s | Agg MB/s | Per-obj MB/s | TTFB p50 | TTFB p95 | "
             "Client CPU % | Wire MB | Errors |", "|---|---|---|---|---|---|---|---|---|---|---|---|"]
    for r in rungs:
        t50 = f"{_pct(r.ttfb_s, 0.5)*1000:.0f} ms" if r.ttfb_s else "—"
        t95 = f"{_pct(r.ttfb_s, 0.95)*1000:.0f} ms" if r.ttfb_s else "—"
        lines.append(f"| {r.phase} | {r.size_label} | {r.concurrency} | {r.objects} | {r.wall_s:.1f} | "
                     f"{r.agg_mbps:.1f} | {r.per_obj_mbps:.1f} | {t50} | {t95} | {r.cpu_pct:.1f} | "
                     f"{r.wire_mb:.0f} | {len(r.errors)} |")
    lines += ["", f"Client CPU is out of {100 * ncpu}% (all cores). A rung near that ceiling is "
                  "client-bound, not gateway-bound.", "",
              "Download rungs are WARM (objects written seconds earlier, served from the ingest cache). "
              "They are not cold-read / drained-copy figures."]
    errs = [e for r in rungs for e in r.errors]
    if errs:
        lines += ["", "## Errors", ""] + [f"- {e}" for e in errs[:20]]
    md = "\n".join(lines)

    (out_dir / f"throughput-{stamp}.md").write_text(md)
    (out_dir / f"throughput-{stamp}.json").write_text(json.dumps(
        [dataclasses.asdict(r) | {"agg_mbps": r.agg_mbps, "per_obj_mbps": r.per_obj_mbps} for r in rungs],
        indent=2))

    if not args.keep:
        print("\n-- cleanup --")
        s3util.delete_bucket_recursive(client, bucket)
    else:
        print(f"\n-- keeping bucket {bucket} --")

    print("\n" + md)
    print(f"\nresults: stress-test/results/throughput-{stamp}.md")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
