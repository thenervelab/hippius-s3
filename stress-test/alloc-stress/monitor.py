"""WI-19 §4.6 Rig A sampler — 250 ms snapshot of allocator coordinator state → JSONL.

The allocator only exports its gauges over OTLP (no HTTP /metrics), and Rig A has no collector, so
leadership/epoch/budget are read straight from the Redis Coordinator keys instead (which are the
actual source of truth the split-brain core defends):

  cephor:leader        -> JSON {"instance": "<id>", "epoch": N}   (the singleton lease holder)
  cephor:epoch         -> INCR counter (current fencing era)
  cephor:alloc:<node>  -> JSON {"budget": "<bytes/s>", "epoch": N} (per-node budget, epoch-stamped)

Each 250 ms tick appends one JSON line:
  {"t": <unix>, "leader": "<id>|null", "leader_epoch": N, "epoch": N,
   "allocs": {"<node>": {"budget": <int>, "epoch": N}, ...},
   "sum_budget": <int>, "stale_allocs": [<node>, ...]}

`stale_allocs` = alloc keys whose stamped epoch is BELOW the current epoch counter — the fence
should make these transient (≤ one alloc-TTL). gate.py turns these lines into PASS/FAIL.

Usage: python monitor.py --redis redis://localhost:6390 --out run.jsonl --interval 0.25 --duration 600
"""

from __future__ import annotations

import argparse
import json
import sys
import time

import redis  # redis-py (sync) — the sampler is a tiny standalone loop


def _num(v: object) -> int:
    try:
        return int(v)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return 0


def sample(r: redis.Redis) -> dict:
    leader_raw = r.get("cephor:leader")
    leader = None
    leader_epoch = 0
    if leader_raw:
        try:
            d = json.loads(leader_raw)
            leader = d.get("instance")
            leader_epoch = _num(d.get("epoch"))
        except (ValueError, TypeError):
            pass

    epoch = _num(r.get("cephor:epoch"))

    allocs: dict[str, dict] = {}
    for key in r.scan_iter(match="cephor:alloc:*", count=100):
        node = key.split("cephor:alloc:", 1)[-1] if isinstance(key, str) else key.decode().split("cephor:alloc:", 1)[-1]
        raw = r.get(key)
        if not raw:
            continue
        try:
            d = json.loads(raw)
        except (ValueError, TypeError):
            continue
        allocs[node] = {"budget": _num(d.get("budget")), "epoch": _num(d.get("epoch"))}

    sum_budget = sum(a["budget"] for a in allocs.values())
    # A stale alloc = a budget still stamped with an epoch below the live counter (a deposed
    # leader's write that should have been fenced / should expire within one alloc-TTL).
    stale = [node for node, a in allocs.items() if epoch and a["epoch"] < epoch]

    return {
        "t": time.time(),
        "leader": leader,
        "leader_epoch": leader_epoch,
        "epoch": epoch,
        "allocs": allocs,
        "sum_budget": sum_budget,
        "stale_allocs": stale,
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis", default="redis://localhost:6390", help="redis-queues URL (host-mapped)")
    ap.add_argument("--out", required=True, help="JSONL output path")
    ap.add_argument("--interval", type=float, default=0.25)
    ap.add_argument("--duration", type=float, default=600.0)
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis, decode_responses=True, socket_timeout=2)
    deadline = time.monotonic() + args.duration
    n = 0
    with open(args.out, "w") as fh:
        while time.monotonic() < deadline:
            tick_start = time.monotonic()
            try:
                snap = sample(r)
            except redis.RedisError as exc:
                snap = {"t": time.time(), "error": str(exc)}
            fh.write(json.dumps(snap) + "\n")
            fh.flush()
            n += 1
            # Hold the cadence: sleep the remainder of the interval, never negative.
            time.sleep(max(0.0, args.interval - (time.monotonic() - tick_start)))
    print(f"monitor: wrote {n} samples to {args.out}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
