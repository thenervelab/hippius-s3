"""Harness configuration: S3 endpoint + creds, and staging cluster access."""

from __future__ import annotations

import dataclasses
import os
import pathlib


REPO_ROOT = pathlib.Path(__file__).resolve().parents[2]
DEFAULT_ENV_FILE = REPO_ROOT / ".aws.cli.env"


@dataclasses.dataclass
class Config:
    endpoint_url: str
    access_key: str
    secret_key: str
    region: str
    # cluster access (optional — S3-facing scenarios run without it; invariant probes need it)
    namespace: str
    monitoring_namespace: str
    pg_pod: str
    pg_db: str
    prometheus_svc: str
    # knobs
    bucket_prefix: str
    drain_convergence_timeout_s: int
    keep_objects: bool


def _load_env_file(path: pathlib.Path) -> dict[str, str]:
    """Parse a `.aws.cli.env`-style file (KEY=VALUE / export KEY=VALUE). Secrets stay in-process."""
    out: dict[str, str] = {}
    if not path.exists():
        return out
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        line = line.removeprefix("export ").strip()
        if "=" not in line:
            continue
        key, _, val = line.partition("=")
        out[key.strip()] = val.strip().strip('"').strip("'")
    return out


def load(env_file: pathlib.Path | None = None) -> Config:
    env = _load_env_file(env_file or DEFAULT_ENV_FILE)

    def pick(name: str, default: str | None = None) -> str:
        val = os.environ.get(name) or env.get(name) or default
        if val is None:
            raise SystemExit(f"missing required config: {name} (set it in .aws.cli.env or the environment)")
        return val

    return Config(
        endpoint_url=pick("HIPPIUS_S3_ENDPOINT", "https://s3-staging.hippius.com"),
        access_key=pick("AWS_ACCESS_KEY_ID"),
        secret_key=pick("AWS_SECRET_ACCESS_KEY"),
        region=pick("AWS_DEFAULT_REGION", "decentralized"),
        namespace=os.environ.get("HIPPIUS_NS", "hippius-s3-staging"),
        monitoring_namespace=os.environ.get("HIPPIUS_MON_NS", "monitoring"),
        pg_pod=os.environ.get("HIPPIUS_PG_POD", "postgres-1"),
        pg_db=os.environ.get("HIPPIUS_PG_DB", "hippius"),
        prometheus_svc=os.environ.get("HIPPIUS_PROM_SVC", "prometheus-server"),
        bucket_prefix=os.environ.get("HIPPIUS_BUCKET_PREFIX", "prodgate"),
        drain_convergence_timeout_s=int(os.environ.get("HIPPIUS_DRAIN_TIMEOUT_S", "300")),
        keep_objects=os.environ.get("HIPPIUS_KEEP_OBJECTS", "").lower() in ("1", "true", "yes"),
    )
