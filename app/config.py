"""Sealed Wire Vault — configuration and filesystem layout.

Every component (web server, capture worker, seal worker, parse worker) resolves
configuration from the environment so the processes can be started completely
separately and still agree on the same data directory.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent


@dataclass(frozen=True)
class Config:
    data_dir: Path
    rules_dir: Path
    fixtures_dir: Path
    chunk_min: int = 512
    chunk_avg: int = 2048
    chunk_max: int = 8192
    max_capture_bytes: int = 5 * 1024 * 1024
    demo_tools: bool = True
    fetch_timeout: float = 15.0
    tool_version: str = "1.0.0"

    @property
    def db_path(self) -> Path:
        return self.data_dir / "vault.db"

    @property
    def chunk_dir(self) -> Path:
        return self.data_dir / "chunks"

    @property
    def manifest_dir(self) -> Path:
        return self.data_dir / "manifests"

    @property
    def staging_dir(self) -> Path:
        return self.data_dir / "staging"

    @property
    def log_dir(self) -> Path:
        return self.data_dir / "logs"

    @property
    def quarantine_dir(self) -> Path:
        return self.data_dir / "quarantine"

    def ensure(self) -> "Config":
        for path in (
            self.data_dir,
            self.chunk_dir,
            self.manifest_dir,
            self.staging_dir,
            self.log_dir,
            self.quarantine_dir,
        ):
            path.mkdir(parents=True, exist_ok=True)
        return self


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    return raw.strip().lower() not in {"0", "false", "no", "off", ""}


def load_config(env=None) -> Config:
    env = os.environ if env is None else env
    data_dir = Path(env.get("VAULT_DATA_DIR", PROJECT_ROOT / "data")).resolve()
    rules_dir = Path(env.get("VAULT_RULES_DIR", PROJECT_ROOT / "rules")).resolve()
    fixtures_dir = Path(env.get("VAULT_FIXTURES_DIR", PROJECT_ROOT / "fixtures")).resolve()
    return Config(
        data_dir=data_dir,
        rules_dir=rules_dir,
        fixtures_dir=fixtures_dir,
        chunk_min=int(env.get("VAULT_CHUNK_MIN", "512")),
        chunk_avg=int(env.get("VAULT_CHUNK_AVG", "2048")),
        chunk_max=int(env.get("VAULT_CHUNK_MAX", "8192")),
        max_capture_bytes=int(env.get("VAULT_MAX_CAPTURE_BYTES", str(5 * 1024 * 1024))),
        demo_tools=_env_bool("VAULT_DEMO_TOOLS", True),
        fetch_timeout=float(env.get("VAULT_FETCH_TIMEOUT", "15")),
        tool_version=env.get("VAULT_TOOL_VERSION", "1.0.0"),
    ).ensure()


def origin_base(env=None) -> str:
    env = os.environ if env is None else env
    return env.get("VAULT_ORIGIN_BASE", "http://127.0.0.1:8811").rstrip("/")


def project_rel(path: Path) -> str:
    """Store paths relative to the data dir when possible (portable manifests)."""
    try:
        return str(Path(path).resolve().relative_to(PROJECT_ROOT))
    except ValueError:
        return str(path)
