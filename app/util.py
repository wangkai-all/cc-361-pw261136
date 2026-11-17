"""Small shared helpers (time, ids, canonical JSON, atomic files)."""
from __future__ import annotations

import errno
import hashlib
import json
import os
import secrets
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def utcnow() -> str:
    """A sortable UTC timestamp with microsecond precision."""
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="microseconds")
        .replace("+00:00", "Z")
    )


def new_id(prefix: str) -> str:
    return f"{prefix}_{secrets.token_hex(6)}"


def sha256_hex(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def canonical_json_bytes(obj) -> bytes:
    return json.dumps(
        obj, sort_keys=True, separators=(",", ":"), ensure_ascii=False
    ).encode("utf-8")


def canonical_json_text(obj) -> str:
    return canonical_json_bytes(obj).decode("utf-8")


def digest_of(obj) -> str:
    return sha256_hex(canonical_json_bytes(obj))


def read_json(path: Path):
    with open(path, "rb") as fh:
        return json.loads(fh.read().decode("utf-8"))


def _fsync_dir(path: Path) -> None:
    try:
        fd = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def atomic_write(path: Path, data: bytes, fsync: bool = True) -> None:
    """Write a file via temp + rename. Never leaves a partial destination."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp_name = tempfile.mkstemp(prefix=f".{path.name}.", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as fh:
            fh.write(data)
            fh.flush()
            if fsync:
                os.fsync(fh.fileno())
        os.replace(tmp_name, path)
        _fsync_dir(path.parent)
    except BaseException:
        try:
            os.unlink(tmp_name)
        except OSError:
            pass
        raise


def atomic_write_json(path: Path, obj, fsync: bool = True) -> None:
    atomic_write(path, canonical_json_bytes(obj), fsync=fsync)


def short(value: str | None, n: int = 12) -> str:
    if not value:
        return ""
    return value if len(value) <= n else value[:n]


def ensure_relative_inside(base: Path, candidate: Path) -> Path:
    base = base.resolve()
    candidate = candidate.resolve()
    if base != candidate and base not in candidate.parents:
        raise ValueError(f"path escapes {base}: {candidate}")
    return candidate


def file_uri_for(path: Path) -> str:
    return path.as_uri()


def remove_quietly(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    except OSError as exc:
        if exc.errno != errno.ENOENT:
            raise
