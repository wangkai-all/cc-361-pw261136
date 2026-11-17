"""The sealed vault: chunk storage, manifests, verification and packages.

Design choices (see docs/design.md for the full comparison):

* Bytes are stored **by content**, not by object. A chunk file is named by the
  SHA-256 of its bytes and is shared by every sealed object that contains it.
  This keeps storage proportional to novel content, lets a withdrawn source's
  bytes remain readable for objects that already reference them, and localises
  damage: one bad chunk degrades exactly the records/objects that depend on it.
* An object is defined by an immutable manifest (content-addressed too) that
  lists every chunk in order with its offset, length and digest, plus a Merkle
  root over the chunk digests and the SHA-256 of the whole byte stream. "Which
  original packet?" has one answer: the object id + manifest digest.
* Whole-object retention is not used as the storage strategy; it would triple
  storage for near-identical captures and make a single corrupt byte hide an
  entire object. Instead the manifest *is* the exact original: concatenating its
  chunks reproduces the captured stream byte for byte, and the stream digest
  proves it.
"""
from __future__ import annotations

import hashlib
import io
import json
import os
import sqlite3
import zipfile
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Iterator

from .chunking import IncrementalChunker
from .config import Config
from .db import log_event, transaction
from .util import (
    atomic_write,
    atomic_write_json,
    canonical_json_bytes,
    canonical_json_text,
    new_id,
    sha256_hex,
    utcnow,
)

MANIFEST_FORMAT = "sealed-wire-manifest/1"
CHUNK_ALGORITHM = "fastcdc-gear-v1"
# Verification statuses for one chunk in one object.
CHUNK_OK = "ok"
CHUNK_MISSING = "missing"
CHUNK_CORRUPT = "corrupt"


# --------------------------------------------------------------------------
# paths and hashing helpers
# --------------------------------------------------------------------------

def chunk_path(cfg: Config, digest: str) -> Path:
    if len(digest) != 64 or any(c not in "0123456789abcdef" for c in digest):
        raise ValueError(f"bad chunk digest: {digest!r}")
    return cfg.chunk_dir / digest[:2] / digest[2:4] / digest


def quarantine_path(cfg: Config, digest: str) -> Path:
    return cfg.quarantine_dir / f"{digest}.orig"


def manifest_path(cfg: Config, manifest_digest: str) -> Path:
    if len(manifest_digest) != 64 or any(c not in "0123456789abcdef" for c in manifest_digest):
        raise ValueError(f"bad manifest digest: {manifest_digest!r}")
    return cfg.manifest_dir / f"{manifest_digest}.json"


def merkle_root(digests: list[str]) -> str:
    """Binary Merkle root over chunk digests, domain-separated by level."""
    if not digests:
        return sha256_hex(b"")
    level = [hashlib.sha256(b"\x00" + bytes.fromhex(d)).digest() for d in digests]
    while len(level) > 1:
        nxt = []
        for i in range(0, len(level), 2):
            left = level[i]
            right = level[i + 1] if i + 1 < len(level) else left
            nxt.append(hashlib.sha256(b"\x01" + left + right).digest())
        level = nxt
    return level[0].hex()


def merkle_proof(digests: list[str], index: int) -> list[dict]:
    """Sibling path for one leaf; used by tests and the package verifier."""
    if not digests:
        raise ValueError("empty tree")
    level = [hashlib.sha256(b"\x00" + bytes.fromhex(d)).digest() for d in digests]
    idx = index
    proof = []
    while len(level) > 1:
        if idx % 2 == 0:
            sib_idx = idx + 1 if idx + 1 < len(level) else idx
            proof.append({"side": "right", "hash": level[sib_idx].hex()})
        else:
            proof.append({"side": "left", "hash": level[idx - 1].hex()})
        nxt = []
        for i in range(0, len(level), 2):
            left = level[i]
            right = level[i + 1] if i + 1 < len(level) else left
            nxt.append(hashlib.sha256(b"\x01" + left + right).digest())
        level = nxt
        idx //= 2
    return proof


# --------------------------------------------------------------------------
# chunk writing
# --------------------------------------------------------------------------

def store_chunk(cfg: Config, data: bytes, digest: str | None = None) -> tuple[str, bool]:
    """Store one chunk. Returns (digest, newly_created).

    Writes are atomic and the final file is read-only. If the chunk already
    exists we only sanity-check its size; full hash checks happen in verify.
    """
    digest = digest or sha256_hex(data)
    if sha256_hex(data) != digest:
        raise ValueError("chunk digest does not match data")
    path = chunk_path(cfg, digest)
    if path.exists():
        try:
            if path.stat().st_size == len(data):
                return digest, False
        except OSError:
            pass
        # Existing file is wrong-sized: leave it alone; verification will flag
        # it and the demo repair path can restore a quarantined original.
        return digest, False
    atomic_write(path, data)
    try:
        os.chmod(path, 0o444)
    except OSError:
        pass
    return digest, True


def read_chunk(cfg: Config, digest: str) -> bytes | None:
    path = chunk_path(cfg, digest)
    try:
        return path.read_bytes()
    except OSError:
        return None


# --------------------------------------------------------------------------
# sealing
# --------------------------------------------------------------------------

@dataclass
class CaptureMeta:
    capture_id: str
    source_id: str
    source_name: str
    source_origin: str
    source_url: str
    fetched_url: str
    http_status: int | None
    content_type: str | None
    headers: dict
    started_at: str
    finished_at: str = ""
    requested_headers: dict = field(default_factory=dict)


@dataclass
class SealedObject:
    object_id: str
    manifest_digest: str
    root_digest: str
    stream_digest: str
    total_bytes: int
    chunk_count: int
    duplicate_bytes: int = 0


class SizeLimitExceeded(RuntimeError):
    pass


# --------------------------------------------------------------------------
# staging (used by the split fetch -> seal pipeline)
# --------------------------------------------------------------------------

def staging_path(cfg: Config, capture_id: str) -> Path:
    if not capture_id or not all(c.isalnum() or c in "_-" for c in capture_id):
        raise ValueError(f"bad capture id: {capture_id!r}")
    return cfg.staging_dir / f"{capture_id}.staged"


def stage_stream(cfg: Config, capture_id: str, chunks: Iterable[bytes], *,
                 max_bytes: int | None = None, on_progress=None) -> dict:
    """Write a fetched byte stream verbatim into staging. No vault writes.

    The fetcher's only durable output is this file plus its metadata; it never
    touches the chunk store or manifests. A later, independent seal worker reads
    staging and does all vault writes, so a fetch can never partially seal.
    """
    max_bytes = cfg.max_capture_bytes if max_bytes is None else max_bytes
    path = staging_path(cfg, capture_id)
    tmp = path.with_suffix(".partial")
    total = 0
    h = hashlib.sha256()
    tmp.parent.mkdir(parents=True, exist_ok=True)
    try:
        with open(tmp, "wb") as fh:
            for piece in chunks:
                if not piece:
                    continue
                if total + len(piece) > max_bytes:
                    raise SizeLimitExceeded(
                        f"capture exceeds limit of {max_bytes} bytes "
                        f"(at {total} + {len(piece)})"
                    )
                fh.write(piece)
                h.update(piece)
                total += len(piece)
                if on_progress:
                    on_progress(total)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, path)
        _fsync_parent(path)
    except BaseException:
        try:
            tmp.unlink()
        except OSError:
            pass
        raise
    return {"path": str(path), "bytes": total, "sha256": h.hexdigest()}


def _fsync_parent(path: Path) -> None:
    from .util import _fsync_dir

    _fsync_dir(path.parent)


def staged_chunks(path: Path, block: int = 1 << 16):
    """Yield a staged file's bytes in blocks; verifies length only, not content."""
    with open(path, "rb") as fh:
        while True:
            data = fh.read(block)
            if not data:
                break
            yield data


def _staged_chunks(path: Path, block: int = 1 << 16):
    yield from staged_chunks(path, block)


class StagingMismatch(RuntimeError):
    pass


def seal_staged(cfg: Config, conn, capture_row, *, worker: str = "seal-worker",
                on_progress=None) -> SealedObject:
    """Seal staged bytes into the vault. Asserts the bytes match the fetch digest.

    The fetch stage recorded the SHA-256 and length of exactly what it wrote.
    Re-hashing here means the sealed object provably equals the fetched response,
    closing the gap between "fetch happened" and "these are the sealed bytes".
    """
    staged_path = capture_row["staged_path"]
    if not staged_path:
        raise StagingMismatch(f"capture {capture_row['id']} has no staged file")
    meta_raw = capture_row["staged_meta_json"] or "{}"
    try:
        staged_meta = json.loads(meta_raw)
    except json.JSONDecodeError as exc:
        raise StagingMismatch(f"staged metadata is not valid JSON: {exc}")
    path = Path(staged_path)
    if not path.exists():
        raise StagingMismatch(f"staged file disappeared: {path}")

    def verified():
        h = hashlib.sha256()
        total = 0
        for block in _staged_chunks(path):
            h.update(block)
            total += len(block)
            yield block
        if total != staged_meta.get("bytes") or h.hexdigest() != staged_meta.get("sha256"):
            raise StagingMismatch(
                "staged bytes do not match the fetch digest "
                f"(expected {staged_meta.get('bytes')}B {staged_meta.get('sha256')}, "
                f"got {total}B {h.hexdigest()})"
            )

    meta = CaptureMeta(
        capture_id=capture_row["id"],
        source_id=capture_row["source_id"],
        source_name=staged_meta.get("source_name", capture_row["source_id"]),
        source_origin=staged_meta.get("source_origin", ""),
        source_url=staged_meta.get("source_url", ""),
        fetched_url=staged_meta.get("fetched_url", ""),
        http_status=staged_meta.get("http_status"),
        content_type=staged_meta.get("content_type"),
        headers=staged_meta.get("headers", {}),
        started_at=staged_meta.get("started_at") or utcnow(),
        requested_headers=staged_meta.get("request_headers", {}),
    )
    sealed = seal_stream(cfg, conn, verified(), meta, on_progress=on_progress, actor=worker)
    try:
        path.unlink()
    except OSError:
        pass
    return sealed


def seal_stream(
    cfg: Config,
    conn: sqlite3.Connection,
    chunks: Iterable[bytes],
    meta: CaptureMeta,
    *,
    max_bytes: int | None = None,
    on_progress=None,
    actor: str = "capture-worker",
) -> SealedObject:
    """Consume a byte stream, store chunks, and commit an immutable object.

    Chunks land on disk before the DB transaction, so a crash mid-seal leaves
    only unreferenced (harmless, reusable) chunk files and no half object.
    """
    max_bytes = cfg.max_capture_bytes if max_bytes is None else max_bytes
    chunker = IncrementalChunker(cfg.chunk_min, cfg.chunk_avg, cfg.chunk_max)
    entries: list[dict] = []
    seen: set[str] = set()
    duplicate_bytes = 0
    total = 0
    store_tmp: list[tuple[int, int, bytes]] = []

    def handle(items):
        nonlocal duplicate_bytes, total
        for start, end, data in items:
            digest, created = store_chunk(cfg, data)
            if not created and digest in seen:
                duplicate_bytes += len(data)
            seen.add(digest)
            entries.append(
                {
                    "seq": len(entries),
                    "digest": digest,
                    "offset": start,
                    "length": len(data),
                }
            )
            total += len(data)
            if on_progress:
                on_progress(total, len(entries))

    for piece in chunks:
        if not piece:
            continue
        if total + len(piece) > max_bytes:
            raise SizeLimitExceeded(
                f"capture exceeds limit of {max_bytes} bytes (at {total} + {len(piece)})"
            )
        handle(chunker.feed(piece))
    handle(chunker.flush())

    stream_digest = chunker.hexdigest
    if total != sum(e["length"] for e in entries):
        raise RuntimeError("internal accounting error while sealing")
    if not entries:
        # Zero-byte source: define a single empty chunk so the manifest is valid.
        digest, _ = store_chunk(cfg, b"")
        entries.append({"seq": 0, "digest": digest, "offset": 0, "length": 0})
        seen.add(digest)

    object_id = new_id("obj")
    created_at = utcnow()
    manifest = {
        "format": MANIFEST_FORMAT,
        "object_id": object_id,
        "source": {
            "id": meta.source_id,
            "name": meta.source_name,
            "origin": meta.source_origin,
            "url": meta.source_url,
        },
        "capture": {
            "id": meta.capture_id,
            "started_at": meta.started_at,
            "finished_at": meta.finished_at or created_at,
            "fetched_url": meta.fetched_url,
            "http_status": meta.http_status,
            "content_type": meta.content_type,
            "request_headers": meta.requested_headers,
            "response_headers": meta.headers,
        },
        "stream": {"sha256": stream_digest, "total_bytes": total},
        "chunking": {
            "algorithm": CHUNK_ALGORITHM,
            "min": cfg.chunk_min,
            "avg": cfg.chunk_avg,
            "max": cfg.chunk_max,
        },
        "chunks": entries,
        "root_digest": merkle_root([e["digest"] for e in entries]),
        "tool_version": cfg.tool_version,
        "created_at": created_at,
    }
    manifest_bytes = canonical_json_bytes(manifest)
    manifest_digest = sha256_hex(manifest_bytes)
    atomic_write(manifest_path(cfg, manifest_digest), manifest_bytes)
    try:
        os.chmod(manifest_path(cfg, manifest_digest), 0o444)
    except OSError:
        pass

    with transaction(conn):
        conn.execute(
            """INSERT INTO objects(id, capture_id, source_id, created_at, total_bytes,
                    chunk_count, root_digest, stream_digest, manifest_digest, manifest_path,
                    content_type, http_status, fetched_url, headers_json,
                    tool_version, algorithm, compression)
               VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                object_id,
                meta.capture_id,
                meta.source_id,
                created_at,
                total,
                len(entries),
                manifest["root_digest"],
                stream_digest,
                manifest_digest,
                str(manifest_path(cfg, manifest_digest)),
                meta.content_type,
                meta.http_status,
                meta.fetched_url,
                canonical_json_text(meta.headers),
                cfg.tool_version,
                f"{CHUNK_ALGORITHM}({cfg.chunk_min}/{cfg.chunk_avg}/{cfg.chunk_max})",
                "none",
            ),
        )
        for e in entries:
            conn.execute(
                "INSERT OR IGNORE INTO chunks(digest, size, first_seen_at, refcount, missing) "
                "VALUES(?,?,?,0,0)",
                (e["digest"], e["length"], created_at),
            )
            conn.execute(
                "INSERT INTO object_chunks(object_id, seq, digest, offset, length) "
                "VALUES(?,?,?,?,?)",
                (object_id, e["seq"], e["digest"], e["offset"], e["length"]),
            )
        # Refcount = number of distinct objects referencing the digest.
        for digest in seen:
            conn.execute(
                "UPDATE chunks SET refcount = refcount + 1 WHERE digest = ?", (digest,)
            )
        conn.execute(
            "UPDATE captures SET status='sealed', finished_at=?, sealed_object_id=?, "
            "bytes_captured=?, http_status=?, content_type=?, fetched_url=?, "
            "headers_json=? WHERE id=?",
            (
                created_at,
                object_id,
                total,
                meta.http_status,
                meta.content_type,
                meta.fetched_url,
                canonical_json_text(meta.headers),
                meta.capture_id,
            ),
        )
        log_event(
            conn,
            "object.sealed",
            "object",
            object_id,
            {
                "source_id": meta.source_id,
                "manifest_digest": manifest_digest,
                "root_digest": manifest["root_digest"],
                "stream_sha256": stream_digest,
                "total_bytes": total,
                "chunks": len(entries),
                "duplicate_bytes": duplicate_bytes,
            },
            actor=actor,
        )
    return SealedObject(
        object_id=object_id,
        manifest_digest=manifest_digest,
        root_digest=manifest["root_digest"],
        stream_digest=stream_digest,
        total_bytes=total,
        chunk_count=len(entries),
        duplicate_bytes=duplicate_bytes,
    )


# --------------------------------------------------------------------------
# reading and integrity
# --------------------------------------------------------------------------

class ObjectDamageError(RuntimeError):
    def __init__(self, message: str, report: dict | None = None):
        super().__init__(message)
        self.report = report or {}


@dataclass
class ObjectManifest:
    manifest: dict
    manifest_digest: str
    raw: bytes

    @property
    def chunks(self) -> list[dict]:
        return self.manifest["chunks"]

    @property
    def total_bytes(self) -> int:
        return self.manifest["stream"]["total_bytes"]

    @property
    def stream_digest(self) -> str:
        return self.manifest["stream"]["sha256"]

    @property
    def root_digest(self) -> str:
        return self.manifest["root_digest"]


def load_manifest(cfg: Config, manifest_digest: str) -> ObjectManifest:
    path = manifest_path(cfg, manifest_digest)
    raw = path.read_bytes()
    actual = sha256_hex(raw)
    if actual != manifest_digest:
        raise ObjectDamageError(
            f"manifest digest mismatch for {manifest_digest}: file hashes to {actual}",
            {"status": "corrupt", "manifest_digest": manifest_digest, "actual": actual},
        )
    manifest = json.loads(raw.decode("utf-8"))
    if manifest.get("format") != MANIFEST_FORMAT:
        raise ObjectDamageError(f"unsupported manifest format {manifest.get('format')!r}")
    return ObjectManifest(manifest=manifest, manifest_digest=manifest_digest, raw=raw)


def get_object(conn, object_id_or_digest: str):
    """Resolve an object by id, manifest digest, root digest, or at least 8-char prefix."""
    row = conn.execute("SELECT * FROM objects WHERE id=?", (object_id_or_digest,)).fetchone()
    if row:
        return row
    row = conn.execute(
        "SELECT * FROM objects WHERE manifest_digest=? OR root_digest=?",
        (object_id_or_digest, object_id_or_digest),
    ).fetchone()
    if row:
        return row
    if len(object_id_or_digest) >= 8:
        rows = conn.execute(
            "SELECT * FROM objects WHERE id LIKE ? OR manifest_digest LIKE ? LIMIT 2",
            (object_id_or_digest + "%", object_id_or_digest + "%"),
        ).fetchall()
        if len(rows) == 1:
            return rows[0]
    return None


def object_chunks_rows(conn, object_id: str) -> list[sqlite3.Row]:
    return conn.execute(
        "SELECT seq, digest, offset, length FROM object_chunks WHERE object_id=? ORDER BY seq",
        (object_id,),
    ).fetchall()


def check_object(cfg: Config, conn, object_row, *, update_flags: bool = True) -> dict:
    """Verify every chunk of one object and produce a localised damage report.

    This is the only place that declares damage. It never deletes or rewrites
    bytes. `update_flags` mirrors the result into chunks.missing so result
    serving can decide which records are blocked without a full re-hash.
    """
    object_id = object_row["id"]
    report = {
        "object_id": object_id,
        "manifest_digest": object_row["manifest_digest"],
        "status": "ok",
        "checked_at": utcnow(),
        "chunks_total": 0,
        "chunks_ok": 0,
        "damaged": [],
        "affected_runs": [],
        "affected_records": 0,
        "total_bytes": object_row["total_bytes"],
    }
    try:
        om = load_manifest(cfg, object_row["manifest_digest"])
    except (OSError, ObjectDamageError) as exc:
        report.update(
            status="manifest_damaged",
            manifest_error=str(exc),
        )
        return report

    rows = object_chunks_rows(conn, object_id)
    report["chunks_total"] = len(rows)
    seen_digests = set()
    for row in rows:
        digest = row["digest"]
        path = chunk_path(cfg, digest)
        status = CHUNK_OK
        actual_size = None
        try:
            data = path.read_bytes()
            actual_size = len(data)
            if len(data) != row["length"] or sha256_hex(data) != digest:
                status = CHUNK_CORRUPT
        except FileNotFoundError:
            status = CHUNK_MISSING
        except OSError:
            status = CHUNK_CORRUPT
        if status == CHUNK_OK:
            report["chunks_ok"] += 1
        else:
            report["damaged"].append(
                {
                    "seq": row["seq"],
                    "digest": digest,
                    "offset": row["offset"],
                    "length": row["length"],
                    "status": status,
                    "actual_size": actual_size,
                }
            )
        seen_digests.add(digest)
        if update_flags:
            conn.execute(
                "UPDATE chunks SET missing=? WHERE digest=?",
                (0 if status == CHUNK_OK else 1, digest),
            )
    if report["damaged"]:
        report["status"] = "damaged"
    if update_flags:
        conn.commit()

    # Which parse runs touch the damaged chunks? A record is dependent when any
    # chunk in its byte range is damaged.
    damaged = {d["digest"] for d in report["damaged"]}
    if damaged:
        runs = conn.execute(
            "SELECT id FROM parse_runs WHERE object_id=? AND status IN ('done','running','paused','interrupted','failed')",
            (object_id,),
        ).fetchall()
        for r in runs:
            n = conn.execute(
                "SELECT COUNT(*) AS n FROM parse_records WHERE run_id=? AND EXISTS ("
                " SELECT 1 FROM json_each(parse_records.dep_digests) j WHERE j.value IN ("
                + ",".join("?" for _ in damaged)
                + "))",
                (r["id"], *damaged),
            ).fetchone()["n"]
            if n:
                report["affected_runs"].append({"run_id": r["id"], "blocked_records": n})
                report["affected_records"] += n
    log_event(
        conn,
        "object.verified",
        "object",
        object_id,
        {
            "status": report["status"],
            "damaged": [
                {"seq": d["seq"], "digest": d["digest"], "status": d["status"]}
                for d in report["damaged"]
            ],
            "affected_records": report["affected_records"],
        },
        actor="verifier",
    )
    conn.commit()
    try:
        atomic_write_json(
            cfg.data_dir / "reports" / "objects" / f"{object_id}.json", report, fsync=False
        )
    except OSError:
        pass
    return report


def read_object_bytes(cfg: Config, conn, object_row, *, limit: int | None = None) -> bytes:
    """Reassemble an object exactly; raises ObjectDamageError if any chunk fails."""
    report = check_object(cfg, conn, object_row, update_flags=False)
    if report["status"] != "ok":
        raise ObjectDamageError(
            f"object {object_row['id']} is {report['status']}", report
        )
    om = load_manifest(cfg, object_row["manifest_digest"])
    out = io.BytesIO()
    for row in object_chunks_rows(conn, object_row["id"]):
        data = read_chunk(cfg, row["digest"])
        if data is None:
            raise ObjectDamageError(f"chunk {row['digest']} missing")
        out.write(data)
        if limit is not None and out.tell() >= limit:
            break
    blob = out.getvalue()
    if sha256_hex(blob) != om.stream_digest:
        raise ObjectDamageError("reassembled stream digest does not match manifest")
    return blob


@dataclass
class ChunkView:
    seq: int
    digest: str
    offset: int
    length: int
    data: bytes | None
    status: str


def iter_object_chunks(cfg: Config, conn, object_row) -> Iterator[ChunkView]:
    """Yield every chunk with per-chunk status instead of failing the object."""
    om = load_manifest(cfg, object_row["manifest_digest"])
    for row in object_chunks_rows(conn, object_row["id"]):
        data = read_chunk(cfg, row["digest"])
        if data is None:
            status = CHUNK_MISSING
        elif len(data) != row["length"] or sha256_hex(data) != row["digest"]:
            status = CHUNK_CORRUPT
            data = None
        else:
            status = CHUNK_OK
        yield ChunkView(
            seq=row["seq"],
            digest=row["digest"],
            offset=row["offset"],
            length=row["length"],
            data=data,
            status=status,
        )


# --------------------------------------------------------------------------
# sharing / withdrawal helpers
# --------------------------------------------------------------------------

def content_key(conn, object_row) -> str:
    """Content identity of an object: its stream digest plus chunking recipe.

    Two captures of unchanged bytes produce different objects (each pins its own
    capture attempt and manifest, with different timestamps) but the same
    content key, and they share every chunk. Use this to find "the same packet".
    """
    return f"sha256:{object_row['stream_digest']}|{object_row['algorithm']}"


def chunk_share_summary(conn, object_id: str) -> dict:
    total = conn.execute(
        "SELECT COUNT(*) AS n, COALESCE(SUM(length),0) AS b FROM object_chunks WHERE object_id=?",
        (object_id,),
    ).fetchone()
    shared = conn.execute(
        """SELECT COUNT(*) AS n, COALESCE(SUM(oc.length),0) AS b
           FROM object_chunks oc JOIN chunks c ON c.digest=oc.digest
           WHERE oc.object_id=? AND c.refcount>1""",
        (object_id,),
    ).fetchone()
    unique = conn.execute(
        """SELECT COUNT(*) AS n, COALESCE(SUM(oc.length),0) AS b
           FROM object_chunks oc JOIN chunks c ON c.digest=oc.digest
           WHERE oc.object_id=? AND c.refcount=1""",
        (object_id,),
    ).fetchone()
    return {
        "chunks": total["n"],
        "bytes": total["b"],
        "shared_chunks": shared["n"],
        "shared_bytes": shared["b"],
        "unique_chunks": unique["n"],
        "unique_bytes": unique["b"],
    }


def objects_referencing_chunk(conn, digest: str) -> list[str]:
    return [
        r["object_id"]
        for r in conn.execute(
            "SELECT DISTINCT object_id FROM object_chunks WHERE digest=?", (digest,)
        ).fetchall()
    ]


def missing_chunk_digests(conn) -> set[str]:
    return {
        r["digest"]
        for r in conn.execute("SELECT digest FROM chunks WHERE missing=1").fetchall()
    }


# --------------------------------------------------------------------------
# demo damage / repair (reversible, gated by config.demo_tools)
# --------------------------------------------------------------------------

def simulate_damage(cfg: Config, conn, object_row, seq: int, mode: str = "corrupt") -> dict:
    """Move a chunk's true bytes to quarantine and leave damage behind.

    Demo-only. Quarantine keeps the original so `restore_chunk` can repair it,
    which lets the UI show that unrelated chunks, objects and results survive.
    """
    if not cfg.demo_tools:
        raise PermissionError("demo tools are disabled")
    row = conn.execute(
        "SELECT seq, digest, length FROM object_chunks WHERE object_id=? AND seq=?",
        (object_row["id"], seq),
    ).fetchone()
    if not row:
        raise ValueError(f"object has no chunk seq {seq}")
    digest = row["digest"]
    path = chunk_path(cfg, digest)
    if not path.exists():
        return {"changed": False, "reason": "chunk already absent", "digest": digest}
    original = path.read_bytes()
    qpath = quarantine_path(cfg, digest)
    if not qpath.exists():
        atomic_write(qpath, original)
    if mode == "missing":
        path.unlink()
    else:
        # Same length, different bytes, deterministic: proves hash detection.
        garbage = bytes((b ^ 0xA5) for b in original) or b"\x00"
        try:
            os.chmod(path, 0o644)
        except OSError:
            pass
        atomic_write(path, garbage)
        try:
            os.chmod(path, 0o444)
        except OSError:
            pass
    conn.execute("UPDATE chunks SET missing=1 WHERE digest=?", (digest,))
    conn.commit()
    log_event(
        conn,
        "demo.damage",
        "chunk",
        digest,
        {"object_id": object_row["id"], "seq": seq, "mode": mode},
        actor="demo",
    )
    conn.commit()
    report = check_object(cfg, conn, object_row)
    return {"changed": True, "digest": digest, "mode": mode, "report": report}


def restore_chunk(cfg: Config, conn, digest: str) -> dict:
    if not cfg.demo_tools:
        raise PermissionError("demo tools are disabled")
    qpath = quarantine_path(cfg, digest)
    if not qpath.exists():
        raise FileNotFoundError(f"no quarantined original for {digest}")
    original = qpath.read_bytes()
    if sha256_hex(original) != digest:
        raise ValueError("quarantined bytes do not match the digest")
    path = chunk_path(cfg, digest)
    try:
        os.chmod(path, 0o644)
    except OSError:
        pass
    atomic_write(path, original)
    try:
        os.chmod(path, 0o444)
    except OSError:
        pass
    qpath.unlink()
    conn.execute("UPDATE chunks SET missing=0 WHERE digest=?", (digest,))
    conn.commit()
    log_event(conn, "demo.repair", "chunk", digest, {}, actor="demo")
    conn.commit()
    return {"digest": digest, "restored": True}


# --------------------------------------------------------------------------
# standalone export package
# --------------------------------------------------------------------------

VERIFY_SCRIPT = '''#!/usr/bin/env python3
"""Standalone verifier for a Sealed Wire export package.

Verifies, using only this package and the Python standard library:
  1. the manifest file hashes to the manifest digest recorded in object.json;
  2. every chunk file exists, has the recorded length, and hashes to its digest;
  3. concatenating chunks in order reproduces the recorded stream SHA-256;
  4. the Merkle root over chunk digests matches the manifest;
  5. prints the exact object, manifest digest and parse versions this package pins.
Exit code 0 means every check passed.
"""
import hashlib, json, os, sys

def sha256(b):
    return hashlib.sha256(b).hexdigest()

def merkle(digests):
    if not digests:
        return sha256(b"")
    level = [hashlib.sha256(b"\\x00" + bytes.fromhex(d)).digest() for d in digests]
    while len(level) > 1:
        nxt = []
        for i in range(0, len(level), 2):
            left = level[i]
            right = level[i + 1] if i + 1 < len(level) else left
            nxt.append(hashlib.sha256(b"\\x01" + left + right).digest())
        level = nxt
    return level[0].hex()

def main(root):
    meta = json.load(open(os.path.join(root, "object.json"), "r", encoding="utf-8"))
    manifest_bytes = open(os.path.join(root, "manifest.json"), "rb").read()
    errors = []
    if sha256(manifest_bytes) != meta["manifest_digest"]:
        errors.append("manifest digest mismatch")
    manifest = json.loads(manifest_bytes.decode("utf-8"))
    if manifest["object_id"] != meta["object_id"]:
        errors.append("manifest object id mismatch")
    h = hashlib.sha256()
    total = 0
    damaged = set(meta.get("damaged_chunks") or [])
    for entry in manifest["chunks"]:
        if entry["seq"] in damaged:
            # Partial package: excluded bytes are declared, never silently skipped.
            if os.path.exists(os.path.join(root, "chunks", entry["digest"] + ".bin")):
                errors.append("damaged chunk seq %s unexpectedly present" % entry["seq"])
            continue
        path = os.path.join(root, "chunks", entry["digest"] + ".bin")
        if not os.path.exists(path):
            errors.append("missing chunk seq %s (%s)" % (entry["seq"], entry["digest"]))
            continue
        data = open(path, "rb").read()
        if len(data) != entry["length"]:
            errors.append("length mismatch seq %s" % entry["seq"])
        if sha256(data) != entry["digest"]:
            errors.append("digest mismatch seq %s" % entry["seq"])
        if entry["offset"] != total:
            errors.append("offset mismatch seq %s" % entry["seq"])
        total += len(data)
        h.update(data)
    if not damaged:
        # Full package: these two checks prove the bytes are the original packet.
        if total != manifest["stream"]["total_bytes"]:
            errors.append("total bytes mismatch")
        if h.hexdigest() != manifest["stream"]["sha256"]:
            errors.append("stream sha256 mismatch")
    else:
        print("PARTIAL package: %d chunk(s) intentionally excluded" % len(damaged))
    if merkle([c["digest"] for c in manifest["chunks"]]) != manifest["root_digest"]:
        errors.append("merkle root mismatch")
    for run in meta.get("parse_runs", []):
        payload_path = os.path.join(root, "results", run["id"] + ".json")
        if not os.path.exists(payload_path):
            errors.append("missing result for run %s" % run["id"])
            continue
        try:
            doc = json.load(open(payload_path, "r", encoding="utf-8"))
        except Exception as exc:
            errors.append("unreadable result %s: %s" % (run["id"], exc))
            continue
        if doc.get("object_manifest_digest") != meta["manifest_digest"]:
            errors.append("result %s pins a different object" % run["id"])
        if doc.get("rule_id") != run["rule_id"]:
            errors.append("result %s rule mismatch" % run["id"])
        # Recompute the reproducible digest the same way the vault does.
        canonical = {
            "format": doc.get("format", "sealed-wire-result/1"),
            "object_id": doc["object_id"],
            "object_manifest_digest": doc["object_manifest_digest"],
            "object_stream_sha256": doc["object_stream_sha256"],
            "rule_id": doc["rule_id"],
            "rule_source_sha256": doc["rule_source_sha256"],
            "records": doc["records"],
            "gaps": doc.get("gaps", []),
            "counts": doc.get("counts"),
        }
        blob = json.dumps(canonical, sort_keys=True, separators=(",", ":"),
                          ensure_ascii=False).encode("utf-8")
        if hashlib.sha256(blob).hexdigest() != run["result_digest"]:
            errors.append("result digest mismatch for run %s" % run["id"])
        if doc.get("result_digest") != run["result_digest"]:
            errors.append("result %s self-declared digest mismatch" % run["id"])
    if errors:
        print("FAILED")
        for e in errors:
            print(" -", e)
        return 1
    print("OK")
    print("object:          ", meta["object_id"])
    print("manifest sha256: ", meta["manifest_digest"])
    print("stream sha256:   ", manifest["stream"]["sha256"])
    print("merkle root:     ", manifest["root_digest"])
    print("bytes:           ", total)
    print("chunks:          ", len(manifest["chunks"]))
    for run in meta.get("parse_runs", []):
        print("parse run %s: rule %s, result sha256 %s" % (
            run["id"], run["rule_id"], run.get("result_digest")))
    return 0

if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else os.path.dirname(os.path.abspath(__file__))))
'''

VERIFY_MD = """# Verifying this package offline

This package is self-contained and needs only Python 3.8+ (standard library).

```
python3 verify_package.py .
```

Checks performed:

1. `manifest.json` hashes to `object.json.manifest_digest`.
2. Each `chunks/<digest>.bin` file exists with the exact recorded length and
   SHA-256.
3. Chunks concatenated in manifest order reproduce `stream.sha256`.
4. The Merkle root over the chunk digests matches the manifest.

`object.json` also pins the parse runs included here: rule id, rule source
digest, result digest, and the per-record byte spans (`results/<run>.json`).
No online source is consulted, and re-running the parse from this package must
produce the same result digest.
"""


def export_package(
    cfg: Config,
    conn,
    object_row,
    *,
    run_ids: list[str] | None = None,
    partial: bool = False,
) -> tuple[bytes, dict]:
    """Build a reproducible zip containing the exact bytes and their proof.

    Refuses to export a package containing damaged chunks unless `partial=True`,
    in which case damaged chunks are excluded and listed in `damage.json`.
    """
    om = load_manifest(cfg, object_row["manifest_digest"])
    report = check_object(cfg, conn, object_row, update_flags=False)
    damaged_seqs = {d["seq"] for d in report["damaged"]}
    if damaged_seqs and not partial:
        raise ObjectDamageError(
            "object has damaged chunks; refusing to export an unverifiable package",
            report,
        )

    runs = []
    if run_ids is None:
        run_ids = [
            r["id"]
            for r in conn.execute(
                "SELECT id FROM parse_runs WHERE object_id=? AND status='done' ORDER BY created_at",
                (object_row["id"],),
            ).fetchall()
        ]
    for rid in run_ids:
        run = conn.execute("SELECT * FROM parse_runs WHERE id=? AND object_id=?",
                           (rid, object_row["id"])).fetchone()
        if not run or run["status"] != "done" or damaged_seqs:
            continue
        rule = conn.execute("SELECT * FROM rules WHERE id=?", (run["rule_id"],)).fetchone()
        result_path = Path(run["result_path"]) if run["result_path"] else None
        result_payload = None
        result_digest = run["result_digest"]
        if result_path and result_path.exists():
            result_payload = result_path.read_bytes()
            # Verify the stored digest against reproducible content, not the
            # file bytes (which embed the run id and a timestamp).
            try:
                doc = json.loads(result_payload.decode("utf-8"))
                from .parse_engine import canonical_result_digest

                if canonical_result_digest(doc) != run["result_digest"]:
                    result_payload = None
            except (UnicodeDecodeError, json.JSONDecodeError, KeyError):
                result_payload = None
        runs.append(
            {
                "id": run["id"],
                "rule_id": run["rule_id"],
                "rule_source_sha256": rule["source_sha256"] if rule else None,
                "started_at": run["started_at"],
                "finished_at": run["finished_at"],
                "result_digest": run["result_digest"],
                "result_count": run["result_count"],
                "object_manifest_digest": object_row["manifest_digest"],
                "payload": result_payload,
            }
        )

    object_meta = {
        "format": "sealed-wire-package/1",
        "object_id": object_row["id"],
        "source_id": object_row["source_id"],
        "capture_id": object_row["capture_id"],
        "manifest_digest": object_row["manifest_digest"],
        "root_digest": object_row["root_digest"],
        "stream_sha256": om.stream_digest,
        "total_bytes": object_row["total_bytes"],
        "created_at": object_row["created_at"],
        "exported_at": utcnow(),
        "tool_version": cfg.tool_version,
        "damaged_chunks": sorted(damaged_seqs),
        "parse_runs": [
            {k: v for k, v in r.items() if k != "payload"} for r in runs
        ],
    }

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=6) as zf:
        def put(name: str, data: bytes):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o444 << 16
            zf.writestr(info, data)

        put("object.json", canonical_json_bytes(object_meta))
        put("manifest.json", om.raw)
        entries_by_digest = {}
        for entry in om.chunks:
            entries_by_digest.setdefault(entry["digest"], entry)
        for digest, entry in entries_by_digest.items():
            if entry["seq"] in damaged_seqs:
                continue
            data = read_chunk(cfg, digest)
            if data is None:
                continue
            put(f"chunks/{digest}.bin", data)
        if damaged_seqs:
            put(
                "damage.json",
                canonical_json_bytes(
                    {
                        "status": "partial",
                        "damaged_chunks": report["damaged"],
                        "note": "This package intentionally excludes damaged chunks; "
                        "it verifies the remaining bytes only.",
                    }
                ),
            )
        for run in runs:
            if run["payload"] is not None:
                put(f"results/{run['id']}.json", run["payload"])
        put("verify_package.py", VERIFY_SCRIPT.encode("utf-8"))
        put("VERIFY.md", VERIFY_MD.encode("utf-8"))
    return buf.getvalue(), {"object_meta": object_meta, "runs": runs, "report": report}
