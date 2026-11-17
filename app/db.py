"""SQLite schema and connection helpers.

The database is the *catalog*: it maps sources -> capture attempts -> sealed
objects -> manifests, plus parse runs and results. Bytes never live in the DB;
chunks live on disk and are addressed by SHA-256 so they can be shared.
"""
from __future__ import annotations

import sqlite3
from contextlib import contextmanager
from pathlib import Path

SCHEMA_VERSION = 1

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;
PRAGMA busy_timeout=10000;

CREATE TABLE IF NOT EXISTS meta (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL
);

-- A logical source. Withdrawal is recorded here and never removes bytes.
CREATE TABLE IF NOT EXISTS sources (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    origin TEXT NOT NULL,
    url TEXT NOT NULL,
    headers_json TEXT NOT NULL DEFAULT '{}',
    mode TEXT NOT NULL DEFAULT 'online',       -- 'online' | 'static'
    static_path TEXT,
    status TEXT NOT NULL DEFAULT 'active',     -- 'active' | 'withdrawn'
    withdrawn_at TEXT,
    withdrawn_reason TEXT,
    created_at TEXT NOT NULL,
    notes TEXT NOT NULL DEFAULT ''
);
CREATE UNIQUE INDEX IF NOT EXISTS sources_name_uq ON sources(name);

-- One fetch attempt of a source. A withdrawn source can never gain a new
-- successful attempt, but historical attempts remain readable.
CREATE TABLE IF NOT EXISTS captures (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id),
    status TEXT NOT NULL,                      -- 'pending'|'running'|'sealed'|'failed'|'aborted'
    started_at TEXT NOT NULL,
    finished_at TEXT,
    sealed_object_id TEXT,
    http_status INTEGER,
    content_type TEXT,
    bytes_captured INTEGER NOT NULL DEFAULT 0,
    fetched_url TEXT,
    headers_json TEXT NOT NULL DEFAULT '{}',
    error TEXT,
    job_id TEXT,
    rule_hint TEXT,
    worker TEXT,
    staged_path TEXT,
    staged_meta_json TEXT NOT NULL DEFAULT '{}',
    staged_at TEXT
);
CREATE INDEX IF NOT EXISTS captures_source_idx ON captures(source_id, started_at DESC);

-- An immutable sealed object: the exact byte stream captured in one attempt.
CREATE TABLE IF NOT EXISTS objects (
    id TEXT PRIMARY KEY,
    capture_id TEXT NOT NULL REFERENCES captures(id),
    source_id TEXT NOT NULL REFERENCES sources(id),
    created_at TEXT NOT NULL,
    total_bytes INTEGER NOT NULL,
    chunk_count INTEGER NOT NULL,
    root_digest TEXT NOT NULL,          -- Merkle root over chunk digests
    stream_digest TEXT NOT NULL,        -- SHA-256 of the whole captured stream
    manifest_digest TEXT NOT NULL,
    manifest_path TEXT NOT NULL,
    content_type TEXT,
    http_status INTEGER,
    fetched_url TEXT,
    headers_json TEXT NOT NULL DEFAULT '{}',
    tool_version TEXT NOT NULL,
    algorithm TEXT NOT NULL,
    compression TEXT NOT NULL DEFAULT 'none'
);
CREATE INDEX IF NOT EXISTS objects_source_idx ON objects(source_id, created_at DESC);

-- A chunk is a content-addressed blob. refcounts are derived, but cached.
CREATE TABLE IF NOT EXISTS chunks (
    digest TEXT PRIMARY KEY,
    size INTEGER NOT NULL,
    first_seen_at TEXT NOT NULL,
    refcount INTEGER NOT NULL DEFAULT 0,
    missing INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS object_chunks (
    object_id TEXT NOT NULL REFERENCES objects(id),
    seq INTEGER NOT NULL,
    digest TEXT NOT NULL REFERENCES chunks(digest),
    offset INTEGER NOT NULL,
    length INTEGER NOT NULL,
    PRIMARY KEY (object_id, seq)
);
CREATE INDEX IF NOT EXISTS object_chunks_digest_idx ON object_chunks(digest);

-- Capture pipeline queues. `fetch_jobs` is the unit of work: one row per
-- capture attempt, carrying whether a single process does both stages
-- (`inline`) or a separate fetch worker and seal worker do them.
-- `seal_jobs` is created by the fetch stage and consumed by the seal stage.
CREATE TABLE IF NOT EXISTS fetch_jobs (
    id TEXT PRIMARY KEY,
    source_id TEXT NOT NULL REFERENCES sources(id),
    capture_id TEXT NOT NULL REFERENCES captures(id),
    status TEXT NOT NULL DEFAULT 'queued',    -- queued|running|fetched|failed|canceled|sealed
    inline INTEGER NOT NULL DEFAULT 1,        -- 1 = one process fetches and seals
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    staged_path TEXT,
    meta_json TEXT NOT NULL DEFAULT '{}',
    stream_sha256 TEXT,
    bytes INTEGER NOT NULL DEFAULT 0,
    http_status INTEGER,
    error TEXT,
    cancel_requested INTEGER NOT NULL DEFAULT 0,
    lease_until TEXT,
    worker TEXT
);
CREATE INDEX IF NOT EXISTS fetch_jobs_status_idx ON fetch_jobs(status, created_at);

CREATE TABLE IF NOT EXISTS seal_jobs (
    id TEXT PRIMARY KEY,
    fetch_job_id TEXT NOT NULL REFERENCES fetch_jobs(id),
    capture_id TEXT NOT NULL REFERENCES captures(id),
    status TEXT NOT NULL DEFAULT 'queued',    -- queued|running|done|failed
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    object_id TEXT,
    error TEXT,
    lease_until TEXT,
    worker TEXT
);
CREATE INDEX IF NOT EXISTS seal_jobs_status_idx ON seal_jobs(status, created_at);

-- Parse rules are immutable versions loaded from rules/*.py.
CREATE TABLE IF NOT EXISTS rules (
    id TEXT PRIMARY KEY,            -- e.g. 'html_records@2'
    name TEXT NOT NULL,
    version INTEGER NOT NULL,
    path TEXT NOT NULL,
    source_sha256 TEXT NOT NULL,
    loaded_at TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    is_active INTEGER NOT NULL DEFAULT 0
);
CREATE UNIQUE INDEX IF NOT EXISTS rules_name_version_uq ON rules(name, version);
CREATE INDEX IF NOT EXISTS rules_name_idx ON rules(name, version DESC);

CREATE TABLE IF NOT EXISTS parse_runs (
    id TEXT PRIMARY KEY,
    object_id TEXT NOT NULL REFERENCES objects(id),
    rule_id TEXT NOT NULL REFERENCES rules(id),
    status TEXT NOT NULL,          -- queued|running|paused|done|failed|canceled
    created_at TEXT NOT NULL,
    started_at TEXT,
    finished_at TEXT,
    cursor INTEGER NOT NULL DEFAULT 0,
    total_chunks INTEGER NOT NULL,
    processed_chunks INTEGER NOT NULL DEFAULT 0,
    resume_state TEXT NOT NULL DEFAULT '{}',
    error TEXT,
    result_digest TEXT,
    result_path TEXT,
    result_count INTEGER,
    job_id TEXT,
    lease_until TEXT,
    worker TEXT,
    stopped_by TEXT,
    stop_requested INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS parse_runs_object_idx ON parse_runs(object_id, created_at DESC);

-- dep_digests is a JSON array of every chunk digest the record's byte span
-- overlaps. It is computed from the manifest, not taken on trust from a rule,
-- and is what lets a damaged chunk block exactly the records that depend on it.
CREATE TABLE IF NOT EXISTS parse_records (
    run_id TEXT NOT NULL REFERENCES parse_runs(id),
    ordinal INTEGER NOT NULL,
    chunk_digest TEXT NOT NULL,
    chunk_seq INTEGER NOT NULL,
    value_json TEXT NOT NULL,
    span_start INTEGER,
    span_end INTEGER,
    dep_digests TEXT NOT NULL DEFAULT '[]',
    PRIMARY KEY (run_id, ordinal)
);
CREATE INDEX IF NOT EXISTS parse_records_chunk_idx ON parse_records(run_id, chunk_digest);

CREATE TABLE IF NOT EXISTS parse_gaps (
    run_id TEXT NOT NULL REFERENCES parse_runs(id),
    start INTEGER NOT NULL,
    end INTEGER NOT NULL,
    reason TEXT NOT NULL,
    PRIMARY KEY (run_id, start)
);

CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    at TEXT NOT NULL,
    kind TEXT NOT NULL,
    entity TEXT,
    entity_id TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}',
    actor TEXT NOT NULL DEFAULT 'web'
);
CREATE INDEX IF NOT EXISTS events_at_idx ON events(at DESC);
"""


def connect(db_path: Path, *, readonly: bool = False) -> sqlite3.Connection:
    db_path = Path(db_path)
    if readonly:
        uri = f"file:{db_path}?mode=ro"
        conn = sqlite3.connect(uri, uri=True, timeout=15.0, isolation_level=None)
    else:
        conn = sqlite3.connect(str(db_path), timeout=15.0, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    conn.execute("PRAGMA busy_timeout=10000")
    return conn


def init_db(db_path: Path) -> None:
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = connect(db_path)
    try:
        conn.executescript(SCHEMA)
        _migrate(conn)
        with transaction(conn):
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('schema_version', ?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (str(SCHEMA_VERSION),),
            )
    finally:
        conn.close()


# Columns added after the first schema version. `CREATE TABLE IF NOT EXISTS`
# does not touch an existing table, so additive changes are applied here.
_ADDITIVE_COLUMNS = {
    "objects": [("stream_digest", "TEXT NOT NULL DEFAULT ''")],
    "parse_runs": [("resume_state", "TEXT NOT NULL DEFAULT '{}'"),
                   ("stop_requested", "INTEGER NOT NULL DEFAULT 0")],
    "parse_records": [("dep_digests", "TEXT NOT NULL DEFAULT '[]'")],
    "captures": [("staged_path", "TEXT"),
                 ("staged_meta_json", "TEXT NOT NULL DEFAULT '{}'"),
                 ("staged_at", "TEXT")],
    "fetch_jobs": [("inline", "INTEGER NOT NULL DEFAULT 1")],
}


def _migrate(conn: sqlite3.Connection) -> None:
    # fetch_jobs once declared a foreign key to the legacy `jobs` table; rows are
    # now identified by their own id, so rebuild the table if the old definition
    # is present. (SQLite cannot drop a constraint in place.)
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='fetch_jobs'"
    ).fetchone()
    if row and "REFERENCES jobs" in (row["sql"] or ""):
        conn.executescript(
            """
            PRAGMA foreign_keys=OFF;
            ALTER TABLE fetch_jobs RENAME TO fetch_jobs_old;
            CREATE TABLE fetch_jobs (
                id TEXT PRIMARY KEY,
                source_id TEXT NOT NULL REFERENCES sources(id),
                capture_id TEXT NOT NULL REFERENCES captures(id),
                status TEXT NOT NULL DEFAULT 'queued',
                inline INTEGER NOT NULL DEFAULT 1,
                created_at TEXT NOT NULL,
                started_at TEXT,
                finished_at TEXT,
                staged_path TEXT,
                meta_json TEXT NOT NULL DEFAULT '{}',
                stream_sha256 TEXT,
                bytes INTEGER NOT NULL DEFAULT 0,
                http_status INTEGER,
                error TEXT,
                cancel_requested INTEGER NOT NULL DEFAULT 0,
                lease_until TEXT,
                worker TEXT
            );
            INSERT INTO fetch_jobs
                SELECT id, source_id, capture_id, status, 1, created_at,
                       started_at, finished_at, staged_path, meta_json,
                       stream_sha256, bytes, http_status, error, cancel_requested,
                       lease_until, worker
                FROM fetch_jobs_old;
            DROP TABLE fetch_jobs_old;
            CREATE INDEX IF NOT EXISTS fetch_jobs_status_idx
                ON fetch_jobs(status, created_at);
            PRAGMA foreign_keys=ON;
            """
        )
    for table, columns in _ADDITIVE_COLUMNS.items():
        existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})")}
        for name, decl in columns:
            if name not in existing:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {decl}")
    # Backfill the stream digest for objects sealed before it was indexed.
    for row in conn.execute(
        "SELECT id, manifest_path FROM objects WHERE stream_digest='' OR stream_digest IS NULL"
    ).fetchall():
        try:
            import json as _json

            with open(row["manifest_path"], "rb") as fh:
                manifest = _json.loads(fh.read().decode("utf-8"))
            digest = manifest["stream"]["sha256"]
        except (OSError, ValueError, KeyError):
            continue
        conn.execute("UPDATE objects SET stream_digest=? WHERE id=?", (digest, row["id"]))
    conn.commit()


@contextmanager
def transaction(conn: sqlite3.Connection, immediate: bool = True):
    conn.execute("BEGIN IMMEDIATE" if immediate else "BEGIN")
    try:
        yield conn
    except Exception:
        conn.execute("ROLLBACK")
        raise
    else:
        conn.execute("COMMIT")


def row_to_dict(row) -> dict | None:
    if row is None:
        return None
    return {k: row[k] for k in row.keys()}


def log_event(conn, kind: str, entity: str | None = None, entity_id: str | None = None,
              detail: dict | None = None, actor: str = "web", at: str | None = None) -> None:
    from .util import canonical_json_text, utcnow

    conn.execute(
        "INSERT INTO events(at, kind, entity, entity_id, detail_json, actor) "
        "VALUES(?,?,?,?,?,?)",
        (at or utcnow(), kind, entity, entity_id, canonical_json_text(detail or {}), actor),
    )
