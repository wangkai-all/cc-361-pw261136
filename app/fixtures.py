"""Fixture-backed sources.

The demo gives each fictional source a deterministic body plus a way to mutate
it (to prove that re-capture gets a new object while old objects stay exact).
`StaticFetcher` serves bytes from disk so offline demos/tests can still exercise
the exact same sealing path as HTTP.
"""
from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from pathlib import Path

from .config import Config
from .db import log_event, transaction
from .util import atomic_write_json, new_id, sha256_hex, utcnow

DEMO_SOURCE_SPECS = [
    {
        "name": "ticker-news",
        "url": "/feeds/ticker-news.html",
        "content_type": "text/html; charset=utf-8",
        "title": "Wire Ticker",
        "kind": "html_records",
    },
    {
        "name": "port-authority",
        "url": "/feeds/port-authority.html",
        "content_type": "text/html; charset=utf-8",
        "title": "Port Authority Notices",
        "kind": "html_records",
    },
    {
        "name": "market-csv",
        "url": "/feeds/market.csv",
        "content_type": "text/csv; charset=utf-8",
        "title": "Market Snapshot",
        "kind": "csv_rows",
    },
]


def _row_html(source_name: str, seq: int, revision: int) -> str:
    topics = ["grain", "copper", "shipping", "rainfall", "power", "insurance"]
    regions = ["north-basin", "east-quay", "delta", "highland", "coast"]
    topic = topics[(seq + revision) % len(topics)]
    region = regions[(seq * 3 + revision) % len(regions)]
    value = 100 + ((seq * 37 + revision * 11) % 500)
    note = (
        f"{topic} desk in {region} reports {value} units; "
        f"{'revised ' + str(revision) if revision else 'initial'} entry."
    )
    return (
        f'<article class="record" data-seq="{seq}" data-rev="{revision}">\n'
        f"  <h2 id=\"r{seq}\">{source_name} / {topic} / {region}</h2>\n"
        f'  <p class="note">{note}</p>\n'
        f'  <span class="value">{value}</span>\n'
        f"</article>\n"
    )


def render_html_source(source_name: str, revision: int = 0, count: int = 6) -> bytes:
    head = (
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\n"
        f"<title>{source_name} (rev {revision})</title></head>\n<body>\n"
        f"<header><h1>{source_name}</h1>"
        f"<p class=\"stamp\">revision {revision}</p></header>\n<main>\n"
    )
    rows = "".join(_row_html(source_name, i, revision) for i in range(1, count + 1))
    tail = "</main>\n<footer>fictional demo feed</footer>\n</body>\n</html>\n"
    return (head + rows + tail).encode("utf-8")


def render_csv_source(revision: int = 0, count: int = 8) -> bytes:
    lines = ["symbol,last,change,batch"]
    for i in range(1, count + 1):
        last = 50 + ((i * 13 + revision * 7) % 90)
        change = ((i * 5 + revision) % 11) - 5
        lines.append(f"SYM{i:02d},{last}.{i%10:01d},{change:+d},rev{revision}")
    return ("\n".join(lines) + "\n").encode("utf-8")


def fixture_path(cfg: Config, source_name: str) -> Path:
    return cfg.fixtures_dir / f"{source_name}.bin"


def fixture_revision_path(cfg: Config, source_name: str) -> Path:
    return cfg.fixtures_dir / f"{source_name}.rev"


def ensure_fixture(cfg: Config, source_name: str, *, revision: int = 0) -> Path:
    """Create the on-disk fixture for a demo source (idempotent per revision)."""
    cfg.fixtures_dir.mkdir(parents=True, exist_ok=True)
    path = fixture_path(cfg, source_name)
    rev_path = fixture_revision_path(cfg, source_name)
    current = 0
    if rev_path.exists():
        try:
            current = int(rev_path.read_text().strip())
        except ValueError:
            current = 0
    if path.exists() and current == revision:
        return path
    spec = next((s for s in DEMO_SOURCE_SPECS if s["name"] == source_name), None)
    if source_name == "market-csv":
        data = render_csv_source(revision)
    elif spec:
        data = render_html_source(source_name, revision)
    else:
        data = (f"fictional source {source_name} revision {revision}\n").encode("utf-8")
    atomic_write_json(
        path.with_suffix(".meta.json"),
        {
            "name": source_name,
            "revision": revision,
            "sha256": sha256_hex(data),
            "generated_at": utcnow(),
            "fictional": True,
        },
        fsync=False,
    )
    path.write_bytes(data)
    rev_path.write_text(str(revision))
    return path


def read_fixture(cfg: Config, source_name: str) -> bytes:
    path = fixture_path(cfg, source_name)
    return path.read_bytes()


def bump_fixture(cfg: Config, source_name: str) -> int:
    """Simulate the origin changing: new revision with real new bytes."""
    rev_path = fixture_revision_path(cfg, source_name)
    current = 0
    if rev_path.exists():
        try:
            current = int(rev_path.read_text().strip())
        except ValueError:
            current = 0
    new_rev = current + 1
    ensure_fixture(cfg, source_name, revision=new_rev)
    return new_rev


def seed_demo_sources(cfg: Config, conn: sqlite3.Connection, *, origin_base: str) -> list[str]:
    """Create the fictional sources and their fixture files. Idempotent."""
    created = []
    for spec in DEMO_SOURCE_SPECS:
        ensure_fixture(cfg, spec["name"], revision=0)
        row = conn.execute("SELECT id FROM sources WHERE name=?", (spec["name"],)).fetchone()
        if row:
            continue
        sid = new_id("src")
        with transaction(conn):
            conn.execute(
                """INSERT INTO sources(id, name, origin, url, headers_json, mode,
                        static_path, status, created_at, notes)
                   VALUES(?,?,?,?,?,?,?,?,?,?)""",
                (
                    sid,
                    spec["name"],
                    f"demo://{spec['name']}",
                    origin_base + spec["url"],
                    json.dumps({}),
                    "static",
                    str(fixture_path(cfg, spec["name"])),
                    "active",
                    utcnow(),
                    f"Fictional {spec['title']} feed. The bytes live in fixtures/ and are "
                    "served by the demo origin when online mode is used.",
                ),
            )
            log_event(conn, "source.created", "source", sid,
                      {"name": spec["name"], "url": origin_base + spec["url"]}, actor="seed")
        created.append(sid)
    return created
