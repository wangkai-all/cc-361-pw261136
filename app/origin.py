"""Fictional origin server (live bytes for the demo feeds).

Run separately from the capture worker and the web UI:

    python3 -m app.origin --port 8811

It serves the current fixture revision for each demo feed. `POST /admin/bump`
advances a feed (simulating the online source changing) and `POST /admin/withdraw`
makes it return 410 Gone (simulating a retraction). Both are demo-only so the
system's guarantees can be observed end to end.
"""
from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .config import load_config
from .db import connect, transaction
from .fixtures import (
    DEMO_SOURCE_SPECS,
    bump_fixture,
    fixture_revision_path,
    read_fixture,
)
from .util import utcnow

ROUTES = {spec["url"]: spec for spec in DEMO_SOURCE_SPECS}


class OriginHandler(BaseHTTPRequestHandler):
    server_version = "DemoOrigin/1.0"
    cfg = None

    def log_message(self, fmt, *args):
        # Keep the demo console readable but record requests to the log dir.
        line = "%s - %s\n" % (utcnow(), fmt % args)
        try:
            self.cfg.log_dir.mkdir(parents=True, exist_ok=True)
            with open(self.cfg.log_dir / "origin.log", "a", encoding="utf-8") as fh:
                fh.write(line)
        except OSError:
            pass

    def _send(self, status: int, body: bytes, ctype="application/octet-stream", extra=None):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Origin-Generated", utcnow())
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, status: int, obj):
        self._send(status, json.dumps(obj).encode(), "application/json")

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/healthz":
            return self._json(200, {"ok": True, "feeds": len(ROUTES)})
        spec = ROUTES.get(parsed.path)
        if not spec:
            return self._send(404, b"not found\n", "text/plain")
        conn = connect(self.cfg.db_path)
        try:
            row = conn.execute("SELECT status FROM sources WHERE name=?", (spec["name"],)).fetchone()
            if row and row["status"] == "withdrawn":
                return self._send(410, b"source withdrawn\n", "text/plain")
        finally:
            conn.close()
        data = read_fixture(self.cfg, spec["name"])
        rev = "0"
        rev_path = fixture_revision_path(self.cfg, spec["name"])
        if rev_path.exists():
            rev = rev_path.read_text().strip()
        return self._send(
            200,
            data,
            spec["content_type"],
            {"X-Feed-Revision": rev, "Cache-Control": "no-store"},
        )

    def do_HEAD(self):
        self.do_GET()

    def do_POST(self):
        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        name = (qs.get("source") or [""])[0]
        if parsed.path not in ("/admin/bump", "/admin/withdraw", "/admin/reactivate"):
            return self._json(404, {"error": "not found"})
        if not any(s["name"] == name for s in DEMO_SOURCE_SPECS):
            return self._json(400, {"error": "unknown demo source"})
        if parsed.path == "/admin/bump":
            rev = bump_fixture(self.cfg, name)
            return self._json(200, {"source": name, "revision": rev, "note":
                                    "origin content changed; previous sealed objects are untouched"})
        conn = connect(self.cfg.db_path)
        try:
            with transaction(conn):
                if parsed.path == "/admin/withdraw":
                    conn.execute(
                        "UPDATE sources SET status='withdrawn', withdrawn_at=?, "
                        "withdrawn_reason=? WHERE name=?",
                        (utcnow(), "retracted by origin (demo)", name),
                    )
                else:
                    conn.execute(
                        "UPDATE sources SET status='active', withdrawn_at=NULL, "
                        "withdrawn_reason=NULL WHERE name=?", (name,)
                    )
        finally:
            conn.close()
        return self._json(200, {"source": name, "withdrawn": parsed.path == "/admin/withdraw"})


def serve(host: str = "127.0.0.1", port: int = 8811):
    cfg = load_config()
    OriginHandler.cfg = cfg
    httpd = ThreadingHTTPServer((host, port), OriginHandler)
    print(f"demo origin on http://{host}:{port} (feeds: {', '.join(ROUTES)})", flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.server_close()


def main(argv=None):
    parser = argparse.ArgumentParser(description="fictional demo origin")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8811)
    args = parser.parse_args(argv)
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
