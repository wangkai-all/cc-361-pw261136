#!/usr/bin/env python3
"""Fetch worker: the only process that talks to a source.

It claims `fetch_jobs` marked for the split pipeline, streams the response into
a staging file, records the exact length + SHA-256, and queues a seal job. It
writes no chunks and no manifests, and it never reads the vault.

    PYTHONPATH=. python3 -m app.fetch_worker [--once] [--idle-exit]
"""
from __future__ import annotations

import argparse
import json

from .capture_worker import run_fetch_worker
from .config import load_config


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="fetch a source into staging")
    ap.add_argument("--once", action="store_true", help="process at most one job")
    ap.add_argument("--idle-exit", action="store_true", help="exit when the queue is empty")
    ap.add_argument("--poll", type=float, default=1.0)
    args = ap.parse_args(argv)
    result = run_fetch_worker(load_config(), once=args.once,
                              idle_exit=args.idle_exit, poll=args.poll)
    if args.once:
        print(json.dumps(result, indent=2) if result else "no queued fetch job")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
