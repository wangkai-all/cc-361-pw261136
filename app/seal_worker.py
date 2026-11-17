#!/usr/bin/env python3
"""Seal worker: the only process that writes the chunk store and manifests.

It claims `seal_jobs` produced by the split fetch worker, re-hashes the staging
file against the digest the fetcher recorded, and only then chunks, stores and
commits the object. It never opens a network connection.

    PYTHONPATH=. python3 -m app.seal_worker [--once] [--idle-exit]
"""
from __future__ import annotations

import argparse
import json

from .capture_worker import run_seal_worker
from .config import load_config


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description="seal staged bytes into the vault")
    ap.add_argument("--once", action="store_true", help="process at most one job")
    ap.add_argument("--idle-exit", action="store_true", help="exit when the queue is empty")
    ap.add_argument("--poll", type=float, default=1.0)
    args = ap.parse_args(argv)
    result = run_seal_worker(load_config(), once=args.once,
                             idle_exit=args.idle_exit, poll=args.poll)
    if args.once:
        print(json.dumps(result, indent=2) if result else "no queued seal job")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
