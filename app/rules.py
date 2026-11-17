"""Parse rules: discovery, sandboxed loading, and span mapping.

A rule is a Python file in `rules/` exposing:

    NAME = "html_records"          # rule family
    VERSION = 2                    # bump on any behaviour change
    DESCRIPTION = "..."
    def parse(text: str, ctx: RuleContext) -> Iterable[dict]

Each yielded dict is one record. Optionally a record may carry
`_span = (start, end)` character offsets into `text`; if it does not, the worker
attributes it to the chunk(s) it was found in. Spans are how every published
result points at an exact field position in the exact original packet.

Rules are trusted code shipped with the project (not user uploads). They are
loaded with `compile()` + `exec` in a fresh module namespace so the exact source
digest can be recorded and re-loaded reproducibly by id.
"""
from __future__ import annotations

import types
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable

from .util import sha256_hex, utcnow


class RuleError(RuntimeError):
    pass


@dataclass
class RuleContext:
    """Information a rule may use; must never depend on wall clock or network."""
    object_id: str
    stream_sha256: str
    text_length: int
    chunk_offset: int = 0
    chunk_length: int = 0
    chunk_digest: str = ""
    extras: dict = field(default_factory=dict)


@dataclass
class LoadedRule:
    id: str
    name: str
    version: int
    description: str
    path: Path
    source_sha256: str
    module: types.ModuleType

    def parse(self, text: str, ctx: RuleContext):
        fn = getattr(self.module, "parse", None)
        if fn is None:
            raise RuleError(f"rule {self.id} has no parse() function")
        result = fn(text, ctx)
        if result is None:
            return []
        return result


def _discover(path: Path) -> list[Path]:
    return sorted(p for p in Path(path).glob("*.py") if not p.name.startswith("_"))


def load_rule_file(path: Path) -> LoadedRule:
    path = Path(path)
    source = path.read_bytes()
    digest = sha256_hex(source)
    mod_name = f"sealed_wire_rule_{path.stem}_{digest[:8]}"
    module = types.ModuleType(mod_name)
    module.__file__ = str(path)
    code = compile(source, str(path), "exec")
    try:
        exec(code, module.__dict__)
    except Exception as exc:  # pragma: no cover - surfaced to UI
        raise RuleError(f"failed to load {path.name}: {exc}") from exc
    name = getattr(module, "NAME", path.stem)
    try:
        version = int(getattr(module, "VERSION"))
    except Exception as exc:
        raise RuleError(f"{path.name} must define integer VERSION") from exc
    if not callable(getattr(module, "parse", None)):
        raise RuleError(f"{path.name} must define parse(text, ctx)")
    return LoadedRule(
        id=f"{name}@{version}",
        name=name,
        version=version,
        description=str(getattr(module, "DESCRIPTION", "")),
        path=path,
        source_sha256=digest,
        module=module,
    )


def scan_rules(rules_dir: Path) -> tuple[list[LoadedRule], list[dict]]:
    rules, errors = [], []
    for path in _discover(rules_dir):
        try:
            rules.append(load_rule_file(path))
        except RuleError as exc:
            errors.append({"path": str(path), "error": str(exc)})
    return rules, errors


def sync_rules(conn, rules_dir: Path) -> dict:
    """Register every on-disk rule version; never overwrite a registered one."""
    rules, errors = scan_rules(rules_dir)
    added = []
    for rule in rules:
        exists = conn.execute("SELECT id FROM rules WHERE id=?", (rule.id,)).fetchone()
        if exists:
            continue
        conn.execute(
            "INSERT INTO rules(id, name, version, path, source_sha256, loaded_at, description) "
            "VALUES(?,?,?,?,?,?,?)",
            (
                rule.id,
                rule.name,
                rule.version,
                str(rule.path),
                rule.source_sha256,
                utcnow(),
                rule.description,
            ),
        )
        added.append(rule.id)
    if added and not conn.execute(
        "SELECT 1 FROM rules WHERE is_active=1 LIMIT 1"
    ).fetchone():
        # Pick the newest version of each family as a convenience default.
        for name in {r.name for r in rules}:
            row = conn.execute(
                "SELECT id FROM rules WHERE name=? ORDER BY version DESC LIMIT 1", (name,)
            ).fetchone()
            if row:
                conn.execute("UPDATE rules SET is_active=1 WHERE id=?", (row["id"],))
    conn.commit()
    return {"registered": added, "rules": [r.id for r in rules], "errors": errors}


def load_rule_by_id(conn, rule_id: str) -> LoadedRule:
    row = conn.execute("SELECT * FROM rules WHERE id=?", (rule_id,)).fetchone()
    if not row:
        raise RuleError(f"unknown rule {rule_id}")
    path = Path(row["path"])
    rule = load_rule_file(path)
    if rule.source_sha256 != row["source_sha256"]:
        raise RuleError(
            f"rule source for {rule_id} changed on disk "
            f"(expected {row['source_sha256'][:12]}, got {rule.source_sha256[:12]}); "
            "register it as a new version instead of editing in place"
        )
    return rule


# --------------------------------------------------------------------------
# dependency mapping
# --------------------------------------------------------------------------

def chunk_overlaps(chunks: list[dict], start: int, end: int) -> list[dict]:
    """Chunks whose byte range intersects [start, end)."""
    hits = []
    for c in chunks:
        c_start = c["offset"]
        c_end = c_start + c["length"]
        if c_start < end and c_end > start:
            hits.append(c)
    return hits


def digest_map(chunks: list[dict]) -> dict[int, str]:
    return {c["offset"]: c["digest"] for c in chunks}


def locate_offset(chunks: list[dict], offset: int) -> dict | None:
    for c in chunks:
        if c["offset"] <= offset < c["offset"] + max(1, c["length"]):
            return c
    return None
