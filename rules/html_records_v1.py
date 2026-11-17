"""HTML record extractor, revision 1.

Deterministic and offline: only `text` and `ctx` are used. Every record carries
`_span = (start, end)` as character offsets into the region text it was parsed
from, so the engine can attach the exact byte range in the sealed packet.

Revision 1 reports the heading verbatim as `title` and the value as a raw
string. Use `html_records_v2.py` to split the heading into fields and coerce
types — the UI can compare the two results over the same sealed object.
"""
import re

NAME = "html_records"
VERSION = 1
DESCRIPTION = "Extract <article class=record> blocks; heading kept verbatim, raw string values."

_ARTICLE = re.compile(
    r"<article\b[^>]*\bdata-seq=\"(?P<seq>\d+)\"[^>]*\bdata-rev=\"(?P<rev>\d+)\"[^>]*>"
    r"(?P<body>.*?)</article>",
    re.DOTALL,
)
_H2 = re.compile(r"<h2[^>]*>(?P<title>.*?)</h2>", re.DOTALL)
_NOTE = re.compile(r"<p class=\"note\">(?P<note>.*?)</p>", re.DOTALL)
_VALUE = re.compile(r"<span class=\"value\">(?P<value>.*?)</span>", re.DOTALL)


def parse(text, ctx):
    for m in _ARTICLE.finditer(text):
        body = m.group("body")
        h2 = _H2.search(body)
        note = _NOTE.search(body)
        value = _VALUE.search(body)
        if not (h2 and value):
            continue
        record = {
            "id": f"seq:{m.group('seq')}",
            "seq": int(m.group("seq")),
            "revision": int(m.group("rev")),
            "title": _strip_tags(h2.group("title")).strip(),
            "value": _strip_tags(value.group("value")).strip(),
            "_span": (m.start(), m.end()),
        }
        if note:
            record["note"] = _strip_tags(note.group("note")).strip()
        yield record


def _strip_tags(raw: str) -> str:
    return re.sub(r"<[^>]+>", "", raw)
