"""HTML record extractor, revision 2.

Behaviour changes vs v1, all visible in a rule comparison over the same sealed
bytes:

* `title` is split into `topic` and `region` fields;
* `value` is emitted as an integer instead of a string;
* the revision number is exposed as `revision` for audit.

Spans still delimit the whole <article> block, so provenance is comparable
across revisions.
"""
import re

NAME = "html_records"
VERSION = 2
DESCRIPTION = "Split heading into topic/region, coerce value to int."

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
        title = re.sub(r"<[^>]+>", "", h2.group("title")).strip()
        parts = [p.strip() for p in title.split("/")]
        record = {
            "id": f"seq:{m.group('seq')}",
            "seq": int(m.group("seq")),
            "revision": int(m.group("rev")),
            "title": title,
            "topic": parts[1] if len(parts) > 1 else None,
            "region": parts[2] if len(parts) > 2 else None,
            "value": _to_int(re.sub(r"<[^>]+>", "", value.group("value"))),
            "_span": (m.start(), m.end()),
        }
        if note:
            record["note"] = re.sub(r"<[^>]+>", "", note.group("note")).strip()
        yield record


def _to_int(raw: str):
    try:
        return int(raw.strip())
    except (TypeError, ValueError):
        return raw.strip()
