"""CSV row extractor. Deterministic; spans cover each row including newline."""

import csv
import io

NAME = "csv_rows"
VERSION = 1
DESCRIPTION = "Parse CSV rows into typed records, preserving each row's byte span."


def parse(text, ctx):
    # Spans are computed on the raw text, so parse line by line with the csv
    # module over the whole text but track offsets manually.
    lines = text.splitlines(keepends=True)
    offset = 0
    header = None
    for line in lines:
        start = offset
        offset += len(line)
        if not line.strip():
            continue
        try:
            row = next(csv.reader(io.StringIO(line)))
        except (csv.Error, StopIteration):
            continue
        if header is None:
            header = [h.strip() for h in row]
            continue
        rec = {}
        for i, name in enumerate(header):
            rec[name] = row[i] if i < len(row) else None
        if rec.get("last") is not None:
            try:
                rec["last"] = float(rec["last"])
            except ValueError:
                pass
        if rec.get("change") is not None:
            try:
                rec["change"] = int(rec["change"])
            except ValueError:
                pass
        rec["id"] = rec.get("symbol")
        rec["_span"] = (start, offset)
        yield rec
