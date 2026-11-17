"""Content-defined chunking (FastCDC-style gear hash).

Chunk boundaries depend only on the bytes around them, not on absolute offset,
so inserting a prefix (or re-capturing a page that changed in the middle) still
produces identical chunks for unchanged regions. That is what makes chunk
sharing across captures and reference sharing across sources work, and it lets
us pinpoint which chunk of which object is damaged.

`cut_points()` (whole buffer) and `IncrementalChunker` (streaming) implement the
same boundary rule; `tests/test_chunking.py` asserts they agree byte for byte.
"""
from __future__ import annotations

import hashlib
from dataclasses import dataclass

_MASK64 = (1 << 64) - 1


def _gear_table(seed: str = "sealed-wire-vault-gear-v1") -> tuple[int, ...]:
    """Deterministic 256-entry table derived from a seed (no RNG at runtime)."""
    table: list[int] = []
    state = hashlib.sha256(seed.encode()).digest()
    counter = 0
    while len(table) < 256:
        block = hashlib.sha256(state + counter.to_bytes(8, "big")).digest()
        counter += 1
        for i in range(0, 32, 8):
            if len(table) >= 256:
                break
            table.append(int.from_bytes(block[i:i + 8], "little"))
    return tuple(table)


GEAR = _gear_table()


@dataclass(frozen=True)
class Chunk:
    start: int
    end: int
    data: bytes

    @property
    def length(self) -> int:
        return self.end - self.start

    @property
    def digest(self) -> str:
        return hashlib.sha256(self.data).hexdigest()


def _mask_bits(bits: int) -> int:
    return (1 << bits) - 1


class _Params:
    __slots__ = ("min_size", "avg_size", "max_size", "mask_small", "mask_large", "norm")

    def __init__(self, min_size: int, avg_size: int, max_size: int):
        if min_size < 1:
            raise ValueError("min_size must be >= 1")
        if not (min_size <= avg_size <= max_size):
            raise ValueError("require min_size <= avg_size <= max_size")
        self.min_size = min_size
        self.avg_size = avg_size
        self.max_size = max_size
        bits = max(1, int(round(avg_size)).bit_length() - 1)
        if (1 << bits) < avg_size:
            bits += 1
        # Normalized chunking: harder to cut early, easier to cut late.
        self.mask_small = _mask_bits(bits + 1)
        self.mask_large = _mask_bits(max(1, bits - 1))
        self.norm = min(max(avg_size, min_size), max_size)

    def find_cut(self, buf, start: int, avail_end: int) -> int | None:
        """Scan for a natural boundary in (start+min, min(avail_end, start+max)].

        Returns the absolute offset or None. The gear state always resets at a
        chunk start, so the result depends only on bytes from `start` onward.
        """
        hard_end = min(avail_end, start + self.max_size)
        h = 0
        pos = start
        while pos < hard_end:
            h = ((h << 1) + GEAR[buf[pos]]) & _MASK64
            pos += 1
            rel = pos - start
            if rel < self.min_size:
                continue
            mask = self.mask_small if rel < self.norm else self.mask_large
            if (h & mask) == 0:
                return pos
        return None


def cut_points(
    data: bytes,
    min_size: int = 512,
    avg_size: int = 2048,
    max_size: int = 8192,
) -> list[int]:
    """Return chunk boundaries, always including 0 and len(data)."""
    p = _Params(min_size, avg_size, max_size)
    n = len(data)
    if n == 0:
        return [0]
    bounds = [0]
    start = 0
    while start < n:
        remaining = n - start
        if remaining <= p.min_size:
            bounds.append(n)
            break
        hit = p.find_cut(data, start, n)
        if hit is None:
            # No natural boundary in the max window: cut at the hard maximum,
            # unless the whole tail fits and is simply the final chunk.
            if remaining > p.max_size:
                bounds.append(start + p.max_size)
                start += p.max_size
                continue
            bounds.append(n)
            break
        bounds.append(hit)
        start = hit
    if len(bounds) == 1:
        bounds.append(n)
    return bounds


def iter_chunks(
    data: bytes,
    min_size: int = 512,
    avg_size: int = 2048,
    max_size: int = 8192,
):
    bounds = cut_points(data, min_size, avg_size, max_size)
    for a, b in zip(bounds, bounds[1:]):
        yield Chunk(a, b, data[a:b])


class IncrementalChunker:
    """Chunk a stream while it arrives, so a capture can be sealed incrementally.

    Feed arbitrary byte runs with `feed()`; completed chunks come back as
    `(start, end, data)` tuples with absolute offsets. `flush()` emits the
    trailing chunk. Chunks are only emitted once their boundary is unambiguous,
    so the output is identical to `iter_chunks()` over the concatenated stream.
    """

    def __init__(self, min_size=512, avg_size=2048, max_size=8192):
        self.p = _Params(min_size, avg_size, max_size)
        self.buf = bytearray()
        self.stream_hash = hashlib.sha256()
        self.total = 0
        self._base = 0  # absolute offset represented by buf[0]

    def feed(self, data: bytes) -> list[tuple[int, int, bytes]]:
        self.stream_hash.update(data)
        self.buf.extend(data)
        self.total += len(data)
        return self._drain(final=False)

    def _drain(self, final: bool) -> list[tuple[int, int, bytes]]:
        out = []
        while True:
            remaining = self.total - self._base
            if remaining <= 0:
                break
            if remaining <= self.p.min_size:
                if final:
                    out.append(self._emit(remaining))
                break
            hit = self.p.find_cut(self.buf, 0, len(self.buf))
            if hit is not None:
                out.append(self._emit(hit))
                continue
            if remaining >= self.p.max_size:
                # A full max window passed with no boundary: hard cut.
                out.append(self._emit(self.p.max_size))
                continue
            if final:
                # Tail fits in one max-sized chunk; no further data is coming.
                out.append(self._emit(remaining))
            break
        return out

    def _emit(self, length: int) -> tuple[int, int, bytes]:
        data = bytes(self.buf[:length])
        start = self._base
        del self.buf[:length]
        self._base += length
        return (start, start + length, data)

    def flush(self) -> list[tuple[int, int, bytes]]:
        return self._drain(final=True)

    @property
    def hexdigest(self) -> str:
        return self.stream_hash.hexdigest()
