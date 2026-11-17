"""Chunking: determinism, boundary bounds, streaming/whole equivalence, sharing."""
from __future__ import annotations

import random
import unittest

from app.chunking import IncrementalChunker, cut_points, iter_chunks


def payload(n: int, seed: int = 7) -> bytes:
    rng = random.Random(seed)
    words = [b"alpha", b"beta", b"gamma", b"delta", b"<tag>", b"</tag>", b",", b"\n"]
    out = bytearray()
    while len(out) < n:
        out += rng.choice(words)
        out += str(rng.randrange(1000)).encode()
        out += b" "
    return bytes(out[:n])


class ChunkingTests(unittest.TestCase):
    def test_bounds_respected(self):
        data = payload(50000)
        bounds = cut_points(data, 128, 512, 2048)
        self.assertEqual(bounds[0], 0)
        self.assertEqual(bounds[-1], len(data))
        sizes = [b - a for a, b in zip(bounds, bounds[1:])]
        self.assertTrue(all(s <= 2048 for s in sizes), max(sizes))
        # Only the final chunk may be below the minimum.
        self.assertTrue(all(s >= 128 for s in sizes[:-1]), min(sizes[:-1]))

    def test_split_concat_exact(self):
        data = payload(20000)
        rebuilt = b"".join(c.data for c in iter_chunks(data, 128, 512, 2048))
        self.assertEqual(rebuilt, data)

    def test_incremental_matches_whole(self):
        data = payload(30000)
        whole = [(c.start, c.end, c.data) for c in iter_chunks(data, 128, 512, 2048)]
        inc = IncrementalChunker(128, 512, 2048)
        got = []
        rng = random.Random(3)
        i = 0
        while i < len(data):
            step = rng.choice([1, 7, 100, 511, 1024, 4096])
            got += inc.feed(data[i:i + step])
            i += step
        got += inc.flush()
        self.assertEqual(got, whole)
        self.assertEqual(inc.hexdigest, __import__("hashlib").sha256(data).hexdigest())

    def test_incremental_with_huge_step(self):
        data = payload(9000)
        whole = [(c.start, c.end, c.data) for c in iter_chunks(data, 128, 512, 2048)]
        inc = IncrementalChunker(128, 512, 2048)
        got = inc.feed(data) + inc.flush()
        self.assertEqual(got, whole)

    def test_prefix_insert_preserves_tail_chunks(self):
        data = payload(40000)
        base = [c.digest for c in iter_chunks(data, 128, 512, 2048)]
        shifted = b"PREFIX-" * 40 + data
        moved = [c.digest for c in iter_chunks(shifted, 128, 512, 2048)]
        # Content-defined boundaries: most original chunks survive a prefix insert.
        overlap = len(set(base) & set(moved))
        self.assertGreater(overlap, len(base) * 0.5, f"{overlap}/{len(base)}")

    def test_deterministic_across_calls(self):
        data = payload(8000, seed=11)
        a = [(c.start, c.end, c.digest) for c in iter_chunks(data, 128, 512, 2048)]
        b = [(c.start, c.end, c.digest) for c in iter_chunks(data, 128, 512, 2048)]
        self.assertEqual(a, b)

    def test_empty_and_tiny(self):
        self.assertEqual(cut_points(b"", 128, 512, 2048), [0])
        self.assertEqual(cut_points(b"abc", 128, 512, 2048), [0, 3])
        inc = IncrementalChunker(128, 512, 2048)
        self.assertEqual(inc.feed(b""), [])
        self.assertEqual(inc.flush(), [])

    def test_invalid_params(self):
        with self.assertRaises(ValueError):
            cut_points(b"x" * 100, 0, 512, 2048)
        with self.assertRaises(ValueError):
            cut_points(b"x" * 100, 1024, 512, 2048)


if __name__ == "__main__":
    unittest.main()
