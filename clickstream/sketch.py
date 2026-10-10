"""K-minimum-values (KMV) sketch for mergeable approximate distinct counts.

Exact while fewer than k distinct values were seen; afterwards the relative
error is about 1/sqrt(k-2) (~3% for k=1024). State per key is bounded by k
integers regardless of cardinality, which is the FR-4 / bottleneck-table
mitigation for high-cardinality distinct counts.
"""
import bisect
import hashlib

_HASH_SPACE = float(1 << 64)


def _hash64(value: str) -> int:
    return int.from_bytes(hashlib.blake2b(value.encode("utf-8"), digest_size=8).digest(), "big")


class KMVSketch:
    def __init__(self, k: int = 1024, hashes=None):
        self.k = k
        self.hashes = list(hashes) if hashes else []  # sorted ascending, distinct

    def add(self, value: str) -> None:
        self._add_hash(_hash64(value))

    def _add_hash(self, h: int) -> None:
        hs = self.hashes
        if len(hs) >= self.k and h >= hs[-1]:
            return
        i = bisect.bisect_left(hs, h)
        if i < len(hs) and hs[i] == h:
            return
        hs.insert(i, h)
        if len(hs) > self.k:
            hs.pop()

    def merge(self, other: "KMVSketch") -> None:
        for h in other.hashes:
            self._add_hash(h)

    def estimate(self) -> int:
        if len(self.hashes) < self.k:
            return len(self.hashes)
        return int(round((self.k - 1) * _HASH_SPACE / self.hashes[-1]))
