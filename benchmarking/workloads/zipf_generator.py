"""
Zipf distribution key generator.
Used to simulate realistic web-cache access patterns where a small fraction
of keys (hot items) receive the vast majority of requests.

Reference: "TinyLFU" paper shows real-world traces follow Zipf with α ≈ 0.9–1.1.
"""

import math
import random
from typing import Iterator


class ZipfGenerator:
    """
    Generates keys according to a Zipf (power-law) distribution.

    With α = 1.0 and N = 10,000 keys:
      - Key #1 receives ~14% of all requests
      - Top 100 keys receive ~54% of all requests
      - Matches observed skew in Amazon product catalog traffic
    """

    def __init__(self, n: int = 10_000, alpha: float = 0.99, seed: int = 42):
        self.n     = n
        self.alpha = alpha
        self.rng   = random.Random(seed)

        # Pre-compute normalisation constant
        self._h = sum(1.0 / (i ** alpha) for i in range(1, n + 1))

        # Build CDF for inverse-CDF sampling
        self._cdf = []
        cumulative = 0.0
        for i in range(1, n + 1):
            cumulative += (1.0 / (i ** alpha)) / self._h
            self._cdf.append(cumulative)

    def next_rank(self) -> int:
        """Return a 1-indexed rank sampled from the Zipf distribution."""
        r = self.rng.random()
        # Binary search in CDF
        lo, hi = 0, len(self._cdf) - 1
        while lo < hi:
            mid = (lo + hi) // 2
            if self._cdf[mid] < r:
                lo = mid + 1
            else:
                hi = mid
        return lo + 1  # 1-indexed

    def next_key(self, namespace: str = "catalog") -> str:
        rank = self.next_rank()
        return f"{namespace}:item:{rank}"

    def stream(self, namespace: str = "catalog") -> Iterator[str]:
        while True:
            yield self.next_key(namespace)


class FlashSaleGenerator:
    """
    Simulates a flash-sale workload: a small burst of requests concentrated
    on very few keys (e.g. top 10 products during a 1-hour sale).
    """

    def __init__(self, hot_key_count: int = 10,
                 hot_fraction: float = 0.95,
                 namespace: str = "catalog",
                 seed: int = 42):
        self.hot_keys     = [f"{namespace}:flash_sale:{i}" for i in range(hot_key_count)]
        self.cold_keys    = [f"{namespace}:item:{i}" for i in range(hot_key_count, 10_000)]
        self.hot_fraction = hot_fraction
        self.rng          = random.Random(seed)

    def next_key(self) -> str:
        if self.rng.random() < self.hot_fraction:
            return self.rng.choice(self.hot_keys)
        else:
            return self.rng.choice(self.cold_keys)

    def stream(self) -> Iterator[str]:
        while True:
            yield self.next_key()
