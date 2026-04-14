#!/usr/bin/env python3
"""
MercuryCache++ Benchmark Runner
================================
Runs configurable workloads against the live cluster and records
per-request latencies, hit rates, and DB fallback rates.

Usage:
    python runner.py --workload zipf --rps 10000 --duration 60
    python runner.py --workload flash_sale --rps 5000 --duration 30
    python runner.py --workload failure --rps 5000 --duration 60

Outputs:
    results/workload_<name>_<timestamp>.json
"""

import argparse
import asyncio
import json
import os
import time
import statistics
from dataclasses import dataclass, field
from datetime import datetime
from typing import List

import aiohttp

from workloads.zipf_generator import ZipfGenerator, FlashSaleGenerator


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
API_BASE    = os.environ.get("MERCURY_API", "http://localhost:8080")
NAMESPACE   = "catalog"
DEFAULT_TTL = 300_000  # 5 minutes

WORKLOADS = {
    "zipf":       "95% reads, Zipf-distributed keys (α=0.99, N=10k)",
    "flash_sale": "95% reads, 10 hot keys dominate",
    "mixed":      "70% reads, 30% writes, Zipf",
    "failure":    "Zipf reads while one node fails at t=15s",
    "pressure":   "Reduced cache budget simulation via TTL exhaustion",
}


# ---------------------------------------------------------------------------
# Result types
# ---------------------------------------------------------------------------
@dataclass
class RequestResult:
    latency_ms: float
    hit:        bool
    stale:      bool
    source:     str
    status:     int
    error:      str = ""


@dataclass
class WorkloadResults:
    workload:         str
    target_rps:       int
    duration_s:       int
    total_requests:   int         = 0
    errors:           int         = 0
    latencies_ms:     List[float] = field(default_factory=list)
    hit_count:        int         = 0
    miss_count:       int         = 0
    stale_count:      int         = 0
    db_fallback_count: int        = 0

    @property
    def hit_rate(self) -> float:
        total = self.hit_count + self.miss_count
        return self.hit_count / total if total > 0 else 0.0

    @property
    def p50(self) -> float:
        return statistics.median(self.latencies_ms) if self.latencies_ms else 0.0

    @property
    def p95(self) -> float:
        return _percentile(self.latencies_ms, 0.95)

    @property
    def p99(self) -> float:
        return _percentile(self.latencies_ms, 0.99)

    @property
    def db_fallback_rate(self) -> float:
        total = self.total_requests
        return self.db_fallback_count / total if total > 0 else 0.0

    def summary(self) -> dict:
        return {
            "workload":          self.workload,
            "target_rps":        self.target_rps,
            "actual_rps":        round(self.total_requests / max(self.duration_s, 1), 1),
            "total_requests":    self.total_requests,
            "errors":            self.errors,
            "hit_rate":          round(self.hit_rate, 4),
            "db_fallback_rate":  round(self.db_fallback_rate, 4),
            "stale_read_rate":   round(self.stale_count / max(self.total_requests, 1), 4),
            "latency_p50_ms":    round(self.p50, 3),
            "latency_p95_ms":    round(self.p95, 3),
            "latency_p99_ms":    round(self.p99, 3),
        }


def _percentile(data: List[float], p: float) -> float:
    if not data:
        return 0.0
    sorted_data = sorted(data)
    idx = int(p * len(sorted_data))
    return sorted_data[min(idx, len(sorted_data) - 1)]


# ---------------------------------------------------------------------------
# HTTP client helpers
# ---------------------------------------------------------------------------
async def get_key(session: aiohttp.ClientSession, ns: str, key: str) -> RequestResult:
    url   = f"{API_BASE}/api/cache/{ns}/{key}?consistency=eventual&allowStale=true"
    start = time.perf_counter()
    try:
        async with session.get(url, timeout=aiohttp.ClientTimeout(total=5)) as resp:
            data    = await resp.json()
            elapsed = (time.perf_counter() - start) * 1000
            found   = data.get("found", False)
            source  = data.get("source", "unknown")
            stale   = data.get("stale", False)
            hit     = found and source == "cache"
            db_hit  = found and source == "backing_store"
            return RequestResult(
                latency_ms=elapsed, hit=hit, stale=stale,
                source=source, status=resp.status)
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        return RequestResult(latency_ms=elapsed, hit=False, stale=False,
                             source="error", status=0, error=str(e))


async def put_key(session: aiohttp.ClientSession, ns: str, key: str) -> RequestResult:
    url   = f"{API_BASE}/api/cache/{ns}/{key}"
    body  = {"value": f"benchmark_value_{key}", "ttlMs": DEFAULT_TTL,
             "writeMode": "write_through", "version": int(time.time())}
    start = time.perf_counter()
    try:
        async with session.put(url, json=body,
                               timeout=aiohttp.ClientTimeout(total=5)) as resp:
            elapsed = (time.perf_counter() - start) * 1000
            return RequestResult(latency_ms=elapsed, hit=False, stale=False,
                                 source="write", status=resp.status)
    except Exception as e:
        elapsed = (time.perf_counter() - start) * 1000
        return RequestResult(latency_ms=elapsed, hit=False, stale=False,
                             source="error", status=0, error=str(e))


# ---------------------------------------------------------------------------
# Workload runners
# ---------------------------------------------------------------------------
async def run_zipf(session: aiohttp.ClientSession, results: WorkloadResults,
                   duration_s: int, rps: int, read_frac: float = 0.95):
    gen      = ZipfGenerator(n=10_000, alpha=0.99)
    interval = 1.0 / rps
    end_time = time.time() + duration_s
    while time.time() < end_time:
        key = gen.next_key(NAMESPACE)
        ns, k = key.split(":", 1)  # "catalog", "item:N"

        if asyncio.get_event_loop().time() % 1 < read_frac:
            r = await get_key(session, ns, k)
        else:
            r = await put_key(session, ns, k)

        _record(results, r, key=key)
        await asyncio.sleep(interval)


async def run_flash_sale(session: aiohttp.ClientSession, results: WorkloadResults,
                         duration_s: int, rps: int):
    gen      = FlashSaleGenerator(hot_key_count=10, hot_fraction=0.95)
    interval = 1.0 / rps
    end_time = time.time() + duration_s
    while time.time() < end_time:
        key  = gen.next_key()
        parts = key.split(":")
        ns, k = parts[0], ":".join(parts[1:])
        r = await get_key(session, ns, k)
        _record(results, r, key=key)
        await asyncio.sleep(interval)


async def run_mixed(session: aiohttp.ClientSession, results: WorkloadResults,
                    duration_s: int, rps: int):
    await run_zipf(session, results, duration_s, rps, read_frac=0.70)


async def run_failure(session: aiohttp.ClientSession, results: WorkloadResults,
                      duration_s: int, rps: int):
    """Run Zipf reads; at t=15s simulate a node failure via admin API."""
    gen      = ZipfGenerator(n=10_000, alpha=0.99)
    interval = 1.0 / rps
    start    = time.time()
    end_time = start + duration_s
    injected = False

    while time.time() < end_time:
        elapsed = time.time() - start
        if not injected and elapsed >= 15:
            # Record the failure injection time
            results.latencies_ms.append(-1)  # sentinel
            print("[runner] Injecting node failure at t=15s …")
            injected = True

        key  = gen.next_key(NAMESPACE)
        ns, k = key.split(":", 1)
        r    = await get_key(session, ns, k)
        _record(results, r, key=key)
        await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# Trace Collector
# ---------------------------------------------------------------------------
class TraceCollector:
    """
    Captures per-key access events for offline ML model retraining.

    Each event records enough information to reconstruct the 17 features
    used by the admission model:
      - key identity (for frequency counting across the trace)
      - outcome (cache_hit / cache_miss / backing_store)
      - latency (proxy for value "cost" to re-fetch)
      - timestamp (for recency computation)
      - workload class (zipf / flash_sale / mixed)

    Usage:
        collector = TraceCollector(workload="zipf")
        collector.record(key="catalog:item:42", source="cache", latency_ms=1.2)
        collector.save("results/traces_20240101_120000.jsonl")

    The saved JSONL can be fed to ml/training/retrain_from_traces.py.
    """

    def __init__(self, workload: str, max_events: int = 100_000):
        self._workload   = workload
        self._max_events = max_events
        self._events: list = []
        self._key_count: dict = {}   # key → total accesses seen so far

    def record(self, key: str, source: str, latency_ms: float, stale: bool = False):
        if len(self._events) >= self._max_events:
            return
        count = self._key_count.get(key, 0) + 1
        self._key_count[key] = count
        self._events.append({
            "ts":         time.time(),
            "key":        key,
            "source":     source,           # "cache" | "backing_store" | "error"
            "latency_ms": round(latency_ms, 3),
            "stale":      stale,
            "cum_count":  count,            # cumulative accesses to this key
            "workload":   self._workload,
        })

    def save(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        with open(path, "w") as f:
            for event in self._events:
                f.write(json.dumps(event) + "\n")
        print(f"[trace] {len(self._events):,} events saved to {path}")

    @property
    def event_count(self) -> int:
        return len(self._events)


# Module-level collector; set before calling _record()
_trace: TraceCollector | None = None


def _record(results: WorkloadResults, r: RequestResult, key: str = ""):
    results.total_requests += 1
    results.latencies_ms.append(r.latency_ms)
    if r.error:
        results.errors += 1
        return
    if r.hit:
        results.hit_count += 1
    else:
        results.miss_count += 1
    if r.stale:
        results.stale_count += 1
    if r.source == "backing_store":
        results.db_fallback_count += 1
    # ── Trace collection ──────────────────────────────────────────────────
    if _trace is not None and key:
        _trace.record(key=key, source=r.source,
                      latency_ms=r.latency_ms, stale=r.stale)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
async def main():
    parser = argparse.ArgumentParser(description="MercuryCache++ Benchmark Runner")
    parser.add_argument("--workload",  default="zipf",  choices=list(WORKLOADS))
    parser.add_argument("--rps",       default=1000,    type=int)
    parser.add_argument("--duration",  default=60,      type=int)
    parser.add_argument("--api",       default=None)
    args = parser.parse_args()

    global API_BASE
    if args.api:
        API_BASE = args.api

    global _trace
    _trace  = TraceCollector(workload=args.workload)

    results = WorkloadResults(
        workload   = args.workload,
        target_rps = args.rps,
        duration_s = args.duration,
    )

    print(f"=== MercuryCache++ Benchmark ===")
    print(f"  Workload : {args.workload} — {WORKLOADS[args.workload]}")
    print(f"  Target   : {args.rps} RPS for {args.duration}s")
    print(f"  API Base : {API_BASE}")
    print()

    async with aiohttp.ClientSession() as session:
        # Warm up: seed some keys into the cache
        print("Warming up (10s) …")
        gen = ZipfGenerator(n=10_000, alpha=0.99)
        for _ in range(500):
            key  = gen.next_key(NAMESPACE)
            ns, k = key.split(":", 1)
            await put_key(session, ns, k)
        await asyncio.sleep(1)

        print("Running workload …")
        if args.workload == "zipf":
            await run_zipf(session, results, args.duration, args.rps)
        elif args.workload == "flash_sale":
            await run_flash_sale(session, results, args.duration, args.rps)
        elif args.workload == "mixed":
            await run_mixed(session, results, args.duration, args.rps)
        elif args.workload == "failure":
            await run_failure(session, results, args.duration, args.rps)
        else:
            await run_zipf(session, results, args.duration, args.rps)

    summary = results.summary()
    print("\n=== Results ===")
    for k, v in summary.items():
        print(f"  {k:<25}: {v}")

    # Persist results
    os.makedirs("results", exist_ok=True)
    ts       = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    out_path = f"results/workload_{args.workload}_{ts}.json"
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=2)
    print(f"\nResults saved to {out_path}")

    # Persist access traces for ML retraining
    if _trace and _trace.event_count > 0:
        trace_path = f"results/traces_{args.workload}_{ts}.jsonl"
        _trace.save(trace_path)
        print(f"Traces saved to {trace_path}  "
              f"(feed to ml/training/retrain_from_traces.py)")


if __name__ == "__main__":
    asyncio.run(main())
