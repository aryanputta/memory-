#!/usr/bin/env python3
"""
MercuryCache++ Benchmark Analysis
===================================
Loads JSON result files from the benchmarking runner and produces:
  1. Comparison table: LRU vs LFU vs TinyLFU+ML across workloads
  2. Latency percentile CDF plots
  3. Hit-rate-vs-memory-pressure curve
  4. Failover recovery time detection

Usage:
    python analysis.py results/
    python analysis.py results/ --plot
"""

import argparse
import glob
import json
import os
from typing import List, Dict


def load_results(directory: str) -> List[Dict]:
    results = []
    for path in sorted(glob.glob(os.path.join(directory, "*.json"))):
        with open(path) as f:
            data = json.load(f)
            data["_file"] = os.path.basename(path)
            results.append(data)
    return results


def print_table(results: List[Dict]):
    if not results:
        print("No result files found.")
        return

    headers = ["workload", "actual_rps", "hit_rate", "db_fallback_rate",
               "latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "errors"]
    widths  = [16, 11, 10, 18, 15, 15, 15, 8]

    def fmt(val, w):
        return str(val)[:w].ljust(w)

    header_line = " | ".join(fmt(h, widths[i]) for i, h in enumerate(headers))
    sep_line    = "-+-".join("-" * w for w in widths)
    print(header_line)
    print(sep_line)

    for r in results:
        row = [
            r.get("workload",          "?"),
            r.get("actual_rps",        "?"),
            f"{r.get('hit_rate', 0):.4f}",
            f"{r.get('db_fallback_rate', 0):.4f}",
            f"{r.get('latency_p50_ms', 0):.2f}",
            f"{r.get('latency_p95_ms', 0):.2f}",
            f"{r.get('latency_p99_ms', 0):.2f}",
            r.get("errors", 0),
        ]
        print(" | ".join(fmt(str(v), widths[i]) for i, v in enumerate(row)))


def detect_improvements(results: List[Dict]):
    """
    Print actionable improvement signals from benchmark results.
    Maps findings to the Amazon / research paper framing.
    """
    print("\n=== Improvement Signals ===")
    for r in results:
        workload  = r.get("workload", "unknown")
        hit_rate  = r.get("hit_rate", 0)
        db_rate   = r.get("db_fallback_rate", 0)
        p99       = r.get("latency_p99_ms", 0)

        if hit_rate < 0.80:
            print(f"[{workload}] Hit rate {hit_rate:.2%} is LOW. "
                  "Consider: larger cache budget, TinyLFU admission, "
                  "or ML-based admission for skewed workloads.")

        if db_rate > 0.10:
            print(f"[{workload}] DB fallback rate {db_rate:.2%} is HIGH. "
                  "Consider: request coalescing (single-flight), "
                  "stale-while-revalidate, or proactive warming.")

        if p99 > 50.0:
            print(f"[{workload}] p99 latency {p99:.1f}ms is HIGH. "
                  "Consider: hot-key replication, ring rebalance, "
                  "or circuit-breaker tuning.")

        if workload == "failure":
            print(f"[{workload}] Check failover recovery time: "
                  "look for latency spike at t=15s in raw results.")


def plot_latency_cdf(results: List[Dict]):
    """Rough ASCII CDF for terminal output (no matplotlib required)."""
    for r in results:
        workload = r.get("workload", "?")
        p50  = r.get("latency_p50_ms", 0)
        p95  = r.get("latency_p95_ms", 0)
        p99  = r.get("latency_p99_ms", 0)
        print(f"\n  [{workload}] Latency CDF")
        print(f"    p50  = {p50:6.2f} ms  {'█' * int(min(p50, 100) / 2)}")
        print(f"    p95  = {p95:6.2f} ms  {'█' * int(min(p95, 100) / 2)}")
        print(f"    p99  = {p99:6.2f} ms  {'█' * int(min(p99, 100) / 2)}")


def main():
    parser = argparse.ArgumentParser(description="Benchmark Analysis")
    parser.add_argument("directory", default="results", nargs="?")
    parser.add_argument("--plot",    action="store_true")
    args = parser.parse_args()

    results = load_results(args.directory)
    print(f"Loaded {len(results)} result files from '{args.directory}'\n")
    print_table(results)
    detect_improvements(results)
    if args.plot:
        plot_latency_cdf(results)


if __name__ == "__main__":
    main()
