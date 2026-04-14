#!/usr/bin/env python3
"""
MercuryCache++ — Offline ML Admission Model Retraining
=======================================================
Reads access-trace JSONL files captured by the benchmark runner
(benchmarking/results/traces_*.jsonl), derives training labels from the
actual hit/miss outcomes, extracts the 17-feature schema used by the
C++ MlAdmissionPolicy, and re-trains the XGBoost admission classifier.

The resulting model is exported as ONNX and written to
  ml/models/admission_model.onnx

which the C++ server loads on startup when compiled with -DWITH_ONNX=ON.

Usage:
    # After a benchmark run that produced trace files:
    python retrain_from_traces.py
    python retrain_from_traces.py --traces-dir ../../benchmarking/results
    python retrain_from_traces.py --threshold 0.6 --output ../models/custom.onnx

How labelling works:
    A cache admission decision is "correct" if the admitted key is later
    accessed again (i.e., the entry was worth caching).  We reconstruct
    this from the trace by labelling each *miss* event:
        admit=1  if the key is accessed ≥ MIN_FUTURE_HITS more times after
                 this event within a 60-second lookahead window.
        admit=0  otherwise (one-hit wonder — not worth caching).

    This mirrors the W-TinyLFU paper's observation that future reuse is the
    ground truth for admission decisions.
"""

import argparse
import collections
import glob
import json
import math
import os
import sys
import time
from dataclasses import dataclass, field
from typing import List, Tuple

import numpy as np

# ── Optional ML dependencies ─────────────────────────────────────────────────
try:
    import xgboost as xgb
    from sklearn.model_selection import StratifiedKFold, cross_val_score
    from sklearn.metrics import roc_auc_score
    HAS_ML = True
except ImportError:
    HAS_ML = False

try:
    from skl2onnx import convert_sklearn
    from skl2onnx.common.data_types import FloatTensorType
    HAS_ONNX = True
except ImportError:
    HAS_ONNX = False

# ── Constants (must match FeatureSchema.cs and MlAdmissionPolicy.h) ──────────
FEATURE_COUNT    = 17
MIN_FUTURE_HITS  = 2       # hits within lookahead window to be labelled admit=1
LOOKAHEAD_SECS   = 60.0    # future window for label derivation
ADMISSION_THRESH = 0.55    # must match C++ threshold


# ── Feature extraction ────────────────────────────────────────────────────────

@dataclass
class KeyStats:
    """Running statistics for a single cache key extracted from trace events."""
    accesses:       List[float] = field(default_factory=list)  # timestamps
    hit_count:      int   = 0
    miss_count:     int   = 0
    total_latency:  float = 0.0
    workload_class: int   = 0   # 0=zipf, 1=flash_sale, 2=mixed


WORKLOAD_ID = {"zipf": 0, "flash_sale": 1, "mixed": 2, "failure": 2, "pressure": 2}


def extract_features(key: str, event: dict, stats: "KeyStats",
                     global_hit_rate: float, global_db_rate: float,
                     now_ts: float) -> np.ndarray:
    """
    Construct the 17-feature vector for one cache admission event.

    Features (index order must match FeatureSchema.cs):
      0  incoming_freq_1m   — accesses in the last 60 s
      1  incoming_freq_10m  — accesses in the last 600 s
      2  victim_freq_1m     — proxy: global miss rate × total requests
      3  victim_freq_10m    — same, over 10-min window
      4  recency_ms         — ms since last access to this key
      5  value_size_bytes   — estimated; 128 B default
      6  ttl_ms             — default 300 000 ms
      7  shard_pressure     — proxy: global_hit_rate (inverse of pressure)
      8  tenant_pressure    — same as shard_pressure (no per-tenant data)
      9  key_hotness        — freq_1m / (freq_1m + 1)
      10 hit_rate           — global cache hit rate
      11 db_fallback_rate   — global DB fallback rate
      12 write_rate         — fraction of events that are writes (0 for GETs)
      13 read_burst_score   — freq_1m / max(freq_10m, 1)
      14 workload_class_id  — 0=zipf, 1=flash_sale, 2=mixed
      15 is_flash_sale_key  — 1 if key contains "flash_sale", else 0
      16 key_hash_norm      — stable hash of key, normalised to [0,1]
    """
    ts = event["ts"]

    freq_1m  = sum(1 for t in stats.accesses if ts - t <= 60)
    freq_10m = sum(1 for t in stats.accesses if ts - t <= 600)
    recency  = (ts - stats.accesses[-2]) * 1000 if len(stats.accesses) >= 2 else 300_000.0

    victim_proxy_1m  = max(1 - global_hit_rate, 0.01) * max(len(stats.accesses), 1)
    victim_proxy_10m = victim_proxy_1m * 10

    key_hotness     = freq_1m / (freq_1m + 1.0)
    read_burst      = freq_1m / max(freq_10m, 1.0)
    key_hash        = abs(hash(key)) % 10_000 / 10_000.0
    is_flash        = 1.0 if "flash_sale" in key else 0.0
    wl_class        = float(stats.workload_class)

    return np.array([
        float(freq_1m),
        float(freq_10m),
        float(victim_proxy_1m),
        float(victim_proxy_10m),
        float(recency),
        128.0,                   # value_size_bytes (constant; no size in trace)
        300_000.0,               # ttl_ms
        float(global_hit_rate),
        float(global_hit_rate),  # tenant_pressure ≈ shard_pressure
        key_hotness,
        float(global_hit_rate),
        float(global_db_rate),
        0.0,                     # write_rate (traces are read events)
        read_burst,
        wl_class,
        is_flash,
        key_hash,
    ], dtype=np.float32)


# ── Trace loading + label derivation ─────────────────────────────────────────

def load_traces(traces_dir: str) -> List[dict]:
    pattern = os.path.join(traces_dir, "traces_*.jsonl")
    files   = sorted(glob.glob(pattern))
    if not files:
        print(f"[retrain] No trace files found matching {pattern}", file=sys.stderr)
        sys.exit(1)

    events = []
    for path in files:
        print(f"[retrain] Loading {path} …", end=" ")
        count = 0
        with open(path) as f:
            for line in f:
                line = line.strip()
                if line:
                    events.append(json.loads(line))
                    count += 1
        print(f"{count:,} events")
    print(f"[retrain] Total: {len(events):,} events from {len(files)} file(s)")
    return events


def build_dataset(
    events: List[dict],
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Convert raw trace events into (X, y) arrays for training.

    Only *miss* events become training samples (we are deciding whether to
    admit the incoming key that just missed).  Each miss is labelled admit=1
    if the key appears ≥ MIN_FUTURE_HITS times in the next LOOKAHEAD_SECS.
    """
    # Sort by timestamp
    events.sort(key=lambda e: e["ts"])

    # Build per-key stats and future-access index
    key_stats: dict[str, KeyStats] = collections.defaultdict(KeyStats)
    future_accesses: dict[str, List[float]] = collections.defaultdict(list)

    for ev in events:
        key   = ev["key"]
        ts    = ev["ts"]
        wl_id = WORKLOAD_ID.get(ev.get("workload", "zipf"), 0)
        stats = key_stats[key]
        stats.accesses.append(ts)
        stats.workload_class = wl_id
        if ev["source"] == "cache":
            stats.hit_count += 1
        else:
            stats.miss_count += 1
        stats.total_latency += ev["latency_ms"]
        future_accesses[key].append(ts)

    total       = len(events)
    hit_events  = sum(1 for e in events if e["source"] == "cache")
    db_events   = sum(1 for e in events if e["source"] == "backing_store")
    global_hr   = hit_events / total if total > 0 else 0.5
    global_dbr  = db_events  / total if total > 0 else 0.1

    X_rows, y_rows = [], []

    for ev in events:
        if ev["source"] == "cache":
            continue  # only train on miss events

        key  = ev["key"]
        ts   = ev["ts"]
        stats = key_stats[key]

        # Label: does this key get accessed again soon?
        future = [t for t in future_accesses[key]
                  if t > ts and t <= ts + LOOKAHEAD_SECS]
        label = 1 if len(future) >= MIN_FUTURE_HITS else 0

        feats = extract_features(key, ev, stats, global_hr, global_dbr, ts)
        X_rows.append(feats)
        y_rows.append(label)

    if not X_rows:
        print("[retrain] No miss events found — cannot build dataset", file=sys.stderr)
        sys.exit(1)

    X = np.stack(X_rows).astype(np.float32)
    y = np.array(y_rows, dtype=np.int32)

    admit_rate = y.mean()
    print(f"[retrain] Dataset: {len(X):,} samples  "
          f"admit_rate={admit_rate:.2%}  features={X.shape[1]}")
    return X, y


# ── Training ──────────────────────────────────────────────────────────────────

def train_and_export(X: np.ndarray, y: np.ndarray,
                     output_path: str, threshold: float):
    if not HAS_ML:
        print("[retrain] xgboost / scikit-learn not installed.  "
              "Run: pip install xgboost scikit-learn skl2onnx", file=sys.stderr)
        sys.exit(1)

    print("[retrain] Training XGBoost classifier …")
    clf = xgb.XGBClassifier(
        n_estimators      = 200,
        max_depth         = 6,
        learning_rate     = 0.05,
        subsample         = 0.8,
        colsample_bytree  = 0.8,
        use_label_encoder = False,
        eval_metric       = "logloss",
        random_state      = 42,
    )

    # Cross-validate
    cv = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    scores = cross_val_score(clf, X, y, cv=cv, scoring="roc_auc")
    print(f"[retrain] CV ROC-AUC: {scores.mean():.4f} ± {scores.std():.4f}")

    # Final fit on full dataset
    clf.fit(X, y)
    train_auc = roc_auc_score(y, clf.predict_proba(X)[:, 1])
    print(f"[retrain] Train ROC-AUC: {train_auc:.4f}")

    admit_at_threshold = (clf.predict_proba(X)[:, 1] >= threshold).mean()
    print(f"[retrain] Admission rate at threshold={threshold}: "
          f"{admit_at_threshold:.2%}")

    # Export to ONNX
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    if HAS_ONNX:
        initial_type = [("float_input", FloatTensorType([None, FEATURE_COUNT]))]
        onnx_model   = convert_sklearn(clf, initial_types=initial_type)
        with open(output_path, "wb") as f:
            f.write(onnx_model.SerializeToString())
        size_kb = os.path.getsize(output_path) / 1024
        print(f"[retrain] ONNX model written to {output_path}  ({size_kb:.1f} KB)")
    else:
        # Save as JSON for manual ONNX conversion
        json_path = output_path.replace(".onnx", "_params.json")
        clf.save_model(json_path)
        print(f"[retrain] skl2onnx not available; model saved as {json_path}")
        print(f"          Install skl2onnx for ONNX export: pip install skl2onnx")

    # Write metadata
    meta_path = output_path.replace(".onnx", "_metadata.json")
    with open(meta_path, "w") as f:
        json.dump({
            "trained_at":       time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "feature_count":    FEATURE_COUNT,
            "admission_threshold": threshold,
            "cv_roc_auc_mean":  float(scores.mean()),
            "cv_roc_auc_std":   float(scores.std()),
            "train_samples":    len(X),
            "admit_rate":       float(y.mean()),
        }, f, indent=2)
    print(f"[retrain] Metadata written to {meta_path}")


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    global MIN_FUTURE_HITS
    parser = argparse.ArgumentParser(
        description="Retrain MercuryCache admission model from benchmark traces")
    parser.add_argument(
        "--traces-dir",
        default=os.path.join(os.path.dirname(__file__),
                             "../../benchmarking/results"),
        help="Directory containing traces_*.jsonl files")
    parser.add_argument(
        "--output",
        default=os.path.join(os.path.dirname(__file__),
                             "../models/admission_model.onnx"),
        help="Output ONNX model path")
    parser.add_argument(
        "--threshold", type=float, default=ADMISSION_THRESH,
        help="Admission probability threshold (default 0.55)")
    parser.add_argument(
        "--min-future-hits", type=int, default=MIN_FUTURE_HITS,
        help="Min future accesses in lookahead window to label admit=1")
    args = parser.parse_args()

    MIN_FUTURE_HITS = args.min_future_hits

    traces_dir = os.path.realpath(args.traces_dir)
    output     = os.path.realpath(args.output)

    print(f"=== MercuryCache++ Admission Model Retraining ===")
    print(f"  Traces dir : {traces_dir}")
    print(f"  Output     : {output}")
    print(f"  Threshold  : {args.threshold}")
    print()

    events  = load_traces(traces_dir)
    X, y    = build_dataset(events)
    train_and_export(X, y, output, args.threshold)

    print("\nDone. Copy the ONNX file to each cache node and restart with -DWITH_ONNX=ON.")


if __name__ == "__main__":
    main()
