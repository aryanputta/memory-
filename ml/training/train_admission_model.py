#!/usr/bin/env python3
"""
MercuryCache++ — ML Admission Model Training
=============================================
Trains an XGBoost binary classifier that predicts whether admitting an incoming
cache entry (over a victim candidate) will improve future hit rate.

Pipeline:
  1. Load access-log traces from benchmarking runs
  2. Extract features per (incoming_key, victim_key) event
  3. Label: 1 if the incoming key was accessed again within the next N requests
  4. Train XGBoost, tune with cross-validation
  5. Export to ONNX for use by both C# (ONNX Runtime) and C++ (ONNX Runtime)

References:
  - TinyLFU: Einziger & Friedman, ACM ToS 2017
  - LeCaR:   Vietri et al., USENIX HotStorage 2018
  - LightGBM: Ke et al., NeurIPS 2017

Usage:
    python train_admission_model.py --traces traces/workload_a.jsonl
    python train_admission_model.py --traces traces/ --export onnx
"""

import argparse
import json
import os
import random
import time
import numpy as np
from typing import List, Tuple

try:
    import xgboost as xgb
    from sklearn.model_selection import StratifiedKFold, cross_val_score
    from sklearn.metrics import roc_auc_score, classification_report
    SKLEARN_AVAILABLE = True
except ImportError:
    SKLEARN_AVAILABLE = False
    print("xgboost/sklearn not installed — running in demo mode")

try:
    import skl2onnx
    from skl2onnx.common.data_types import FloatTensorType
    ONNX_EXPORT_AVAILABLE = True
except ImportError:
    ONNX_EXPORT_AVAILABLE = False

# ── Feature schema (must match FeatureSchema.cs and MlAdmissionPolicy.h) ──
FEATURE_NAMES = [
    "incoming_est_freq_1m",
    "incoming_est_freq_10m",
    "victim_est_freq_1m",
    "victim_est_freq_10m",
    "incoming_recency_ms",
    "victim_recency_ms",
    "incoming_size_bytes",
    "victim_size_bytes",
    "shard_pressure_ratio",
    "tenant_pressure_ratio",
    "key_hotness_score",
    "cache_hit_rate_1m",
    "db_fallback_rate_1m",
    "ttl_ms",
    "write_rate_1m",
    "read_burst_score",
    "workload_class_id",
]
N_FEATURES = len(FEATURE_NAMES)


# ────────────────────────────────────────────────────────────────
# Synthetic trace generator (used when no real traces available)
# ────────────────────────────────────────────────────────────────
def generate_synthetic_traces(n_samples: int = 50_000,
                               seed: int = 42) -> Tuple[np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)

    # Rule: admit if incoming_est_freq > victim_est_freq (TinyLFU-like)
    # but with some noise for realism
    X = rng.random((n_samples, N_FEATURES)).astype(np.float32)

    # Normalise frequency columns to [0, 100]
    X[:, 0] *= 100   # incoming_est_freq_1m
    X[:, 1] *= 100   # incoming_est_freq_10m
    X[:, 2] *= 100   # victim_est_freq_1m
    X[:, 3] *= 100   # victim_est_freq_10m
    # Recency in ms [0, 60_000]
    X[:, 4] *= 60_000
    X[:, 5] *= 60_000
    # Sizes in bytes [64, 65535]
    X[:, 6] = (X[:, 6] * 65471 + 64).astype(np.float32)
    X[:, 7] = (X[:, 7] * 65471 + 64).astype(np.float32)
    # TTL [0, 600_000]
    X[:, 13] *= 600_000
    # workload_class: 0,1,2
    X[:, 16] = rng.integers(0, 3, n_samples).astype(np.float32)

    # Label: admit if incoming freq ≥ victim freq (with 10% noise)
    freq_advantage = (X[:, 0] + X[:, 1]) / 2 - (X[:, 2] + X[:, 3]) / 2
    labels = (freq_advantage >= 0).astype(int)
    # Add noise
    flip = rng.random(n_samples) < 0.10
    labels[flip] = 1 - labels[flip]

    return X, labels


# ────────────────────────────────────────────────────────────────
# Training
# ────────────────────────────────────────────────────────────────
def train(X: np.ndarray, y: np.ndarray) -> "xgb.XGBClassifier":
    model = xgb.XGBClassifier(
        n_estimators      = 300,
        max_depth         = 6,
        learning_rate     = 0.05,
        subsample         = 0.8,
        colsample_bytree  = 0.8,
        use_label_encoder = False,
        eval_metric       = "logloss",
        random_state      = 42,
    )

    # Cross-validation
    cv  = StratifiedKFold(n_splits=5, shuffle=True, random_state=42)
    auc = cross_val_score(model, X, y, cv=cv, scoring="roc_auc")
    print(f"  CV AUC:  mean={auc.mean():.4f}  std={auc.std():.4f}")

    model.fit(X, y)

    # Feature importance
    importance = dict(zip(FEATURE_NAMES, model.feature_importances_))
    top5 = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:5]
    print("  Top-5 features:")
    for name, imp in top5:
        print(f"    {name:<30} {imp:.4f}")

    return model


# ────────────────────────────────────────────────────────────────
# ONNX export
# ────────────────────────────────────────────────────────────────
def export_onnx(model, output_path: str):
    if not ONNX_EXPORT_AVAILABLE:
        print("skl2onnx not available — skipping ONNX export")
        return
    initial_type = [("features", FloatTensorType([None, N_FEATURES]))]
    onnx_model   = skl2onnx.convert_sklearn(model, "admission_model", initial_type)
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    with open(output_path, "wb") as f:
        f.write(onnx_model.SerializeToString())
    print(f"  ONNX model exported to: {output_path}")


# ────────────────────────────────────────────────────────────────
# Main
# ────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--traces",    default=None)
    parser.add_argument("--export",    default="onnx", choices=["onnx", "none"])
    parser.add_argument("--out",       default="../../ml/exported_models/admission_model.onnx")
    parser.add_argument("--n_samples", default=50_000, type=int)
    args = parser.parse_args()

    if not SKLEARN_AVAILABLE:
        print("Demo mode: install xgboost and scikit-learn to train a real model.")
        return

    print("=== MercuryCache++ Admission Model Training ===")

    if args.traces:
        print(f"Loading traces from: {args.traces}")
        # Real impl: parse JSONL access logs and extract features
        # For now, fall back to synthetic
        print("  (trace parsing not yet implemented — using synthetic data)")

    print(f"Generating {args.n_samples:,} synthetic training examples …")
    X, y = generate_synthetic_traces(args.n_samples)
    print(f"  Class balance: {y.mean():.2%} admit / {1-y.mean():.2%} reject")

    print("Training XGBoost model …")
    model = train(X, y)

    if args.export == "onnx":
        export_onnx(model, args.out)

    print("Done.")


if __name__ == "__main__":
    main()
