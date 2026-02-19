# src/detector/ml.py

from __future__ import annotations
from typing import Dict, Any, List
from pathlib import Path
import json

import numpy as np
import pandas as pd
import joblib

# Path to the frozen Scenario-1 detection model
_REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = _REPO_ROOT / "out" / "r5.2" / "ml"

_feature_cols: List[str] | None = None
_scaler = None
_model = None
_threshold: float | None = None  # you can tune this later


def _load_artifacts() -> None:
    """Load feature spec, scaler, and trained model into module-level globals."""
    global _feature_cols, _scaler, _model, _threshold

    if _feature_cols is not None:
        return  # already loaded

    spec_path = MODEL_DIR / "feature_spec.json"
    scaler_path = MODEL_DIR / "scaler.pkl"
    model_path = MODEL_DIR / "supervised_model_xgb.pkl"

    # Check if model exists, if not skip ML detector
    if not model_path.exists():
        print(f"[ML] Model not found at {model_path} - ML detector disabled")
        _feature_cols = []  # Mark as loaded but empty
        return

    spec = json.loads(spec_path.read_text())

    # CHANGE THIS to match whatever key actually holds your feature list
    FEATURE_LIST_KEY = "features"  # e.g. "feature_cols", "columns", "input_cols"
    _feature_cols = spec[FEATURE_LIST_KEY]

    _scaler = joblib.load(scaler_path)
    _model = joblib.load(model_path)

    # Optional: if you stored a threshold somewhere, load it here.
    # For now, use 0.5 as a placeholder.
    _threshold = 0.5


def _window_to_feature_vector(user_window: list[dict]) -> np.ndarray:
    """
    Convert the last 14 days of a user's window into a 1D feature vector
    using the same column ordering as in training.
    """
    assert _feature_cols is not None

    # Take only the last 14 entries
    last = user_window[-14:]

    rows = []
    for entry in last:
        # Each entry is {"day": "...", "features": {...}}
        feats = dict(entry.get("features", {}))
        rows.append(feats)

    df = pd.DataFrame(rows)

    # If any columns are missing (early days, weird windows), fill with 0
    for col in _feature_cols:
        if col not in df.columns:
            df[col] = 0.0

    df = df[_feature_cols]
    X = df.to_numpy().reshape(1, -1)
    return X


def check(ctx: Dict[str, Any]) -> list[Dict[str, Any]]:
    """
    Run the frozen Scenario-1 supervised detector.

    ctx structure from run_loop:
      {
        "user_key": str,
        "window": [ { "day": str, "features": {...} }, ... ],
        "features": {...},                 # today's row
        "rules_score": float,
        "anomaly_score": float,
      }

    Returns a list of alert dicts:
      [ { "reason": "ml:supervised_s1", "score": prob }, ... ]
    """
    _load_artifacts()
    
    # If model not loaded (disabled), return empty
    if not _feature_cols or _model is None:
        return []
    
    window = ctx["window"]
    user_key = ctx["user_key"]

    # Need a full 14-day window to match training.
    if len(window) < 14:
        return []

    X = _window_to_feature_vector(window)
    X_scaled = _scaler.transform(X)

    proba = float(_model.predict_proba(X_scaled)[0, 1])
    thr = _threshold if _threshold is not None else 0.5

    if proba < thr:
        return []

    return [{
        "user_key": user_key,
        "reason": "ml:supervised_s1",
        "score": proba,
    }]