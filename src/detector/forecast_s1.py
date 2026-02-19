# src/detector/forecast_s1.py

from __future__ import annotations
from typing import Dict, Any, List
from pathlib import Path
import json
import pickle

import numpy as np
import pandas as pd
import joblib

# Root of the repo, same pattern as src/detector/ml.py
_REPO_ROOT = Path(__file__).resolve().parents[2]

_feature_cols: List[str] | None = None
_scaler = None
_model = None

# Initial forecast threshold: can be tuned later based on eval_supervised_forecast.py
_threshold: float | None = 0.5


def _read_release_tag() -> str:
    """
    Read the active release tag from release.txt.
    Falls back to 'r5.2' if missing so local tests don't implode.
    """
    try:
        text = (_REPO_ROOT / "release.txt").read_text().strip()
        return text or "r5.2"
    except Exception:
        return "r5.2"


def _load_artifacts() -> None:
    """
    Load feature spec, forecast scaler, and forecast model into module-level globals.

    This uses:
      - out/<REL>/ml/feature_spec.json
      - out/<REL>/ml_forecast/scaler_forecast.pkl
      - out/<REL>/ml_forecast/supervised_model_xgb_forecast.pkl
    """
    global _feature_cols, _scaler, _model, _threshold

    # Already loaded
    if _feature_cols is not None:
        return

    rel = _read_release_tag()
    root = _REPO_ROOT / "out" / rel

    spec_path = root / "ml" / "feature_spec.json"
    scaler_path = root / "ml_forecast" / "scaler_forecast.pkl"
    model_path = root / "ml_forecast" / "supervised_model_xgb_forecast.pkl"

    if not model_path.exists():
        print(f"[FORECAST] Model not found at {model_path} - forecast detector disabled")
        _feature_cols = []  # mark as loaded but empty
        return

    if not spec_path.exists():
        raise FileNotFoundError(f"[FORECAST] feature_spec.json not found at {spec_path}")

    spec = json.loads(spec_path.read_text())
    FEATURE_LIST_KEY = "features"
    feats = spec.get(FEATURE_LIST_KEY)
    if not isinstance(feats, list) or not feats:
        raise ValueError("[FORECAST] feature_spec.json is missing a non-empty 'features' list")

    _feature_cols = feats
    _scaler = joblib.load(scaler_path)

    with model_path.open("rb") as f:
        _model = pickle.load(f)

    # Threshold stays at 0.5 for now; if you decide to tune it based on eval_supervised_forecast.py,
    # you can either hard-code that here or load it from a small JSON next to the model.


def _window_to_feature_vector(user_window: list[dict]) -> np.ndarray:
    """
    Convert the last 14 days of a user's window into a 1D feature vector
    using the same column ordering as in training.

    Each entry in user_window is expected to look like:
      { "day": "YYYY-MM-DD", "features": {<daily_user columns>} }

    We:
      - take the last 14 entries,
      - build a DataFrame with one row per day,
      - select feature_spec columns in order,
      - flatten to shape (1, 14 * F).
    """
    assert _feature_cols is not None

    # Take only the last 14 entries
    last = user_window[-14:]

    rows = []
    for entry in last:
        feats = dict(entry.get("features", {}))
        rows.append(feats)

    df = pd.DataFrame(rows)

    # If any columns are missing (early days / sparse history), fill with 0
    for col in _feature_cols:
        if col not in df.columns:
            df[col] = 0.0

    df = df[_feature_cols]
    X = df.to_numpy().reshape(1, -1)
    return X


def check(ctx: Dict[str, Any]) -> list[Dict[str, Any]]:
    """
    Run the frozen Scenario-1 **forecast** model.

    ctx structure from run_loop (same as for ml.check):
      {
        "user_key": str,
        "window": [ { "day": str, "features": {...} }, ... ],
        "features": {...},                 # today's row (unused here)
        "rules_score": float,              # currently ignored by forecast
        "anomaly_score": float,            # currently ignored by forecast
      }

    Returns a list of alert dicts:
      [
        {
          "user_key": str,
          "reason": "forecast:s1_exfil_7d",
          "score": float,   # probability in [0, 1]
        },
        ...
      ]

    We emit at most one forecast alert per (user, day), and only if the
    predicted probability exceeds the forecast threshold.
    """
    _load_artifacts()

    # If model not loaded (disabled), return empty
    if not _feature_cols or _model is None or _scaler is None:
        return []

    window = ctx["window"]
    user_key = ctx["user_key"]

    # Need a full 14-day window to match training.
    if len(window) < 14:
        return []

    X = _window_to_feature_vector(window)
    X_scaled = _scaler.transform(X)

    # XGBClassifier supports predict_proba; keep a fallback just in case.
    if hasattr(_model, "predict_proba"):
        proba = float(_model.predict_proba(X_scaled)[0, 1])
    elif hasattr(_model, "decision_function"):
        raw = _model.decision_function(X_scaled)
        proba = float(1.0 / (1.0 + np.exp(-raw[0])))
    else:
        # Model is not in a usable form for probabilities
        return []

    thr = _threshold if _threshold is not None else 0.5
    if proba < thr:
        return []

    return [
        {
            "user_key": user_key,
            "reason": "forecast:s1_exfil_7d",
            "score": proba,
        }
    ]