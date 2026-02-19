"""
Anomaly Detector (Online Mode) 

Integrates with run_loop.py using Isolation Forest trained on normal behavior.
Returns raw IF anomaly scores (0-1) for ensemble integration.

Z-score baseline analysis is available offline via window_zscores.parquet lookup.

Interface: check(ctx: Dict[str, Any]) -> List[Dict[str, Any]]
"""

from __future__ import annotations
from typing import Dict, Any, List
from pathlib import Path
import json
import numpy as np
import joblib

_REPO_ROOT = Path(__file__).resolve().parents[2]
MODEL_DIR = _REPO_ROOT / "out" / "r5.2" / "anomaly"

_model = None
_score_scaler = None
_feature_scaler = None
_variance_selector = None
_feature_config = None

_z_lookup = None  # type: ignore


def _load_zscores() -> None:
    """
    Load per-window z-scores from window_zscores.parquet into an in-memory lookup.

    Key:   (user_key, end_day_yyyy_mm_dd)
    Value: {"z_personal": float | NaN,
            "z_role": float | NaN,
            "z_max": float | NaN}
    """
    global _z_lookup
    if _z_lookup is not None:
        # Already loaded
        return

    from pathlib import Path
    import duckdb

    # Resolve release + path
    try:
        rel = Path("release.txt").read_text().strip()
    except FileNotFoundError:
        raise RuntimeError(
            "[ANOMALY] release.txt not found; cannot locate window_zscores.parquet"
        )

    zs_path = Path(f"out/{rel}/anomaly/window_zscores.parquet")

    if not zs_path.exists():
        raise RuntimeError(
            f"[ANOMALY] Expected z-score parquet at {zs_path}, but it does not exist.\n"
            "Run the anomaly pipeline first:\n"
            "  1) python -m src.anomaly.build_user_roles\n"
            "  2) python -m src.anomaly.build_user_org_structure\n"
            "  3) python -m src.anomaly.train_isolation_forest\n"
            "  4) python -m src.anomaly.build_window_scores\n"
            "  5) python -m src.anomaly.compute_baselines"
        )

    print(f"[ANOMALY] Loading z-scores from {zs_path}")

    con = duckdb.connect(database=":memory:")
    df = con.execute(
        f"""
        SELECT
            lower(user_key) AS user_key,
            end_day,
            z_personal,
            z_role,
            z_max
        FROM '{zs_path}'
        """
    ).df()
    con.close()

    lookup = {}
    for _, row in df.iterrows():
        # end_day is a timestamp; normalize to YYYY-MM-DD string
        key = (str(row["user_key"]).lower(), str(row["end_day"])[:10])
        lookup[key] = {
            "z_personal": row["z_personal"],
            "z_role": row["z_role"],
            "z_max": row["z_max"],
        }

    # Hard guard: empty / tiny lookup = refuse to run
    if not lookup:
        raise RuntimeError(
            "[ANOMALY] z-score lookup is EMPTY after loading window_zscores.parquet.\n"
            "Refusing to run anomaly detector with raw scores only."
        )

    # Optional sanity threshold so “half-baked” artifacts don’t sneak in
    if len(lookup) < 1000:
        raise RuntimeError(
            f"[ANOMALY] z-score lookup is suspiciously small ({len(lookup)} entries).\n"
            "Refusing to run. Check that compute_baselines finished correctly."
        )

    _z_lookup = lookup
    print(f"[ANOMALY] Loaded z-scores for {len(_z_lookup):,} windows")

# def _lookup_is_loaded() -> bool:
#     return _z_lookup is not None


def _load_artifacts() -> None:
    """Load trained Isolation Forest model and feature engineering pipeline."""
    global _model, _score_scaler, _feature_scaler, _variance_selector, _feature_config
    
    if _model is not None:
        return
    
    if not (MODEL_DIR / "isolation_forest.pkl").exists():
        print(f"[ANOMALY] Model not found at {MODEL_DIR}. Detector disabled.")
        return
    
    _model = joblib.load(MODEL_DIR / "isolation_forest.pkl")
    _score_scaler = joblib.load(MODEL_DIR / "score_scaler.pkl")
    _feature_scaler = joblib.load(MODEL_DIR / "feature_scaler.pkl")
    _variance_selector = joblib.load(MODEL_DIR / "variance_selector.pkl")
    
    with open(MODEL_DIR / "feature_config.json") as f:
        _feature_config = json.load(f)
    
    print(f"[ANOMALY] Isolation Forest loaded")


def _window_to_features(window: List[Dict[str, Any]], user_key: str) -> np.ndarray:
    """Convert 14-day window to feature vector using same pipeline as batch training."""
    features_list = _feature_config["features"]
    num_features = _feature_config["num_features"]
    num_days = _feature_config["num_days"]
    relevant_indices = _feature_config["relevant_indices"]
    
    last_14 = window[-14:] if len(window) >= 14 else window
    X_flat = np.zeros((1, num_features * num_days), dtype=np.float32)
    
    for day_idx, entry in enumerate(last_14):
        features_dict = entry.get("features", {})
        for feat_idx, feat_name in enumerate(features_list):
            col_idx = feat_idx * num_days + day_idx
            val = features_dict.get(feat_name, 0.0)
            try:
                val_float = float(val)
                X_flat[0, col_idx] = val_float if not np.isnan(val_float) else 0.0
            except (TypeError, ValueError):
                X_flat[0, col_idx] = 0.0
    
    # Reshape to (1, num_features, num_days)
    X_3d = X_flat.reshape(1, num_features, num_days)
    
    # Aggregate FIRST (same as training): mean, max, std over 14 days
    X_mean = np.mean(X_3d, axis=2)  # Shape: (1, 53)
    X_max = np.max(X_3d, axis=2)    # Shape: (1, 53)
    X_std = np.std(X_3d, axis=2)     # Shape: (1, 53)
    X_agg = np.hstack([X_mean, X_max, X_std])  # Shape: (1, 159)
    
    # Apply variance selection (same as training)
    X_sel = _variance_selector.transform(X_agg)  # Shape: (1, num_selected_features)
    
    # Apply standard scaling (same as training)
    X_final = _feature_scaler.transform(X_sel)
    
    return X_final


def check(ctx: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Run Isolation Forest anomaly detection on user's 14-day window.
    
    Returns raw IF anomaly score (0-1 scale) for ensemble integration.
    Always returns a score - no thresholding or filtering applied here.
    
    """
    _load_artifacts()
    
    if _model is None:
        return []
    
    window = ctx["window"]
    user_key = ctx["user_key"]
    
    # Need at least 7 days of data for meaningful feature extraction
    if len(window) < 7:
        return []
    
    # Extract features and compute IF score
    try:
        X = _window_to_features(window, user_key)
    except Exception as e:
        print(f"[ANOMALY ERROR] Feature extraction failed for {user_key}: {e}")
        return []
    
    # Get raw decision function output (higher = more anomalous)
    base_score_raw = float(-_model.decision_function(X)[0])

    # Scale to 0-1 range using training-time scaler
    base_score = float(_score_scaler.transform([[base_score_raw]])[0, 0])

    # Get window end date for z-score lookup
    window_end = window[-1]["day"] if window else None
    window_end_str = str(window_end)[:10] if window_end is not None else None

    # Load z-scores and look up this window
    _load_zscores()
    z_personal = None
    z_role = None
    z_max = None

    if _z_lookup and window_end_str is not None:
        key = (user_key.lower(), window_end_str)
        row = _z_lookup.get(key)
        if row:
            z_personal = row.get("z_personal")
            z_role = row.get("z_role")
            z_max = row.get("z_max")

    # ---------- boost / damp logic ----------

    # If base score is tiny and we don't have any strong z-signal, bail early
    if base_score < 0.5 and (z_max is None or z_max < 2.0):
        return []

    boost_pct = 0.0

    if z_max is not None:
        if z_max >= 3.5:
            boost_pct = 0.3    # was 0.5
        elif z_max >= 3.0:
            boost_pct = 0.2    # was 0.3
        elif z_max >= 2.5:
            boost_pct = 0.1    # was 0.15
        elif z_max < 1.0:
            boost_pct = -0.5   # stronger damp for "within profile"

    boosted_score = base_score * (1.0 + boost_pct)
    # Global cap so anomaly doesn't dominate even before ensemble:
    boosted_score = max(0.0, min(0.9, boosted_score))

    # If after damping it's tiny, don't alert
    if boosted_score < 0.4:
        return []

    return [{
        "reason": "anomaly:isolation_forest",
        "score": boosted_score,
        "evidence": {
            # for evaluation scripts
            "anomaly_score_raw": round(base_score, 4),
            "anomaly_score_boosted": round(boosted_score, 4),
            "boost_pct": round(boost_pct * 100, 1),
            # extra debug
            "base_score_raw_if": round(base_score_raw, 4),
            "z_personal": None if z_personal is None else round(z_personal, 2),
            "z_role": None if z_role is None else round(z_role, 2),
            "z_max": None if z_max is None else round(z_max, 2),
            "window_days": len(window),
            "window_end": window_end_str,
        },
    }]
