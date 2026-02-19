"""
Evaluate the supervised **forecast** model for Scenario 1 (S1).

This is the v2 forecasting evaluator. It is completely separate from the
v1 detection evaluator and only reads from the forecast-specific paths:

    - out/<REL>/windows_forecast/windows_test.parquet
    - out/<REL>/ml_forecast/X_test.npy, y_test.npy
    - out/<REL>/ml_forecast/scaler_forecast.pkl
    - out/<REL>/ml_forecast/supervised_model_xgb_forecast.pkl

It also consults:

    - out/<REL>/ml/feature_spec.json        (same spec as v1 detection)
    - out/<REL>/labels/exfil_ranges.parquet (to get S1 exfil_start)

The script reports:

1. Window-level metrics on the test split:
    - ROC AUC
    - Average Precision (AP)

2. User-level forecasting performance for Scenario 1:
    - For each exfil user in TEST:
        * Whether they were ever flagged before exfil_start
        * Earliest flagged window end_day
        * Lead time in days: (exfil_start - earliest_flagged_end_day)

    - Summary stats across exfil users in test:
        * n_exfil_users_test
        * n_detected_users
        * detection_rate
        * lead_time_days: {min, max, mean, median} for detected users

Flags are determined by a score threshold (default 0.5), configurable via:
    --threshold 0.5
"""

from __future__ import annotations

import argparse
import json
import pickle
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import duckdb
import joblib
import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score, roc_auc_score


def read_release_arg(cli_release: str | None) -> str:
    """Return the release string, preferring CLI arg over release.txt."""
    if cli_release is not None:
        return cli_release
    release_file = Path("release.txt")
    if not release_file.exists():
        raise FileNotFoundError("release.txt not found and --release not provided")
    text = release_file.read_text().strip()
    if not text:
        raise ValueError("release.txt is empty")
    return text


def load_feature_spec(path: Path) -> List[str]:
    """Load feature_spec.json and return the ordered list of feature names.

    Expected structure:
      { "features": ["feat1", "feat2", ...] }

    This spec MUST match the flattened column names used by the baseline
    detection model, so that the forecast model sees the same feature layout.
    """
    if not path.exists():
        raise FileNotFoundError(f"feature_spec.json not found at {path}")

    with path.open("r", encoding="utf-8") as f:
        spec = json.load(f)

    feats = spec.get("features")
    if not isinstance(feats, list) or not feats:
        raise ValueError("feature_spec.json must contain a non-empty 'features' list")

    return feats


def load_test_windows(
    windows_path: Path,
) -> pd.DataFrame:
    """Load the forecast test windows into a DataFrame.

    Expected columns:
        - user_key
        - end_day
        - label
        - window_json
    """
    if not windows_path.exists():
        raise FileNotFoundError(f"Missing forecast test windows at {windows_path}")

    con = duckdb.connect()
    df = con.execute(
        f"""
        SELECT
            user_key,
            end_day,
            label,
            window_json
        FROM read_parquet('{windows_path.as_posix()}')
        """
    ).df()

    if df.empty:
        raise RuntimeError(f"No rows found in {windows_path}")

    # Ensure datetime for end_day
    if not np.issubdtype(df["end_day"].dtype, np.datetime64):
        df["end_day"] = pd.to_datetime(df["end_day"])

    return df


def flatten_windows(
    df: pd.DataFrame,
    feature_cols: List[str],
) -> Tuple[np.ndarray, np.ndarray]:
    """Flatten each window's 14×F daily features into a 1D vector.

    Assumes each row has:
      - 'window_json': JSON string of the 14 daily rows
      - 'label': window-level label (0/1)

    The JSON is expected to have a 'day' column and the daily feature columns.
    We sort by 'day' to enforce consistent ordering, then stack features
    according to feature_cols.
    """
    X_rows: List[np.ndarray] = []
    y_rows: List[int] = []

    for _, row in df.iterrows():
        data = json.loads(row["window_json"])
        w = pd.DataFrame.from_records(data)

        # Ensure deterministic ordering by day if present
        if "day" in w.columns:
            w = w.sort_values("day")

        missing = [c for c in feature_cols if c not in w.columns]
        if missing:
            raise KeyError(f"Window missing expected feature columns: {missing}")

        vals = w[feature_cols].to_numpy()  # shape (14, F)
        X_rows.append(vals.reshape(-1))    # flatten to (14*F,)
        y_rows.append(int(row["label"]))

    X = np.vstack(X_rows).astype(np.float32)
    y = np.asarray(y_rows, dtype="int8")

    if X.shape[0] != y.shape[0]:
        raise RuntimeError(
            f"Mismatch between features and labels: {X.shape[0]} vs {y.shape[0]}"
        )

    return X, y


def load_model_and_scaler(
    ml_dir: Path,
) -> Tuple[Any, Any]:
    """Load the trained forecast model and scaler."""
    model_path = ml_dir / "supervised_model_xgb_forecast.pkl"
    scaler_path = ml_dir / "scaler_forecast.pkl"

    if not model_path.exists():
        raise FileNotFoundError(f"Missing forecast model at {model_path}")
    if not scaler_path.exists():
        raise FileNotFoundError(f"Missing forecast scaler at {scaler_path}")

    with model_path.open("rb") as f:
        model = pickle.load(f)

    scaler = joblib.load(scaler_path)
    return model, scaler


@dataclass
class LeadTimeRecord:
    user_key: str
    exfil_start: pd.Timestamp
    earliest_flagged_end_day: Optional[pd.Timestamp]
    lead_time_days: Optional[int]  # exfil_start - earliest_flagged_end_day
    detected: bool


def load_s1_exfil_starts(exfil_ranges_path: Path) -> pd.DataFrame:
    """Load Scenario-1 exfil_start per user (earliest start only)."""
    if not exfil_ranges_path.exists():
        raise FileNotFoundError(f"Missing exfil_ranges at {exfil_ranges_path}")

    con = duckdb.connect()
    df = con.execute(
        f"""
        SELECT
            user_key,
            MIN(exfil_start) AS exfil_start
        FROM read_parquet('{exfil_ranges_path.as_posix()}')
        WHERE scenario_id = 'SCENARIO_1'
        GROUP BY user_key
        """
    ).df()

    if df.empty:
        raise RuntimeError("No Scenario-1 exfil ranges found")

    df["exfil_start"] = pd.to_datetime(df["exfil_start"])
    return df


def compute_window_metrics(y_true: np.ndarray, y_score: np.ndarray) -> Dict[str, float]:
    """Compute window-level AUC and AP for the test split."""
    metrics: Dict[str, float] = {}
    if len(np.unique(y_true)) < 2:
        metrics["auc"] = float("nan")
        metrics["ap"] = float("nan")
        return metrics

    metrics["auc"] = float(roc_auc_score(y_true, y_score))
    metrics["ap"] = float(average_precision_score(y_true, y_score))
    return metrics


def compute_lead_times(
    df_test: pd.DataFrame,
    scores: np.ndarray,
    s1_exfil: pd.DataFrame,
    threshold: float,
) -> Dict[str, Any]:
    """Compute earliest-flag lead times per Scenario-1 exfil user in TEST.

    - A user is considered "detected" if any window with end_day <= exfil_start
      has score >= threshold.
    - Lead time is defined as (exfil_start.date - earliest_flagged_end_day.date).
    """
    if len(df_test) != len(scores):
        raise RuntimeError(
            f"Length mismatch between test windows and scores: "
            f"{len(df_test)} vs {len(scores)}"
        )

    df = df_test.copy()
    df["score"] = scores

    # Only keep users that appear both in test windows and in S1 exfil ranges.
    exfil_users = set(s1_exfil["user_key"].unique())
    test_users = set(df["user_key"].unique())
    users_in_test_exfil = sorted(exfil_users & test_users)

    records: List[LeadTimeRecord] = []

    s1_exfil_idx = s1_exfil.set_index("user_key")

    for user in users_in_test_exfil:
        exfil_start = s1_exfil_idx.loc[user, "exfil_start"]

        df_user = df[df["user_key"] == user]

        # Flagged windows before or on exfil_start
        df_flagged = df_user[
            (df_user["end_day"] <= exfil_start) & (df_user["score"] >= threshold)
        ].sort_values("end_day")

        if df_flagged.empty:
            rec = LeadTimeRecord(
                user_key=user,
                exfil_start=exfil_start,
                earliest_flagged_end_day=None,
                lead_time_days=None,
                detected=False,
            )
        else:
            first_flag = df_flagged.iloc[0]
            end_day = first_flag["end_day"]
            lead_time = (exfil_start.normalize() - end_day.normalize()).days

            rec = LeadTimeRecord(
                user_key=user,
                exfil_start=exfil_start,
                earliest_flagged_end_day=end_day,
                lead_time_days=int(lead_time),
                detected=True,
            )

        records.append(rec)

    # Aggregate stats
    n_exfil_users_test = len(records)
    detected_records = [r for r in records if r.detected]
    n_detected = len(detected_records)
    detection_rate = float(n_detected / n_exfil_users_test) if n_exfil_users_test else 0.0

    lead_times = [r.lead_time_days for r in detected_records if r.lead_time_days is not None]

    if lead_times:
        lt_arr = np.array(lead_times, dtype=float)
        lead_time_stats = {
            "min": float(lt_arr.min()),
            "max": float(lt_arr.max()),
            "mean": float(lt_arr.mean()),
            "median": float(np.median(lt_arr)),
        }
    else:
        lead_time_stats = {
            "min": None,
            "max": None,
            "mean": None,
            "median": None,
        }

    # Expand records into a serializable structure
    per_user = [
        {
            "user_key": r.user_key,
            "exfil_start": r.exfil_start.isoformat(),
            "earliest_flagged_end_day": (
                r.earliest_flagged_end_day.isoformat() if r.earliest_flagged_end_day else None
            ),
            "lead_time_days": r.lead_time_days,
            "detected": r.detected,
        }
        for r in records
    ]

    return {
        "threshold": float(threshold),
        "n_exfil_users_test": n_exfil_users_test,
        "n_detected_users": n_detected,
        "detection_rate": detection_rate,
        "lead_time_days": lead_time_stats,
        "per_user": per_user,
    }


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Evaluate supervised forecast model for Scenario 1."
    )
    parser.add_argument(
        "--release",
        type=str,
        default=None,
        help="CERT release (default: read from release.txt)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=0.5,
        help="Score threshold for marking a window as 'flagged' (default: 0.5)",
    )

    args = parser.parse_args()
    release = read_release_arg(args.release)
    threshold = args.threshold

    print({"release": release, "threshold": threshold})

    root = Path("out") / release
    ml_dir = root / "ml_forecast"
    windows_test_path = root / "windows_forecast" / "windows_test.parquet"
    feature_spec_path = root / "ml" / "feature_spec.json"
    exfil_ranges_path = root / "labels" / "exfil_ranges.parquet"

    # Load configs and artifacts
    feature_cols = load_feature_spec(feature_spec_path)
    df_test = load_test_windows(windows_test_path)
    X_test, y_test = flatten_windows(df_test, feature_cols)
    model, scaler = load_model_and_scaler(ml_dir)

    # Apply scaler from forecast preprocessing
    X_test_scaled = scaler.transform(X_test)

    # Predict scores
    if hasattr(model, "predict_proba"):
        y_score = model.predict_proba(X_test_scaled)[:, 1]
    elif hasattr(model, "decision_function"):
        # Fallback, though XGBClassifier should support predict_proba
        raw = model.decision_function(X_test_scaled)
        # Map raw scores to [0,1] with a sigmoid for comparability
        y_score = 1 / (1 + np.exp(-raw))
    else:
        raise TypeError("Model does not support predict_proba or decision_function.")

    # Window-level metrics
    window_metrics = compute_window_metrics(y_test, y_score)

    # Lead time analysis for Scenario-1 exfil users in TEST
    s1_exfil = load_s1_exfil_starts(exfil_ranges_path)
    lead_time_summary = compute_lead_times(df_test, y_score, s1_exfil, threshold)

    summary = {
        "window_metrics": window_metrics,
        "lead_time": lead_time_summary,
    }

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
