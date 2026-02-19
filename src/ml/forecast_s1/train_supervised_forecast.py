

"""
Train a supervised forecast model for Scenario 1.

This script trains a classifier to predict **future** exfiltration for S1
using the forecast windows and preprocessed matrices built by:

    - src/ml/forecast_s1/build_daily_labels_forecast.py
    - src/ml/forecast_s1/make_windows_forecast.py
    - src/ml/forecast_s1/preprocess_forecast.py

It is deliberately separate from the v1 detection stack and only reads
from `out/<REL>/ml_forecast/`. The frozen detection model and its
artifacts under `out/<REL>/ml/` are not touched.

Inputs (under out/<REL>/ml_forecast/):
    - X_train.npy, y_train.npy
    - X_val.npy,   y_val.npy
    - X_test.npy,  y_test.npy
    - scaler_forecast.pkl  (not used here, but kept for run_loop usage)

Outputs (also under out/<REL>/ml_forecast/):
    - supervised_model_xgb_forecast.pkl
    - A small JSON-like summary printed to stdout with train/val metrics.

We keep the interface simple: the primary control is the release (via
--release or release.txt) and an optional random seed.
"""

from __future__ import annotations

import argparse
import json
import pickle
from pathlib import Path
from typing import Any, Dict

import numpy as np

from sklearn.metrics import average_precision_score, roc_auc_score
from sklearn.model_selection import StratifiedKFold

try:
    from xgboost import XGBClassifier
except ImportError as e:
    XGBClassifier = None  # type: ignore[misc]


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


def load_matrices(root: Path) -> Dict[str, np.ndarray]:
    """Load preprocessed forecast matrices from out/<REL>/ml_forecast/."""
    ml_dir = root / "ml_forecast"

    def _load(name: str) -> np.ndarray:
        path = ml_dir / name
        if not path.exists():
            raise FileNotFoundError(f"Missing matrix at {path}. "
                                    "Run preprocess_forecast.py first.")
        return np.load(path)

    X_train = _load("X_train.npy")
    y_train = _load("y_train.npy")
    X_val = _load("X_val.npy")
    y_val = _load("y_val.npy")
    X_test = _load("X_test.npy")
    y_test = _load("y_test.npy")

    return {
        "X_train": X_train,
        "y_train": y_train,
        "X_val": X_val,
        "y_val": y_val,
        "X_test": X_test,
        "y_test": y_test,
        "ml_dir": ml_dir,
    }


def build_model(random_state: int) -> Any:
    """Construct the XGBClassifier with conservative defaults.

    We mirror a typical tabular configuration: small-ish trees, limited depth,
    and scale_pos_weight to help with the extreme imbalance.
    """
    if XGBClassifier is None:
        raise ImportError(
            "xgboost is required for training the forecast model. "
            "Install it with 'pip install xgboost'."
        )

    # NOTE: These are reasonable defaults for a first pass. If you later decide
    # to tune hyperparameters, do it in this file only, without touching the
    # v1 detection stack.
    model = XGBClassifier(
        n_estimators=300,
        max_depth=4,
        learning_rate=0.05,
        subsample=0.8,
        colsample_bytree=0.8,
        objective="binary:logistic",
        eval_metric="aucpr",
        tree_method="hist",
        random_state=random_state,
        n_jobs=4,
    )
    return model


def compute_class_weight(y: np.ndarray) -> float:
    """Compute scale_pos_weight for XGBoost given a binary label vector."""
    # Avoid division by zero if somehow all labels are one class.
    pos = int((y == 1).sum())
    neg = int((y == 0).sum())
    if pos == 0 or neg == 0:
        return 1.0
    return neg / max(pos, 1)


def eval_split(y_true: np.ndarray, y_score: np.ndarray) -> Dict[str, float]:
    """Compute AUC and Average Precision for a single split."""
    # Guard against degenerate cases (all one class) where metrics may fail.
    if len(np.unique(y_true)) < 2:
        return {"auc": float("nan"), "ap": float("nan")}

    auc = roc_auc_score(y_true, y_score)
    ap = average_precision_score(y_true, y_score)
    return {"auc": float(auc), "ap": float(ap)}


def train_forecast_model(
    X_train: np.ndarray,
    y_train: np.ndarray,
    X_val: np.ndarray,
    y_val: np.ndarray,
    ml_dir: Path,
    random_state: int,
) -> Dict[str, Any]:
    """Train the forecast model and save it to ml_forecast dir.

    Returns a small summary dict with train/val metrics and paths.
    """
    ml_dir.mkdir(parents=True, exist_ok=True)

    # Build model and set imbalance weight.
    model = build_model(random_state=random_state)
    scale_pos_weight = compute_class_weight(y_train)
    model.set_params(scale_pos_weight=scale_pos_weight)

    # We use the explicit val split for early stopping.
    eval_set = [(X_train, y_train), (X_val, y_val)]

    model.fit(
        X_train,
        y_train,
        eval_set=eval_set,
        verbose=False,
    )

    # Evaluate on train and val using the best iteration.
    train_scores = model.predict_proba(X_train)[:, 1]
    val_scores = model.predict_proba(X_val)[:, 1]

    train_metrics = eval_split(y_train, train_scores)
    val_metrics = eval_split(y_val, val_scores)

    # Persist the model alongside the forecast matrices.
    model_path = ml_dir / "supervised_model_xgb_forecast.pkl"
    with model_path.open("wb") as f:
        pickle.dump(model, f)

    summary: Dict[str, Any] = {
        "model_path": str(model_path),
        "scale_pos_weight": float(scale_pos_weight),
        "train": {
            "n": int(len(y_train)),
            "pos": int((y_train == 1).sum()),
            "auc": train_metrics["auc"],
            "ap": train_metrics["ap"],
        },
        "val": {
            "n": int(len(y_val)),
            "pos": int((y_val == 1).sum()),
            "auc": val_metrics["auc"],
            "ap": val_metrics["ap"],
        },
    }

    return summary


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Train supervised forecast model for Scenario 1."
    )
    parser.add_argument(
        "--release",
        type=str,
        default=None,
        help="CERT release (default: read from release.txt)",
    )
    parser.add_argument(
        "--random-state",
        type=int,
        default=42,
        help="Random seed for model initialization (default: 42)",
    )

    args = parser.parse_args()
    release = read_release_arg(args.release)
    random_state = args.random_state

    print({"release": release, "random_state": random_state})

    root = Path("out") / release
    matrices = load_matrices(root)

    summary = train_forecast_model(
        X_train=matrices["X_train"],
        y_train=matrices["y_train"],
        X_val=matrices["X_val"],
        y_val=matrices["y_val"],
        ml_dir=matrices["ml_dir"],
        random_state=random_state,
    )

    # Pretty-print JSON-like summary for logs.
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()