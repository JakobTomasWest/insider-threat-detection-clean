"""
Preprocess forecast windows into model-ready NumPy arrays.

This script reads the forecast windows:

    out/<REL>/windows_forecast/windows_{train,val,test}.parquet

Each of those files is expected to contain:

    - user_key
    - end_day
    - label          (0/1 forecast label)
    - window_json    (JSON-encoded fixed-length numeric vector)

It then:

    - Parses window_json into dense feature matrices X_*
    - Collects labels into y_*
    - Fits a StandardScaler on X_train only
    - Applies the scaler to train/val/test
    - Writes arrays and scaler into:

        out/<REL>/ml_forecast/X_{train,val,test}.npy
        out/<REL>/ml_forecast/y_{train,val,test}.npy
        out/<REL>/ml_forecast/scaler_forecast.pkl

This does NOT touch the baseline detection artifacts under out/<REL>/ml.
"""

import argparse
import json
from pathlib import Path

import duckdb
import joblib
import numpy as np
from sklearn.preprocessing import StandardScaler
import pandas as pd


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


def load_feature_spec(path: Path) -> list[str]:
    """Load feature_spec.json and return the ordered list of feature names.

    Expected structure:
      { "features": ["feat1", "feat2", ...] }

    This spec MUST match the flattened column names produced for the
    baseline detection model so that the forecast model sees the same
    feature layout.
    """
    if not path.exists():
        raise FileNotFoundError(f"feature_spec.json not found at {path}")

    with path.open("r", encoding="utf-8") as f:
        spec = json.load(f)

    feats = spec.get("features")
    if not isinstance(feats, list) or not feats:
        raise ValueError("feature_spec.json must contain a non-empty 'features' list")

    return feats


def load_split_windows(root: Path, split: str, feature_cols: list[str]) -> tuple[np.ndarray, np.ndarray]:
    """
    Load forecast windows for a given split and return (X, y):

        - X: shape (N, F_flat) float32, where F_flat = 14 * len(feature_cols)
        - y: shape (N,) int8

    Uses the same feature_spec.json as the baseline detection model to
    ensure feature alignment.
    """
    in_path = root / "windows_forecast" / f"windows_{split}.parquet"
    if not in_path.exists():
        raise FileNotFoundError(f"Missing forecast windows at {in_path}")

    con = duckdb.connect()
    df = con.execute(
        f"""
        SELECT
            label,
            window_json
        FROM read_parquet('{in_path.as_posix()}')
        """
    ).df()

    if df.empty:
        raise RuntimeError(f"No rows found in {in_path}")

    X_rows: list[np.ndarray] = []
    y_rows: list[int] = []

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
            f"Mismatch between features and labels for split {split}: "
            f"{X.shape[0]} vs {y.shape[0]}"
        )

    return X, y


def preprocess_forecast(release: str) -> None:
    root = Path("out") / release
    out_dir = root / "ml_forecast"
    out_dir.mkdir(parents=True, exist_ok=True)

    # Load feature spec from the baseline detection model so we reuse
    # the exact same per-day feature layout.
    feature_spec_path = root / "ml" / "feature_spec.json"
    feature_cols = load_feature_spec(feature_spec_path)

    # Load raw feature matrices and labels
    X_train, y_train = load_split_windows(root, "train", feature_cols)
    X_val, y_val = load_split_windows(root, "val", feature_cols)
    X_test, y_test = load_split_windows(root, "test", feature_cols)

    # Fit scaler on train only
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)

    # Save arrays
    np.save(out_dir / "X_train.npy", X_train_scaled)
    np.save(out_dir / "y_train.npy", y_train)

    np.save(out_dir / "X_val.npy", X_val_scaled)
    np.save(out_dir / "y_val.npy", y_val)

    np.save(out_dir / "X_test.npy", X_test_scaled)
    np.save(out_dir / "y_test.npy", y_test)

    # Save scaler
    joblib.dump(scaler, out_dir / "scaler_forecast.pkl")

    summary = {
        "release": release,
        "out_dir": str(out_dir),
        "train": {
            "n_windows": int(X_train.shape[0]),
            "n_features": int(X_train.shape[1]),
            "n_pos": int(y_train.sum()),
        },
        "val": {
            "n_windows": int(X_val.shape[0]),
            "n_features": int(X_val.shape[1]),
            "n_pos": int(y_val.sum()),
        },
        "test": {
            "n_windows": int(X_test.shape[0]),
            "n_features": int(X_test.shape[1]),
            "n_pos": int(y_test.sum()),
        },
    }
    print(summary)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Preprocess forecast windows into NumPy arrays."
    )
    parser.add_argument(
        "--release",
        type=str,
        default=None,
        help="CERT release (default: read from release.txt)",
    )

    args = parser.parse_args()
    release = read_release_arg(args.release)

    print({"release": release})
    preprocess_forecast(release)


if __name__ == "__main__":
    main()
