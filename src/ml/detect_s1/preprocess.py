from pathlib import Path
import json
import pickle

import numpy as np
import pandas as pd
from tqdm import tqdm

try:
    from sklearn.preprocessing import StandardScaler
except ImportError as e:
    StandardScaler = None


REL = "r5.2"

# Windows are expected to be split beforehand into train/val/test
WINDOWS_DIR = Path(f"out/{REL}/windows")
WINDOWS_TRAIN = WINDOWS_DIR / "windows_train.parquet"
WINDOWS_VAL = WINDOWS_DIR / "windows_val.parquet"
WINDOWS_TEST = WINDOWS_DIR / "windows_test.parquet"

# Feature spec describing which flattened columns to keep
ML_DIR = Path(f"out/{REL}/ml")
FEATURE_SPEC_PATH = ML_DIR / "feature_spec.json"
ML_DIR.mkdir(parents=True, exist_ok=True)

# Outputs: preprocessed matrices + scaler
X_TRAIN_PATH = ML_DIR / "X_train.npy"
X_VAL_PATH = ML_DIR / "X_val.npy"
X_TEST_PATH = ML_DIR / "X_test.npy"
Y_TRAIN_PATH = ML_DIR / "y_train.npy"
Y_VAL_PATH = ML_DIR / "y_val.npy"
Y_TEST_PATH = ML_DIR / "y_test.npy"
SCALER_PATH = ML_DIR / "scaler.pkl"


def load_feature_spec(path: Path) -> list:
    """Load feature_spec.json and return the ordered list of feature names.

    Expected structure:
      { "features": ["feat1", "feat2", ...] }

    This spec MUST match the flattened column names produced in STEP 7.
    """
    if not path.exists():
        raise FileNotFoundError(f"feature_spec.json not found at {path}")

    with path.open("r", encoding="utf-8") as f:
        spec = json.load(f)

    feats = spec.get("features")
    if not isinstance(feats, list) or not feats:
        raise ValueError("feature_spec.json must contain a non-empty 'features' list")

    return feats


def flatten_windows(df: pd.DataFrame, feature_cols: list) -> tuple[np.ndarray, np.ndarray]:
    """Flatten each window's 14×F daily features into a 1D vector.

    Assumes each row has:
      - 'window_json': JSON string of the 14 daily rows
      - 'label': window-level label (0/1)

    The JSON is expected to have a 'day' column and the daily feature columns.
    We sort by 'day' to enforce consistent ordering, then stack features according
    to feature_cols.
    """
    X_rows = []
    y_rows = []

    for _, row in tqdm(df.iterrows(), total=len(df), desc="flatten", ncols=80):
        data = json.loads(row["window_json"])
        w = pd.DataFrame.from_records(data)

        # Ensure deterministic ordering by day if present
        if "day" in w.columns:
            w = w.sort_values("day")

        # Select only the numeric daily features we care about
        missing = [c for c in feature_cols if c not in w.columns]
        if missing:
            raise KeyError(f"Window missing expected feature columns: {missing}")

        vals = w[feature_cols].to_numpy()
        X_rows.append(vals.reshape(-1))  # 14×F -> (14*F,)
        y_rows.append(row["label"])

    X = np.vstack(X_rows).astype(np.float32)
    y = np.array(y_rows, dtype=np.int64)
    return X, y


def main():
    if StandardScaler is None:
        raise ImportError(
            "scikit-learn is required for preprocessing. Install with 'pip install scikit-learn'."
        )

    print("Loading feature spec...")
    feature_cols = load_feature_spec(FEATURE_SPEC_PATH)

    print("Loading windows (train/val/test)...")
    df_train = pd.read_parquet(WINDOWS_TRAIN)
    df_val = pd.read_parquet(WINDOWS_VAL)
    df_test = pd.read_parquet(WINDOWS_TEST)

    print("Flattening train windows...")
    X_train, y_train = flatten_windows(df_train, feature_cols)

    print("Flattening val windows...")
    X_val, y_val = flatten_windows(df_val, feature_cols)

    print("Flattening test windows...")
    X_test, y_test = flatten_windows(df_test, feature_cols)

    print("Fitting scaler on X_train only...")
    scaler = StandardScaler()
    scaler.fit(X_train)  # fit on train only, per ml_assistant_context.md

    print("Transforming val/test with train-fitted scaler...")
    X_train_scaled = scaler.transform(X_train)
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)

    print("Saving matrices and scaler...")
    np.save(X_TRAIN_PATH, X_train_scaled)
    np.save(X_VAL_PATH, X_val_scaled)
    np.save(X_TEST_PATH, X_test_scaled)
    np.save(Y_TRAIN_PATH, y_train)
    np.save(Y_VAL_PATH, y_val)
    np.save(Y_TEST_PATH, y_test)

    with SCALER_PATH.open("wb") as f:
        pickle.dump(scaler, f)

    print("Preprocessing complete.")
    print(f"X_train: {X_train_scaled.shape}, positives={y_train.sum()}")
    print(f"X_val:   {X_val_scaled.shape}, positives={y_val.sum()}")
    print(f"X_test:  {X_test_scaled.shape}, positives={y_test.sum()}")


if __name__ == "__main__":
    main()
