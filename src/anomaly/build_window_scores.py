"""
Step 1: Build Window Scores

Purpose:
  Generate Isolation Forest anomaly scores for ALL historical 14-day windows.
  This creates the dataset needed to compute per-user baselines.

What it does:
  1. Loads daily_user.parquet (all user activity)
  2. Loads user_roles.parquet (for peer grouping)
  3. For each user, creates 14-day sliding windows 
  4. Uses trained IF model
  5. Saves all scores to window_scores.parquet

Output:
  out/r5.2/anomaly/window_scores.parquet
  
Columns:
  - user_key: lowercase user ID
  - end_day: last day of the 14-day window
  - base_score: Isolation Forest anomaly score (0-1, higher = more anomalous)
  - role_id: user's job role

Usage:
  python -m src.anomaly.build_window_scores
"""

from pathlib import Path
from collections import defaultdict, deque
import duckdb
import pandas as pd
import numpy as np
import json
import joblib
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

# Read release tag
REL = Path("release.txt").read_text().strip()

# Inputs
DAILY = Path(f"out/{REL}/features_v2/daily_user/daily_user.parquet")
ROLES = Path(f"out/{REL}/anomaly/user_roles.parquet")
MODEL_DIR = Path(f"out/{REL}/anomaly")

# Output
OUT = MODEL_DIR / "window_scores.parquet"


def extract_features_from_window(window_df, feature_names):
    """
    Extract features from a 14-day window DataFrame.
    """
    # Get feature columns only
    feature_cols = [c for c in window_df.columns if c in feature_names]
    
    # Extract feature matrix (14 days × n_features)
    X = window_df[feature_cols].values.astype(np.float32)
    
    # Replace NaN with 0
    X = np.nan_to_num(X, nan=0.0)
    
    # Aggregate over 14 days: mean, max, std
    mean_features = np.mean(X, axis=0)
    max_features = np.max(X, axis=0)
    std_features = np.std(X, axis=0)
    
    # Concatenate
    aggregated = np.concatenate([mean_features, max_features, std_features])
    
    return aggregated.reshape(1, -1)


def main():
    """Generate IF scores for all historical windows."""
    
    # Check inputs
    if not DAILY.exists():
        raise FileNotFoundError(f"Daily features not found at {DAILY}")
    if not ROLES.exists():
        raise FileNotFoundError(
            f"User roles not found at {ROLES}. "
            f"Run: python -m src.anomaly.build_user_roles"
        )
    
    # Load trained artifacts (suppress joblib verbose output)
    print("Loading trained Isolation Forest model...")
    import os
    os.environ['JOBLIB_VERBOSE'] = '0'
    
    model = joblib.load(MODEL_DIR / "isolation_forest.pkl")
    feature_scaler = joblib.load(MODEL_DIR / "feature_scaler.pkl")
    score_scaler = joblib.load(MODEL_DIR / "score_scaler.pkl")
    variance_selector = joblib.load(MODEL_DIR / "variance_selector.pkl")
    
    with open(MODEL_DIR / "feature_config.json", "r") as f:
        feature_config = json.load(f)
    
    feature_names = feature_config["features"]
    print(f"✅ Model loaded from: {MODEL_DIR}")
    print(f"Using {len(feature_names)} features")
    
    # Load user roles
    print(f"\nLoading user roles from: {ROLES}")
    roles_df = pd.read_parquet(ROLES)
    user_roles = dict(zip(roles_df["user_key"].str.lower(), roles_df["role_id"]))
    
    # Load daily features using DuckDB
    print(f"Loading daily features from: {DAILY}")
    con = duckdb.connect(database=":memory:")
    
    # Get all data, sorted by user and day
    query = f"""
        SELECT 
            lower(user_key) AS user_key,
            day,
            *
        FROM read_parquet('{DAILY}')
        ORDER BY user_key, day
    """
    df = con.execute(query).df()
    print(f"Loaded {len(df)} daily records for {df['user_key'].nunique()} users")
    
    # Build 14-day sliding windows per user (matching run_loop.py logic)
    print("\nGenerating 14-day sliding windows...")
    windows_dict = defaultdict(lambda: deque(maxlen=14))
    records = []
    
    for _, row in tqdm(df.iterrows(), total=len(df), desc="Processing days"):
        user = row["user_key"]
        day_str = str(row["day"])[:10]  # YYYY-MM-DD
        
        # Build features dict (exclude meta columns)
        features = row.drop(["user_key", "day"]).to_dict()
        
        # Append to user's rolling window (same as run_loop.py)
        windows_dict[user].append({"day": day_str, "features": features})
        
        # Score window if we have 14 days
        if len(windows_dict[user]) == 14:
            window_json = list(windows_dict[user])
            
            try:
                # Convert window to DataFrame (same format as training)
                window_rows = []
                for entry in window_json:
                    row_data = entry["features"].copy()
                    row_data["day"] = entry["day"]
                    window_rows.append(row_data)
                window_df = pd.DataFrame(window_rows)
                
                # Extract features (mean/max/std over 14 days)
                X = extract_features_from_window(window_df, feature_names)
                
                # Apply preprocessing pipeline (same as training)
                X_selected = variance_selector.transform(X)
                X_scaled = feature_scaler.transform(X_selected)
                
                # Get raw IF score
                raw_score = -model.decision_function(X_scaled)[0]
                
                # Scale to [0, 1]
                base_score = float(score_scaler.transform([[raw_score]])[0, 0])
                
                # Get role
                role_id = user_roles.get(user)
                
                records.append({
                    "user_key": user,
                    "end_day": day_str,
                    "base_score": base_score,
                    "role_id": role_id,
                })
            except Exception as e:
                # Debug: Print first few errors
                if len(records) == 0:
                    print(f"\n⚠️ Error scoring window for {user} on {day_str}: {e}")
                pass
    
    # Save results
    print(f"\nGenerated {len(records)} window scores")
    
    if len(records) == 0:
        print(" No windows generated! Check that daily_user.parquet has valid data.")
        return
    
    result_df = pd.DataFrame(records)
    result_df.to_parquet(OUT, index=False)
    
    print(f" Wrote window scores to: {OUT}")
    
    # Show summary statistics
    print("\n=== Summary Statistics ===")
    print(f"Total windows: {len(result_df)}")
    print(f"Unique users: {result_df['user_key'].nunique()}")
    print(f"Date range: {result_df['end_day'].min()} to {result_df['end_day'].max()}")
    print(f"\nScore distribution:")
    print(result_df['base_score'].describe())
    print(f"\nWindows per user:")
    print(result_df.groupby('user_key').size().describe())


if __name__ == "__main__":
    main()
