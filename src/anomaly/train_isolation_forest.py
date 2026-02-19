"""
Step 2: Train Isolation Forest

Purpose:
  Train an Isolation Forest model on normal user behavior.
  This model learns global patterns of what "normal" looks like.

What it does:
  1. Load windows_train.parquet
  2. Extract malicious user list from answers/insiders.csv
  3. Filter out ALL malicious users (train only on normal users)
  4. Extract features from 14-day windows
  5. Apply preprocessing:
     - Temporal aggregation (mean, max, std over 14 days)
     - Remove low-variance features
     - Standard scaling
  6. Train Isolation Forest
  7. Save model and all preprocessing artifacts

Output:
  out/r5.2/anomaly/isolation_forest.pkl
  out/r5.2/anomaly/feature_scaler.pkl
  out/r5.2/anomaly/score_scaler.pkl
  out/r5.2/anomaly/variance_selector.pkl
  out/r5.2/anomaly/feature_config.json

Usage:
  python -m src.anomaly.train_isolation_forest
"""

from pathlib import Path
import json
import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.feature_selection import VarianceThreshold
import joblib
from tqdm import tqdm

# Read release tag
REL = Path("release.txt").read_text().strip()

# Inputs
WINDOWS_TRAIN = Path(f"out/{REL}/windows/windows_train.parquet")
INSIDERS_CSV = Path("answers/insiders.csv")

# Output directory
OUT_DIR = Path(f"out/{REL}/anomaly")
OUT_DIR.mkdir(parents=True, exist_ok=True)


def load_malicious_users():
    """Load list of r5.2 malicious users only from insiders.csv"""
    df = pd.read_csv(INSIDERS_CSV)
    
    # Filter to ONLY r5.2 scenarios (dataset = 5.2)
    r52_df = df[df['dataset'].astype(str) == '5.2']
    
    # Get unique malicious users (lowercase for consistency)
    malicious = set(r52_df['user'].str.lower().unique())
    
    print(f"Loaded {len(malicious)} r5.2 malicious users from {INSIDERS_CSV}")
    return malicious


def load_windows(parquet_path):
    """Load windows parquet file."""
    df = pd.read_parquet(parquet_path)
    print(f"Loaded {len(df)} windows from {parquet_path}")
    print(f"  Unique users: {df['user_key'].nunique()}")
    return df


def extract_features_from_windows(windows_df):
    """
    Extract features from window JSON.
    
    Each row has:
      - user_key
      - end_day
      - label
      - window_json: JSON string of 14 daily rows
    """
    feature_vectors = []
    users = []
    end_days = []
    labels = []
    
    for _, row in tqdm(windows_df.iterrows(), total=len(windows_df), desc="Extracting features"):
        try:
            # Parse window JSON
            window_data = json.loads(row['window_json'])
            
            # Convert to DataFrame
            window_df = pd.DataFrame(window_data)
            
            # Sort by day
            if 'day' in window_df.columns:
                window_df = window_df.sort_values('day')
            
            # Remove non-feature columns
            feature_cols = [c for c in window_df.columns if c not in ['day', 'user_key', 'label', 'split', 'train', 'val', 'test']]
            
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
            
            feature_vectors.append(aggregated)
            users.append(row['user_key'])
            end_days.append(row['end_day'])
            labels.append(row['label'])
            
        except Exception as e:
            print(f"Error processing window for {row.get('user_key')}: {e}")
            continue
    
    return np.array(feature_vectors), users, end_days, labels, feature_cols


def main():
    """Train Isolation Forest on normal users."""
    
    # Check inputs
    if not WINDOWS_TRAIN.exists():
        raise FileNotFoundError(f"Training windows not found at {WINDOWS_TRAIN}")
    if not INSIDERS_CSV.exists():
        raise FileNotFoundError(f"Insiders list not found at {INSIDERS_CSV}")
    
    print("="*60)
    print("STEP 1: Load Data")
    print("="*60)
    
    # Load malicious users
    malicious_users = load_malicious_users()
    
    # Load training windows
    windows_train = load_windows(WINDOWS_TRAIN)
    
    print("\n" + "="*60)
    print("STEP 2: Extract Features")
    print("="*60)
    
    # Extract features
    X_train, users_train, days_train, labels_train, feature_names = extract_features_from_windows(windows_train)
    
    print(f"\nExtracted features shape: {X_train.shape}")
    print(f"  Features per window: {X_train.shape[1]}")
    print(f"  (mean, max, std over 14 days × {len(feature_names)} raw features)")
    
    print("\n" + "="*60)
    print("STEP 3: Filter to Normal Users Only")
    print("="*60)
    
    # Filter out malicious users
    normal_mask = np.array([u.lower() not in malicious_users for u in users_train])
    X_train_normal = X_train[normal_mask]
    
    print(f"Total windows: {len(X_train)}")
    print(f"Malicious user windows removed: {(~normal_mask).sum()}")
    print(f"Normal user windows: {len(X_train_normal)}")
    
    print("\n" + "="*60)
    print("STEP 4: Preprocessing")
    print("="*60)
    
    # Remove low-variance features
    print("\n4a. Removing low-variance features...")
    variance_selector = VarianceThreshold(threshold=0.01)
    X_selected = variance_selector.fit_transform(X_train_normal)
    print(f"  Features after variance selection: {X_selected.shape[1]} (removed {X_train_normal.shape[1] - X_selected.shape[1]})")
    
    # Standard scaling
    print("\n4b. Applying standard scaling...")
    feature_scaler = StandardScaler()
    X_scaled = feature_scaler.fit_transform(X_selected)
    print(f"  Scaled to mean=0, std=1")
    
    print("\n" + "="*60)
    print("STEP 5: Train Isolation Forest")
    print("="*60)
    
    # Train Isolation Forest
    print("\nTraining Isolation Forest...")
    print(f"  Note: Training on NORMAL users only (malicious already filtered out)")
    print(f"  Setting contamination='auto' since we filtered malicious users")
    
    iso_forest = IsolationForest(
        n_estimators=200,
        max_samples='auto',
        contamination='auto',  # Let sklearn determine, since we filtered malicious users
        random_state=42,
        n_jobs=-1,
        verbose=1
    )
    
    iso_forest.fit(X_scaled)
    print("✅ Training complete!")
    
    # Get training scores for normalization
    print("\nGenerating training scores for normalization...")
    train_scores_raw = -iso_forest.decision_function(X_scaled)  # Negative for higher = more anomalous
    
    # Normalize scores to [0, 1]
    score_scaler = MinMaxScaler()
    score_scaler.fit(train_scores_raw.reshape(-1, 1))
    
    print(f"  Score range: [{train_scores_raw.min():.3f}, {train_scores_raw.max():.3f}]")
    
    print("\n" + "="*60)
    print("STEP 6: Save Artifacts")
    print("="*60)
    
    # Save model
    joblib.dump(iso_forest, OUT_DIR / "isolation_forest.pkl")
    print(f" Saved model: {OUT_DIR / 'isolation_forest.pkl'}")
    
    # Save scalers
    joblib.dump(feature_scaler, OUT_DIR / "feature_scaler.pkl")
    print(f" Saved feature scaler: {OUT_DIR / 'feature_scaler.pkl'}")
    
    joblib.dump(score_scaler, OUT_DIR / "score_scaler.pkl")
    print(f" Saved score scaler: {OUT_DIR / 'score_scaler.pkl'}")
    
    # Save variance selector
    joblib.dump(variance_selector, OUT_DIR / "variance_selector.pkl")
    print(f" Saved variance selector: {OUT_DIR / 'variance_selector.pkl'}")
    
    # Save feature configuration
    feature_config = {
        "features": feature_names,
        "num_features": len(feature_names),
        "num_days": 14,
        "relevant_indices": variance_selector.get_support().tolist()
    }
    
    with open(OUT_DIR / "feature_config.json", 'w') as f:
        json.dump(feature_config, f, indent=2)
    print(f"Saved feature config: {OUT_DIR / 'feature_config.json'}")
    
    print("\n" + "="*60)
    print("TRAINING COMPLETE!")
    print("="*60)
    print(f"\nModel artifacts saved to: {OUT_DIR}")
    print("\nNext steps:")
    print("  1. python -m src.anomaly.build_window_scores")
    print("  2. python -m src.anomaly.compute_baselines")
    print("  3. python -m src.anomaly.tune_threshold")


if __name__ == "__main__":
    main()
