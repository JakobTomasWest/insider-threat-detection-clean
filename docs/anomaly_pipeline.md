# Anomaly Detection Pipeline

The `src/anomaly/` folder contains the training and baseline computation pipeline for the Isolation Forest anomaly detector.

## Pipeline Overview

The anomaly detection system uses an Isolation Forest trained on normal user behavior to identify unusual activity patterns. It operates in 4 steps:

### 1. Train Isolation Forest (`train_isolation_forest.py`)

Trains the core anomaly detection model:
- Loads 14-day windows from `windows_train.parquet`
- Filters out known malicious users (trains only on normal behavior)
- Aggregates features: mean, max, std over 14 days (53 features → 159 features)
- Applies variance selection to remove low-variance features
- Applies standard scaling
- Trains Isolation Forest (200 estimators)
- Saves artifacts: `isolation_forest.pkl`, `feature_scaler.pkl`, `variance_selector.pkl`, `score_scaler.pkl`, `feature_config.json`

**Usage:**
```bash
python -m src.anomaly.train_isolation_forest
```

### 2. Score Windows (`build_window_scores.py`)

Generates anomaly scores for all user windows:
- Loads trained IF model
- Scores all windows (train + val + test sets)
- Outputs: `window_scores.parquet` (~680k windows with base IF scores)

**Usage:**
```bash
python -m src.anomaly.build_window_scores
```

### 3. Build User Roles (`build_user_roles.py`)

Extracts organizational structure for baseline grouping:
- Reads LDAP data to build user→role, user→business_unit, user→department mappings
- Outputs: `user_roles.parquet` (user organizational metadata)

**Usage:**
```bash
python -m src.anomaly.build_user_roles
```

### 4. Compute Baselines (`compute_baselines.py`)

Calculates z-scores for score interpretation:
- For each window, computes how unusual the score is compared to:
  - **Personal baseline**: user's own historical scores
  - **Role baseline**: peers in same role/department/team
  - **Organizational baseline**: business unit, functional unit
- Uses strict temporal windowing (only past data, no same-day leakage)
- Outputs: `window_zscores.parquet` (667k windows with z_personal, z_role, z_max, etc.)

**Usage:**
```bash
python -m src.anomaly.compute_baselines
```

## Output Artifacts

### Model Artifacts (`out/r5.2/anomaly/`)
- `isolation_forest.pkl` - Trained IF model (200 trees)
- `feature_scaler.pkl` - StandardScaler for selected features
- `variance_selector.pkl` - VarianceThreshold selector
- `score_scaler.pkl` - MinMaxScaler for normalizing scores to [0,1]
- `feature_config.json` - Feature names and selection mask

### Data Artifacts
- `window_scores.parquet` - Raw IF scores for all windows
- `window_zscores.parquet` - Scores + z-score baselines (used by UI)
- `user_roles.parquet` - User organizational metadata

## Integration with Runtime Detector

The online detector in `src/detector/anomaly.py` uses these artifacts:
1. Loads model artifacts at startup
2. Receives 14-day windows from `run_loop.py`
3. Aggregates features (mean, max, std)
4. Applies variance selection + scaling
5. Scores with IF model
6. Returns raw score (0-1) with evidence: `{base_score, window_days, window_end}`


## Helper Scripts

- `analyze_window_scores.py` - Analyzes `window_scores.parquet` (raw IF scores): distribution stats, high-scoring windows, insider analysis
- `build_user_org_structure.py` - Extended org structure builder with team/functional unit mappings
