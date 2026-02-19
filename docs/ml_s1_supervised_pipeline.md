# Scenario-1 Supervised Model (r5.2, 14-day windows)

This doc explains how the **preprocessing, training, validation, and testing** flow works
for the Scenario-1 supervised model.

All paths here assume:

- CERT release: **r5.2**
- Scenario answers folder: `answers/r5.2-1`
- Features table: `out/r5.2/features_v2/daily_user/daily_user.parquet`
- Scripts in `src/ml/detect_s1/`

---

## 1. Label building

### 1.1 Exfil ranges per user

Script: `src/ml/detect_s1/build_exfil_ranges.py`

Command:

```bash
python -m src.ml.detect_s1.build_exfil_ranges
```

What it does:

- Reads all CSVs from `answers/r5.2-1/*.csv`
- For each file:
  - Extracts column 3 (timestamp) from each line.
  - Parses to a `datetime` using `%m/%d/%Y %H:%M:%S`.
  - Takes `min(timestamp)` as `exfil_start`, `max(timestamp)` as `exfil_end`.
- Writes one row per user to:

  - `out/r5.2/labels/exfil_ranges.parquet` with:
    - `user_key`
    - `scenario_id = "SCENARIO_1"`
    - `exfil_start` (Timestamp)
    - `exfil_end` (Timestamp)

### 1.2 Daily labels

Script: `src/ml/detect_s1/build_daily_labels.py`

Command:

```bash
python -m src.ml.detect_s1.build_daily_labels
```

What it does:

- Loads daily features:
  - `out/r5.2/features_v2/daily_user/daily_user.parquet`
- Loads exfil ranges:
  - `out/r5.2/labels/exfil_ranges.parquet`
- Adds a new column `label` to daily_user, initialized to 0.
- For each `(user_key, exfil_start, exfil_end)`:
  - For that user’s rows, sets `label = 1` when:
    - `day >= exfil_start` and `day <= exfil_end`
- Writes:

  - `out/r5.2/labels/daily_labels.parquet`

So **label=1 means “this calendar day is inside the exfil window for that user.”**

---

## 2. Train / val / test split (by user)

Script: `src/ml/detect_s1/make_train_val_test.py`

Command:

```bash
python -m src.ml.detect_s1.make_train_val_test
```

What it does:

- Loads `out/r5.2/labels/daily_labels.parquet`
- Gets the unique `user_key` list.
- Shuffles users with a **fixed RNG seed (42)** for reproducibility.
- Splits users:

  - 70% → `train`
  - 15% → `val`
  - 15% → `test`

- Assigns a `split` column to each daily row based on its `user_key`.
- Writes:

  - `out/r5.2/ml_splits/daily_labels_splits.parquet`

Key point:

> A user appears in **exactly one** of train / val / test.  
> No user leakage across splits.

---

## 3. Sliding windows (14-day context)

Script: `src/ml/detect_s1/make_windows.py`

Command:

```bash
python -m src.ml.detect_s1.make_windows
```

Config:

- `WINDOW = 14` days (fixed by design)

What it does:

- Loads `out/r5.2/ml_splits/daily_labels_splits.parquet`
- For each split (`train`, `val`, `test`):
  - Filters to that split.
  - Sorts by `user_key, day`.
  - For each user:
    - Walks over their time series and, for every position `i >= 13`:
      - Takes the 14-day slice `[i-13 .. i]`.
      - Builds one window row:

        - `user_key`
        - `end_day` = last day in the window
        - `label` = label of `end_day` (0/1)
        - `window_json` = JSON list of the 14 daily rows

- Writes three Parquets:

  - `out/r5.2/windows/windows_train.parquet`
  - `out/r5.2/windows/windows_val.parquet`
  - `out/r5.2/windows/windows_test.parquet`

So at this point:

> Each row = one 14-day window ending on `end_day`, labeled by whether that last day is in the exfil range.

---

## 4. Feature selection & flattening

### 4.1 Feature spec

File: `out/r5.2/ml/feature_spec.json`

Structure:

```json
{
  "features": [
    "logon_after_hours_rate",
    "logon_on_shared_pc_rate",
    ...
    "usb_novel"
  ]
}
```

This list tells the preprocessing code **which daily columns** to keep and in what order.

### 4.2 Flattening windows → ML matrices

Script: `src/ml/detect_s1/preprocess.py`

Command:

```bash
python -m src.ml.detect_s1.preprocess
```

What it does:

1. Loads feature spec:
   - `out/r5.2/ml/feature_spec.json` → `feature_cols`
2. Loads window Parquets:
   - train/val/test from `out/r5.2/windows/`
3. For each split, runs `flatten_windows(df, feature_cols)`:

   - For each row:
     - Parses `window_json` into a small DataFrame.
     - Sorts by `day` to enforce consistent order.
     - Selects `feature_cols` (e.g., `ah_rate_1d`, `usb_count_1d`, etc.).
     - Gets a `14 × F` numeric matrix.
     - Reshapes into a 1D vector of length `14*F`.
   - Stacks all rows into:
     - `X_split`: shape `(n_windows, 14*len(feature_cols))`
     - `y_split`: shape `(n_windows,)`, from the window’s `label`.

4. Fits a `StandardScaler` **on `X_train` only**.
5. Transforms `X_train`, `X_val`, `X_test` using that scaler.
6. Saves outputs under `out/r5.2/ml/`:

   - `X_train.npy`, `X_val.npy`, `X_test.npy`
   - `y_train.npy`, `y_val.npy`, `y_test.npy`
   - `scaler.pkl`

End result:

> Ready-to-train NumPy matrices with consistent scaling, no leakage of val/test into the scaler.

---

## 5. Training / validation / test

Script: `src/ml/detect_s1/train_supervised.py`

Command:

```bash
python -m src.ml.detect_s1.train_supervised
```

What it does:

1. Loads preprocessed arrays:

   - `X_train.npy`, `X_val.npy`, `X_test.npy`
   - `y_train.npy`, `y_val.npy`, `y_test.npy`

2. Creates an `XGBClassifier` with:

   - `n_estimators=300`
   - `max_depth=4`
   - `learning_rate=0.05`
   - `subsample=0.8`
   - `colsample_bytree=0.8`
   - `objective="binary:logistic"`
   - `eval_metric="logloss"`
   - `tree_method="hist"`
   - `n_jobs=4`

3. Trains on **train set only**:

   - \`model.fit(X_train, y_train, eval_set=[(X_val, y_val)], verbose=True)\`
   - The validation set is used to monitor performance during training (for tuning / sanity).

4. After training, evaluates on:

   - Validation:
     - `val_probs = model.predict_proba(X_val)[:, 1]`
   - Test:
     - `test_probs = model.predict_proba(X_test)[:, 1]`

   Using:
   - ROC AUC (`roc_auc_score`)
   - Average Precision (`average_precision_score`)

5. Prints:

   - Validation AUC / AP
   - Test AUC / AP

6. Saves model:

   - `out/r5.2/ml/supervised_model_xgb.pkl`

---

## 6. Test-only evaluation summary

Script: `src/ml/detect_s1/eval_supervised.py`

Command:

```bash
python -m src.ml.detect_s1.eval_supervised
```

What it does:

- Loads `X_test.npy`, `y_test.npy`, and the trained model.
- Computes predicted probabilities on test only.
- Prints:

  - Test AUC
  - Test Average Precision
  - Classification report at threshold `0.5` for:
    - Precision
    - Recall
    - F1
    - Support per class

This is **purely test-only**; no train/val contamination.

---

## 7. Repro pipeline (one-shot summary)

From a clean state with r5.2 features built:

```bash
# 1) Build labels from answer CSVs
python -m src.ml.detect_s1.build_exfil_ranges
python -m src.ml.detect_s1.build_daily_labels

# 2) Split by user into train/val/test
python -m src.ml.detect_s1.make_train_val_test

# 3) Build 14-day windows
python -m src.ml.detect_s1.make_windows

# 4) Flatten & scale features
python -m src.ml.detect_s1.preprocess

# 5) Train model (uses val for monitoring; reports val+test metrics)
python -m src.ml.detect_s1.train_supervised

# 6) Test-only metrics (sanity check)
python -m src.ml.detect_s1.eval_supervised
```
