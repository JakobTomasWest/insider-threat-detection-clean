# Scenario-1 Forecast Model (r5.2, 14‑day Windows, 7‑day Horizon) – Results

This document summarizes the **Scenario‑1 forecasting model** trained on CERT r5.2.
It parallels the structure of `m1_s1_detection_results.md` but reflects the
**forecasting** objective: predict exfil **before** it begins.

The forecasting pipeline is completely separate from the v1 “during‑exfil”
detection stack and uses its own:
- daily labels (`labels_forecast`)
- windows (`windows_forecast`)
- preprocessed matrices (`ml_forecast`)
- model (`supervised_model_xgb_forecast.pkl`)
- evaluation script (`eval_supervised_forecast.py`)

---

## 1. Setup Recap

- **CERT release:** r5.2
- **Scenario:** 1 (USB → Dropbox/Wikileaks)
- **Forecasting horizon:** **7 days before exfil_start**
- **Features table:** `out/r5.2/features_v2/daily_user/daily_user.parquet`
- **Exfil ranges:** `out/r5.2/labels/exfil_ranges.parquet`

### 1.1 Daily labels (forecast version)
Built by `src/ml/forecast_s1/build_daily_labels_forecast.py`:
- `forecast_label = 1` if a day falls in `[exfil_start − 7, exfil_start − 1]`.
- `forecast_label = 0` otherwise.
- Saved to: `out/r5.2/labels_forecast/daily_labels_forecast.parquet`.

This produces **very few** positive days (144 in r5.2), since only “pre‑exfil”
intervals are labeled.

### 1.2 Train/Val/Test split
Same split as the v1 detection model (to make comparisons meaningful):
- Split by **user** into 70/15/15.
- Stored in: `out/r5.2/ml_splits/daily_labels_splits.parquet`.

### 1.3 Windows (forecast version)
Built by `src/ml/forecast_s1/make_windows_forecast.py`:
- 14‑day sliding windows ending on each day.
- Window label = forecast_label of **end_day**.
- Must satisfy `min_history_days ≥ 14`.
- Each window stores the flattened 14×F feature block in `window_json`.
- Saved under:
  - `out/r5.2/windows_forecast/windows_train.parquet`
  - `out/r5.2/windows_forecast/windows_val.parquet`
  - `out/r5.2/windows_forecast/windows_test.parquet`

### 1.4 Feature spec (shared with v1 detector)
`feature_spec.json` is reused from:
- `out/r5.2/ml/feature_spec.json`

So the forecast model sees **identical per‑day feature layout** as the v1
supervised detector.

### 1.5 Preprocessing (forecast version)
Performed by `src/ml/forecast_s1/preprocess_forecast.py`:
- Parse each `window_json` into a 14×F DataFrame.
- Sort by day, extract columns from `feature_spec.json`.
- Flatten into a 1D vector of length `14 * F` (F = 336 in r5.2).
- Fit **StandardScaler on train only**, reuse for val/test.
- Save to:
  - `out/r5.2/ml_forecast/X_*.npy`
  - `out/r5.2/ml_forecast/y_*.npy`
  - `out/r5.2/ml_forecast/scaler_forecast.pkl`

### 1.6 Model
Trained by `src/ml/forecast_s1/train_supervised_forecast.py`:
- **Model:** `XGBClassifier`
- **Key parameters:**
  - `n_estimators = 300`
  - `max_depth = 4`
  - `learning_rate = 0.05`
  - `subsample = 0.8`
  - `colsample_bytree = 0.8`
  - `objective = "binary:logistic"`
  - `eval_metric = "aucpr"`
  - `tree_method = "hist"`
  - `scale_pos_weight = neg/pos ≈ 5501.75` (computed from train labels)
- Saved to: `out/r5.2/ml_forecast/supervised_model_xgb_forecast.pkl`

---

## 2. Class Balance (Window Level)
From forecast windows:

| split | windows | positives | positive rate |
|-------|----------|-----------|----------------|
| train | 467,734 | 85 | ~0.018% |
| val   | 99,177  | 45 | ~0.045% |
| test  | 100,590 | 14 | ~0.014% |

The forecasting task is **more imbalanced** than the detection task because
only short “pre‑exfil” periods are labeled.

---

## 3. Aggregate Metrics (Window Level, Test)
From `eval_supervised_forecast.py`:

- **AUC:** 0.8280
- **AP:** 0.1862

Interpretation:
- For a random positive and negative forecast window, the model ranks the
  positive higher **83%** of the time.
- AP ≈ 0.19 is far above the random baseline (~0.00014), showing meaningful
  precision–recall structure despite extreme imbalance.
- These values reflect a **much harder task** than “during‑exfil detection,”
  where the signal is immediate.

---

## 4. User‑Level Forecasting: Lead Time Analysis
From `eval_supervised_forecast.py --threshold 0.5`:

- Test Scenario‑1 exfil users: **3**
- Users detected before exfil_start: **2**
- User‑level detection rate: **66.7%**
- Lead times (days before exfil_start):
  - **min:** 1
  - **max:** 9
  - **mean:** 5
  - **median:** 5

### Per‑User Summary
| user_key | exfil_start | earliest flag | lead time | detected |
|----------|-------------|----------------|-----------|----------|
| alt1465  | 2010‑08‑13  | 2010‑08‑04     | 9 days    | yes |
| jup1472  | 2011‑02‑11  | 2011‑02‑10     | 1 day     | yes |
| vah1292  | 2011‑04‑29  | none           | —         | no |

Interpretation:
- The model provides **meaningful early warning** for two out of three test
  Scenario‑1 insiders, with lead times ranging from **1 to 9 days**.
- The third user shows no pre‑exfil activity detectable at threshold 0.5.

---

## 5. Comparison to the Detection Model (v1)
| Aspect | Detection (v1) | Forecast (v2) |
|--------|----------------|----------------|
| Labels | During exfil | 7‑day pre‑exfil |
| Positives | more | fewer |
| AUC (test) | ~1.00 | ~0.83 |
| AP (test) | ~0.97 | ~0.19 |
| User‑level | all 3 detected | 2 of 3 detected |
| Lead time | none (during) | 1–9 days early |

The forecasting model performs **much worse numerically** (by design) because
its task is substantially harder: no direct exfil behavior appears inside the
window.

Yet it still reveals actionable signal: certain users exhibit noticeable
behavioral shifts days before exfil begins.

---

## 6. Suggested Capstone Summary
> We trained a forecasting‑focused XGBoost model on CERT r5.2 Scenario 1 using
> 14‑day windows that end before the attacker’s first exfil day. Labels marked
> windows whose last day falls in the 7‑day pre‑exfil horizon. Using the same
> per‑day feature layout as the detection model, we trained a forecast‑specific
> pipeline and evaluated it against a user‑held‑out test split.
>
> The model achieves AUC ≈ 0.83 and AP ≈ 0.19 at the window level, reflecting a
> meaningful ability to rank windows by risk despite extreme class imbalance.
> At the user level, it detects 2 of the 3 Scenario‑1 insiders in the test
> split **before** exfil begins, providing between 1 and 9 days of lead time.
>
> The forecasting task is substantially more difficult than detection and
does not benefit from direct exfil artifacts. The results indicate that
> certain behavioral precursors to Scenario‑1 activity are detectable in
> advance, supporting the feasibility of early‑warning models in later stages of
> the capstone.
