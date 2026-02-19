# Scenario-1 Supervised Detector (r5.2, 14-day  Windows) – Results

This document summarizes how well the **Scenario-1 supervised model** performs
on CERT r5.2 using 14-day windows and the "during-exfil" label definition.

It is meant as a reference for the capstone report (metrics, class balance, and
high-level interpretation).

---

## 1. Setup recap

- **CERT release:** r5.2
- **Scenario subset:** r5.2-1 (Scenario 1 malicious users only)
- **Features table:** \`out/r5.2/features_v2/daily_user/daily_user.parquet\`
- **Exfil ranges:** built from \`answers/r5.2-1/*.csv\` into  
  \`out/r5.2/labels/exfil_ranges.parquet\`
- **Daily labels:**
  - \`label = 1\` if \`day \in [exfil_start, exfil_end]\` for that user
  - \`label = 0\` otherwise
  - Saved to \`out/r5.2/labels/daily_labels.parquet\`
- **Train/val/test split:** by **user**, 70/15/15 %, seed = 42,  
  saved to \`out/r5.2/ml_splits/daily_labels_splits.parquet\`
- **Windows:**
  - 14-day sliding windows, stride 1 (fixed)
  - Window label = label of **last day** in the 14-day slice
  - Saved to:
    - \`out/r5.2/windows/windows_train.parquet\`
    - \`out/r5.2/windows/windows_val.parquet\`
    - \`out/r5.2/windows/windows_test.parquet\`

- **Feature spec:** \`out/r5.2/ml/feature_spec.json\`
  - USB/removable activity
  - After-hours rates
  - Wikileaks/Dropbox/Job-site counts
  - Novelty and baseline/ trend features for after-hours and USB

- **Preprocessing:** (src/ml/detect_s1/preprocess.py)
  - Flatten each 14×F window into a 1D vector (length = 14 * |features|)
  - Fit **StandardScaler on X_train only**
  - Apply same scaler to val/test
  - Save:
    - \`X_train.npy\`, \`X_val.npy\`, \`X_test.npy\`
    - \`y_train.npy\`, \`y_val.npy\`, \`y_test.npy\`
    - \`scaler.pkl\`

- **Model:** (src/ml/detect_s1/train_supervised.py)
  - Algorithm: XGBoost (\`XGBClassifier\`)
  - Params:
    - \`n_estimators=300\`
    - \`max_depth=4\`
    - \`learning_rate=0.05\`
    - \`subsample=0.8\`
    - \`colsample_bytree=0.8\`
    - \`objective="binary:logistic"\`
    - \`eval_metric="logloss"\`
    - \`tree_method="hist"\`
    - \`n_jobs=4\`
    - \`scale_pos_weight=None\` (no explicit reweighting)
  - Trained on **train** only, monitored on **val**.
  - Model artifact: \`out/r5.2/ml/supervised_model_xgb.pkl\`.

---

## 2. Class balance (window level)

Windows built from \`out/r5.2/windows/*.parquet\`:

| split | windows  | positives (label=1) | positive rate |
|-------|----------|---------------------|---------------|
| train | 467,812  | 78                  | ~0.017%       |
| val   | 99,225   | 48                  | ~0.048%       |
| test  | 100,612  | 22                  | ~0.022%       |

Notes:

- Very **severe imbalance**: positives are on the order of **1–5 per 10,000** windows.
- This is expected: only 29 malicious users in r5.2-1, and exfil intervals are short.

---

## 3. Aggregate metrics (ROC AUC, Average Precision)

### 3.1 Validation / test metrics from training script

From \`python -m src.ml.detect_s1.train_supervised\`:

- **Validation AUC:** 0.9966  
- **Test AUC:**       1.0000  
- **Validation AP:**  0.7307  
- **Test AP:**        0.9703  

Interpretation:

- **AUC ≈ 1.0** means the model can **perfectly rank** exfil vs non-exfil windows:
  - For a random positive and negative window, the positive always receives a higher score.
- **Average Precision (AP)** is high despite heavy imbalance:
  - On test, AP ≈ 0.97 indicates that the top-ranked windows are almost all truly exfil windows.
- These numbers reflect the fact that:
  - Labels mark days **during** the exfil interval.
  - Features include strong, scenario-specific signals
    (e.g., removable writes, Wikileaks/Dropbox counts, USB anomalies).

This is a **Scenario-1-only detection problem with strong features**, not
a generic “mild anomaly detection” setup.

---

## 4. Threshold-based performance (test set, window level)

From \`python -m src.ml.eval_supervised\` using a **0.5** threshold:

- **Test AUC:** 1.0000  
- **Test AP:**  0.9703  

Classification report (test, threshold = 0.5):

- **Class 0 (non-exfil windows):**
  - Precision ≈ 0.9999
  - Recall ≈ 1.0000
  - F1 ≈ 1.0000
  - Support: 100,590

- **Class 1 (exfil windows):**
  - Precision = 1.0000
  - Recall ≈ 0.6364
  - F1 ≈ 0.7778
  - Support: 22

Confusion matrix (window level, test, threshold = 0.5):

- True Positives (TP): 14
- False Negatives (FN): 8
- False Positives (FP): 0
- True Negatives (TN): 100,590

Resulting overall accuracy on test:

- Accuracy ≈ (TP + TN) / total = (14 + 100,590) / 100,612 ≈ 0.9999

Interpretation:

- At threshold 0.5, the model is **extremely conservative**:
  - It raises **no false alarms** (FP = 0).
  - It catches only the **higher-signal** exfil windows (TP = 14).
  - It misses lower-signal exfil windows (FN = 8).
- This is consistent with:
  - High AUC/AP (excellent ranking capability).
  - Imperfect recall at a fixed threshold.

In other words:

> The model is very good at ranking risk, but a strict 0.5 threshold
> trades some recall for perfect precision on exfil windows.

---

## 5. User-level detection (test split)

Looking only at **users who have exfil windows in TEST**:

- Test exfil users: \`alt1465\`, \`jup1472\`, \`vah1292\`
- For each user, we check if **any** of their windows has \`prob >= 0.5\`.

Result (threshold = 0.5):

- \`alt1465\`: at least one window flagged
- \`jup1472\`: at least one window flagged
- \`vah1292\`: at least one window flagged
- **Users with exfil but no window ≥ 0.5:** none

So, at the **user level** on the test split:

- Exfil users in test: 3
- Users flagged at least once: 3
- User-level recall (exfil users): **1.0**
- User-level false positives at this threshold: effectively **0**  
  (no non-exfil window crosses 0.5, so no benign user is “flagged” if we define flagging
  as “any window ≥ 0.5 for that user”).

Interpretation:

> Although the model does not catch every exfil **window**, it does flag
> every exfil **user** at least once during their exfil period at the
> default 0.5 threshold.

This is a useful property for an analyst-facing detector: every test-set
Scenario-1 insider would appear in the alert stream at least once.

---

## 6. Why results are so strong (and why that’s OK for now)

The very strong metrics (AUC ~1.0, AP ~0.97) are explained by:

1. **Scenario-1-only labeling**  
   - Only classic Scenario-1 insiders (after-hours, USB, Wikileaks/Dropbox).
   - No mixture of different scenario types in this model.

2. **“During-exfil” label definition**  
   - Positive windows **include** the days when exfil is actively happening.
   - Exfil behavior (removable writes, Wikileaks hits, USB spikes) appears
     directly inside many labeled windows.

3. **Feature design**  
   - Features are intentionally aligned with the Scenario-1 behavior:
     - \`file_n_to_removable\`, \`file_n_from_removable\`
     - \`http_n_wikileaks\`, \`http_n_dropbox\`
     - USB counts and novelty
     - After-hours rates and trends

4. **Clean split by user**  
   - No user appears in more than one split.
   - Malicious patterns do not leak between train/val/test.

Taken together, this makes the problem **much easier** than a realistic,
multi-scenario, early-warning insider-threat task. The model is not "magical";
the scenario and features are strong and tightly aligned.

---

## 7. How to describe this model in the capstone report

Suggested summary:

> We trained a supervised XGBoost model on r5.2 Scenario 1 using 14-day sliding
> windows and labels defined over the “during-exfil” interval. The model uses
> 1D flattened windows of behavioral features focused on after-hours activity,
> USB/removable usage, and Scenario-1-specific web destinations (Wikileaks and
> personal cloud storage).
>
> On the held-out test split (users not seen during training), the model
> achieves an AUC of 1.00 and an average precision of 0.97, indicating nearly
> perfect separation between exfil and non-exfil windows. At a default
> probability threshold of 0.5, it maintains perfect precision on exfil windows
> (no false positives) while recalling approximately 64% of exfil windows. At
> the user level, every exfiltrating user in the test set is flagged at least
> once, making the model practically useful as a high-precision detector for
> Scenario 1 insiders.
>
> These strong metrics are expected given that (1) the labels are defined
> during the exfil interval, and (2) the feature set includes direct
> indicators of Scenario-1 behavior. In later experiments, we plan to use
> more challenging label definitions (e.g., forecasting before exfil starts)
> and reduced feature sets to better approximate a realistic early-warning
> insider-threat detector.
