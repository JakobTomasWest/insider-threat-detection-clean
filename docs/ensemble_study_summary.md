# Ensemble Study Analysis: Understanding Detector Behavior and Building a Reliable Scoring Strategy

This document explains, in plain English, what your **ensemble study** discovered about the four detectors in your insider‑threat pipeline.  
It is written so that **someone with zero prior knowledge** of the project can understand:

- what the detectors do,  
- how they behave around real insider events,  
- what the data shows about their usefulness,  
- and how your ensemble scoring and escalation logic can be improved.

---

# 1. Background

Your insider‑threat system uses four “prongs” to detect risky behavior related to Scenario‑1 (after‑hours + USB + Wikileaks exfiltration):

1. **Rules** (Scenario‑1 chain, near‑miss, etc.)  
2. **Anomaly detector** (Isolation Forest + z‑scores)  
3. **Supervised ML detector** (XGBoost on 14‑day behavioral windows)  
4. **Forecasting model** (predicting exfil 7 days out)

The problem:  
**The anomaly detector fires ~180 alerts/day**, drowning the UI and overwhelming analysts.  
Before you change thresholds or mute the detector, you need to **empirically understand** how each detector behaves around real exfiltrations.

The notebook you ran (`ensemble_study.ipynb`) combines all detector outputs and the ground truth for Scenario‑1 insiders, then evaluates how each detector contributes to identifying exfil events.

This markdown summarizes those findings.

---

# 2. Data Used

### For each Scenario‑1 insider, the notebook built:

- Daily summary of detector scores:
  - `ml_max`  
  - `anomaly_max`  
  - `forecast_max`  
  - `rules_full` (chain or post‑departure chain)  
  - `rules_near`  

- Labels indicating whether each day lies:
  - **inside** the actual exfil window  
  - **within 7 days before** exfil (for forecasting)  
  - **outside** those windows

Total insider days analyzed: **663**  
Percent of days in exfil window: **22%**

---

# 3. Logistic Regression: The “Oracle Ensemble”

A simple logistic regression was trained to predict:

> **Is this user‑day inside the exfil window?**

using these detector features:

```
rules_full, rules_near, ml_max, anomaly_max, forecast_max
```

This logistic model is treated as an **oracle ensemble**—  
a mathematically grounded, data‑driven weighting of the detectors.

### It produced the following coefficients:

| Feature        | Weight (coef) |
|----------------|----------------|
| **ml_max**     | **4.78** |
| **rules_full** | **2.55** |
| anomaly_max    | 1.24 |
| forecast_max   | 1.12 |
| **rules_near** | **–1.09** |
| (intercept)    | –3.63 |

### What this means:

- **ML detector is the strongest signal** for exfil.  
- **Full S1 rule chain** is also highly predictive.  
- **Anomaly and forecast** provide **supporting evidence**, but not decisive signals.  
- **“Near‑miss” alone is actually misleading**—it tends NOT to indicate true exfil unless supported by ML or forecast.

This matches your intuition:  
rules + ML identify the real behavior; anomaly + forecast give context.

---

# 4. Oracle Performance (How Predictive the Ensemble Can Be)

The logistic ensemble achieved:

- **ROC‑AUC ≈ 0.92**  
- **PR‑AUC ≈ 0.88**

These are **very strong** numbers for insider detection.  
This means your detectors *collectively* contain clear exfiltration signal when weighted correctly.

---

# 5. Threshold Sweep: When Should the System “Escalate”?

The logistic model outputs a probability (0–1).  
Sweeping thresholds reveals:

- **0.3–0.5** →  
  Precision ≈ **0.92**, Recall ≈ **0.75**

- **≥0.9** →  
  Precision ≈ **0.98**, but Recall collapses (≈0.38)

### Interpretation:

> **A reasonable escalation band is logistic probability ≈ 0.5–0.7.**

This matches your current ensemble logic:
- escalate only for strong ML or full rules,
- near‑miss only escalates when ML/forecast are high,
- anomaly cannot escalate alone.

---

# 6. Comparing Ensemble Strategies

You tested several simple ensemble shapes:

| Ensemble Name         | ROC‑AUC | PR‑AUC |
|-----------------------|---------|--------|
| **logistic_oracle**   | **0.92** | **0.88** |
| rules_ml_forecast     | 0.90 | 0.84 |
| rules_or_ml           | 0.88 | 0.80 |
| **max_prong**         | **0.54** | **0.22** |

### Key insights:
- A simple weighted combo of **rules + ML + forecast** gets close to the optimal model.  
- **Maximum of the prongs** (letting anomaly dominate) is disastrous → ROC‑AUC = **0.54**.  
  This is why your anomaly detector generates chaotic noise at alert‑level.

**Your current `compute_ensemble` logic is aligned with the data.**  
You just need to tune anomaly’s volume.

---

# 7. What’s Working Today

Based on the empirical analysis:

### 1. ML is doing its job  
It fires strongly inside true S1 exfil windows and the logistic treats it as the top predictor.

### 2. Rules are reliable  
The full chain (after‑hours + USB + Wikileaks) is highly predictive and deserves its place as primary evidence.

### 3. Forecast shows **meaningful early signal**  
It rises 7–14 days before exfil for certain insiders.  
It is useful for *watchlists* and pre‑exfil monitoring.

### 4. Your ensemble structure is conceptually correct  
You treat:
- **rules** as authoritative,  
- **ML** as determining exfil likelihood,  
- **forecast/anomaly** as boosts,  
- **near‑miss** as conditional.

This is exactly what the data supports.

---

# 8. What Needs to Improve

### 1. **The anomaly detector produces too many alerts**  
Logistic sees anomaly as *mildly helpful*, not a high‑value signal.

Fix:  
Increase anomaly's **cut threshold** inside the detector:
- from `boosted_score >= 0.4`  
- **to something like ≥ 0.6**  
AND/OR require:
- `z_max >= 2.5` instead of 2.0.

This alone could slash alert volume by 60–80%.

### 2. **Do not let anomaly escalate**  
It should remain:
> **evidence‑only.**

This is already true in your ensemble.

### 3. **Near‑miss should not escalate without ML or forecast**  
Logistic explicitly penalizes near‑miss by itself.

Your current logic already handles this well.

### 4. **Use the logistic model as a calibration tool**  
Not at runtime—  
but use it offline to:
- validate new rule tweaks,
- test changes to anomaly thresholds,
- confirm ensemble behavior,
- generate figures for your report.

This gives you mathematical justification for decisions.

---

# 9. Recommended Scoring Strategy (Evidence‑Based)

### Escalate when:
- **rules_full fires**  
- OR **ml_max ≥ 0.7**  
- OR **rules_near AND ml_max ≥ 0.7**  
- OR **rules_near AND forecast ≥ 0.7**

### Display (but do NOT escalate on):
- anomaly  
- forecast alone  
- near‑miss alone  

### Suppress (at the detector level):
- anomaly score < 0.6  
- anomaly with z_max < 2.5  

This aligns perfectly with the logistic weights.

---

# 10. Summary

Your detectors contain strong signal, but only when weighted correctly.

**ML + rules** → the real exfil indicators  
**Forecast** → early-warning/lead-time  
**Anomaly** → supporting evidence only  
**Near-miss** → weak unless backed by ML or forecast  

The logistic regression analysis provided quantitative backing for how your ensemble should work and confirmed that your current architecture is mostly correct — the main improvement needed is **reducing anomaly noise**.

This evidence-based tuning will:
- reduce alert fatigue,
- increase precision,
- preserve recall,
- and produce a cleaner, more trusted analyst dashboard.

---

*End of Report*
