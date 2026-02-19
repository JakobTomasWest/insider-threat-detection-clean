
# Anomaly Detector Performance Evaluation  
Isolation Forest + Z-Score Hybrid (Scenario 1, r5.2)

This document summarizes the performance of the anomaly detector used in the CS 6019 insider-threat pipeline.  
The detector combines:

- Isolation Forest base anomaly score  
- Per-user z-score normalization  
- Peer-group z-score normalization  
- Threshold-based alerting within `run_loop.py`

Evaluation uses:
- CERT r5.2  
- Scenario 1 insider labels from `answers/insiders.csv`  
- Daily window z-score features  
- Real anomaly alerts emitted by the pipeline

---

## 1. Detection Performance on Scenario 1 Insiders

### Exfil-Window Detection
The anomaly detector is evaluated on the exact exfiltration window (start → end) for each insider.

- **True Positives (TP):** 116  
- **False Negatives (FN):** 32  
- **Recall:** 78.4%

For an unsupervised detector operating on sliding windows, recall near 0.8 is strong.

---

## 2. Early-Warning Performance

A key goal of the project is not only detecting exfiltration while it happens, but *anticipating* it.

Early-warning positives (EWP) are alerts occurring in the interval:
```
exfil_start − K days  →  exfil_start − 1
```
(K = 30 in this evaluation)

### Early Warning Results
- **EWP alerts:** 42  
- **Insiders receiving early-warning signal:** 14  
- **Median lead time:** 2 days  
- **Maximum lead time:** 14 days  

This demonstrates that the anomaly detector provides actionable predictive signal, not just reactive alerts.

---

## 3. Precision Metrics

Two precision metrics are reported:

### Strict Precision
Counts only in-window TP:
```
Precision_strict = TP / (TP + FP)
= 0.462
```

### Precision Including Early Warning
Counts early-warning positives as useful signal:
```
Precision_with_EWP = (TP + EWP) / (TP + EWP + FP)
= 0.539
```

The second metric better reflects operational usefulness in an analyst setting.

---

## 4. False-Positive Behavior on Non-Insiders

False positives are evaluated across all employees *not* listed as Scenario 1 insiders.

### Summary Statistics
- **Total non-insiders evaluated:** 1,971  
- **Users with ≥1 anomaly alert:** 340 (17.25%)  
- **Median false-positive rate:** 0.000  
- **Mean false-positive rate:** 0.052  
- **75th percentile FP rate:** 0.000  
- **Noise concentration:** The top ~1% of users account for most FP activity.

### Interpretation
Most employees (75%) never trigger an anomaly alert at all.  
A small minority of chronically irregular users generate ongoing signal, which is expected behavior for anomaly detectors.

---

## 5. Overall Conclusions

1. **High recall** on true exfiltration windows.  
2. **Reliable early warning**, with up to **14 days** of lead time.  
3. **Useful precision** when early-warning positives are counted.  
4. **Low noise** across the general employee population.  
5. **Noise localized** to a small set of behavioral outliers.  

This anomaly detector is a strong contributor in a multi-prong ensemble system:  
- It stays quiet on normal users  
- It becomes active around real threats  
- It often signals *before* the threat fully materializes

This behavior is ideal for the intended use case.

---

## 6. Artifact Reproducibility

All results come from:
- `out/<REL>/anomaly/window_zscores.parquet`  
- `out/<REL>/features_v2/daily_user/daily_user.parquet`  
- `out/mvp0/alerts_ndjson/alerts.ndjson`  
- Scenario 1 labels from `answers/insiders.csv`  
- Evaluation notebook: `notebooks/anomaly_inspector_s1.ipynb`

Any future re-training or re-tuning should re-run this notebook to ensure consistency.
