## Anomaly Threshold Selection and Rationale

The Isolation Forest–based anomaly detector produces a continuous score in \[0,1\], where higher values indicate more unusual 14-day behavioral windows. Because the anomaly detector is not scenario-aware, its output naturally contains substantial “background weirdness” from normal employees. A global threshold is therefore required to reduce alert volume before anomaly signals are incorporated into the ensemble.

We evaluated candidate anomaly thresholds by sweeping a range of decision cutoffs (0.30–0.90) and computing, for each threshold:

- **alerts_kept** – total anomaly alerts surviving the threshold  
- **s1_users_hit** – number of Scenario-1 insiders who receive *at least one* anomaly alert  
- **s1_user_recall** – fraction of all Scenario-1 insiders covered  
- **s1_alerts** – total number of anomaly alerts belonging to S1 insiders  
- **user_precision** – among all users hit above threshold, fraction that are S1  
- **non_s1_per_s1_alert** – number of non-S1 alerts per S1 alert (noise level)  
- **mean_z_max / median_z_max** – strength of z-score signals among surviving windows  

Using the full year of r5.2 alerts (14-day windows), we observed the following pattern:

| threshold | alerts_kept | s1_users_hit | s1_user_recall | s1_alerts | non_s1_per_s1_alert |
|----------:|------------:|-------------:|----------------:|----------:|---------------------:|
| 0.40      | 72,321      | 29           | 1.0             | 585       | 122.6               |
| 0.75      | 12,637      | 29           | 1.0             | 107       | 117.1               |

Two results are central:

1. Raising the threshold from **0.40 → 0.75** reduces alert volume by **~83%** (72k → 12.6k).  
2. **S1 user recall remains 100% at both thresholds.** Every insider still receives at least one anomaly alert above 0.75.

This indicates that Scenario-1 insiders produce relatively strong anomaly spikes (boosted scores ≥ 0.40 on their weakest days), while the majority of non-insider anomalies occupy the interval below ~0.70. The bottom half of the anomaly score distribution is therefore disproportionately noise, and the upper tail contains the signals that matter.

The small change in noise ratio between 0.40 (122.6:1) and 0.75 (117.1:1) reflects the heavy-tailed nature of the CERT dataset: anomaly detection is inherently noisy, but higher thresholds preferentially retain windows with stronger z-score evidence (mean z\_max increases from 2.82 → 3.21). This yields a substantially smaller and more interpretable anomaly set without sacrificing insider coverage.

### Final Threshold Decision

We selected **0.75** as the anomaly decision threshold because it:

- preserves **100%** insider recall for Scenario-1  
- reduces anomaly alert volume by **more than 80%**  
- retains windows with stronger behavioral deviations (higher z\_max)  
- minimizes analyst overload while keeping anomaly evidence available in the UI  

Operationally, anomaly is designed to serve as *supporting* evidence in the multi-prong system, not as a primary detector. A high threshold aligns the anomaly layer with this intended role: contribute meaningful behavioral deviations without overwhelming analysts or drowning out the rule-based and supervised detectors.