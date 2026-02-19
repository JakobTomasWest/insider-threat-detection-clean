# 6019 Ensemble & Meta Layer Roadmap
_Reference guide for integrating the final ensemble + per-user meta state into the insider-threat pipeline._

This document exists to keep future work (and future chats) aligned with the **final direction** for fusion, risk scoring, and analyst-facing context.


---

## 1. High-Level Goals

The ensemble + meta layer must:

1. **Fuse detectors in an empirically grounded way**
   - Use the **logistic regression ensemble** trained on prong scores as the canonical fusion model.
   - Detectors: `rules_full`, `rules_near`, `ml_max`, `anomaly_max`, `forecast_max`.
   - No additional learned classifier on top of this ensemble.

2. **Provide stable, interpretable severity / escalation**
   - Map fused risk into a small set of **severity buckets**: `critical`, `high`, `medium`, `low`.
   - Ensure buckets are defensible and consistent over time.

3. **Maintain per-user meta state across days**
   - Track trends and streaks (e.g., sustained high forecast, repeated partial S1 patterns).
   - Use meta for **context**, tie-breaking, and analyst notes, not as a new classifier.

4. **Support analyst triage and investigation**
   - Provide a sortable **priority score** within buckets.
   - Build **analyst notes** from detector outputs + meta to explain *why* a user is high-risk.

5. **Be simple to implement and debug**
   - Deterministic logic in the fusion layer.
   - Logistic ensemble parameters are frozen and documented.
   - No “ensemble-on-ensemble” complexity.

---

## 2. Architectural Overview

Final data flow for each `(day, user_key)`:

1. **Window construction**
   - Build a 14-day causal window from `features_v2/daily_user`.
   - Same window semantics as existing detectors.

2. **Run detectors**
   - `rules.check(window_df)` → `rules_full`, `rules_near`, evidence.
   - `ml.check(window_df)` → `ml_max` probability (from frozen S1 model).
   - `forecast.check(window_df)` → `forecast_max` probability (7-day exfil risk).
   - `anomaly.check(window_df)` → `anomaly_max` score and any flags.

3. **Load previous meta state**
   - Retrieve `meta_prev = user_meta[user_key]` (or default if first time).

4. **Ensemble + meta fusion**
   - Compute **logistic ensemble probability** `p_ensemble` using frozen coefficients.
   - Update **per-user meta** based on today’s detector outputs and `meta_prev`.
   - Decide **severity bucket** using detectors + logistic + meta (rules first, then logistic thresholds, then meta at the edges).
   - Compute **priority_score** for sorting within bucket.
   - Generate **analyst_notes** summarizing key drivers and trends.

5. **Emit alert row**
   - Write an alert record with:
     - day, user_key
     - raw detector outputs
     - logistic probability
     - severity bucket
     - priority_score
     - meta snapshot (or selected fields)
     - analyst_notes

6. **Persist updated meta state**
   - Update `user_meta[user_key] = meta_now` in memory.
   - Optionally checkpoint to disk periodically for long runs.

This preserves existing detector implementations and replaces the previous “bucket-risk fusion” logic with a cleaner, empirically validated scheme.

---

## 3. Logistic Ensemble Specification

### 3.1 Inputs

Per `(day, user_key)` the ensemble takes:

- `rules_full`     (binary or score)
- `rules_near`     (binary or score)
- `ml_max`         (max ML detector probability over window)
- `anomaly_max`    (max anomaly score over window)
- `forecast_max`   (max forecast probability over window)

These should match the definitions used in `ensemble_study.ipynb` / `ensemble_study_summary.md`.

### 3.2 Model form

The ensemble is a **logistic regression** trained offline:

```text
logit = w0
      + w_rules_full   * rules_full
      + w_rules_near   * rules_near
      + w_ml_max       * ml_max
      + w_anomaly_max  * anomaly_max
      + w_forecast_max * forecast_max

p_ensemble = sigmoid(logit)
```

Key properties:

- `ml_max` has the strongest positive coefficient.
- `rules_full` is the next strongest.
- `anomaly_max` and `forecast_max` provide supportive lift.
- `rules_near` is weak or mildly negative alone and is only useful in combination.

### 3.3 Frozen artifacts

The following must be treated as **frozen**:

- Coefficients `w0, w_rules_full, w_rules_near, w_ml_max, w_anomaly_max, w_forecast_max`.
- Any normalization logic applied in the study (if used).
- Chosen operating regions (e.g., where `p_ensemble >= 0.5` is “high-risk”).

These should be stored in a small config file, e.g.:

```yaml
ensemble_logistic:
  w0: ...
  w_rules_full: ...
  w_rules_near: ...
  w_ml_max: ...
  w_anomaly_max: ...
  w_forecast_max: ...
  thresholds:
    critical: 0.70
    high: 0.50
    medium: 0.30
```

At runtime, the ensemble is a simple deterministic function using this config.

---

## 4. Per-User Meta State

### 4.1 Purpose

Meta state tracks **history and trends** that a single 14-day window cannot fully express.

Meta is used to:

- Refine bucket assignment near decision boundaries.
- Provide richer analyst notes (“trending up for 4 days”, “repeated partial S1 behavior”, etc.).
- Help define watchlist behavior and long-running risk patterns.

Meta is **not**:

- A new classifier.
- A replacement for the logistic ensemble.

### 4.2 Suggested meta fields

Per `user_key`, maintain:

**Trend counters**

- `forecast_trend_up_days`  
  Number of consecutive days forecast probability has increased by a meaningful delta.
- `ml_high_streak`  
  Number of consecutive days `ml_max` has been above a high-ish threshold.
- `anom_high_streak`  
  Number of consecutive days anomaly has been considered “high”.
- `rule_partial_streak`  
  Number of recent days with partial Scenario-1 patterns (e.g., after-hours + USB without full chain).

**Rolling maxima / peaks**

- `max_forecast_last_7d`
- `max_ml_last_7d`
- `max_anomaly_last_7d`

**Severity history**

- `days_in_high_or_above`  
  Count of days in `high` or `critical` severity within a recent window.
- `last_severity`  
  Last day’s bucket.
- `days_since_last_critical`  
  Distance from the most recent `critical` event.

**Watchlist-related**

- `on_watchlist`        (bool)
- `watchlist_since_day` (day when user first entered `high` or above)

### 4.3 Meta update function

Each day, for each user:

1. Start from `meta_prev` (or defaults for first appearance).
2. Update counters based on today’s detectors and logistic probability.
3. Return `meta_now` for use in bucket decision and notes.

Meta update is **pure logic** (no training).

---

## 5. Severity Buckets

Buckets provide a coarse-grained severity level for the analyst.

### 5.1 Core rules (base behavior)

A reasonable starting point (subject to tuning based on ensemble study):

- **Critical**
  - `rules_full == 1`, or
  - `p_ensemble >= 0.70`, or
  - `forecast_max >= 0.75`

- **High**
  - `0.50 <= p_ensemble < 0.70`, or
  - `rules_near == 1` and `ml_max` or `forecast_max` elevated, or
  - `forecast_max >= 0.50` with upward trend (`forecast_trend_up_days >= 2`)

- **Medium**
  - `0.30 <= p_ensemble < 0.50`, or
  - sustained anomaly streak (`anom_high_streak` above some threshold), or
  - repeated partial rule patterns (`rule_partial_streak` above some threshold)

- **Low**
  - everything else

### 5.2 Anomaly constraints

- Anomaly **never** promotes a user directly into `critical` or `high` by itself.
- Anomaly is treated as **supporting evidence only**, mainly affecting:
  - `medium` vs `low`
  - priority order within a bucket
  - analyst notes

### 5.3 Meta influence

Meta should only affect bucket decisions **at the edges**, for example:

- Upgrade from `medium` → `high` when:
  - `p_ensemble` is near 0.5 and
  - `forecast_trend_up_days` or `rule_partial_streak` is large.

- Keep bucket stable or de-escalate more slowly for users with long `days_in_high_or_above` or a very recent `critical` event.

This keeps behavior predictable and explainable.

---

## 6. Priority Score

Within a bucket, alerts are sorted by a simple **priority_score** derived from:

- `p_ensemble` (main driver)
- Meta trend counters (e.g., `forecast_trend_up_days`, `rule_partial_streak`, `anom_high_streak`)
- Bucket weight so that all `critical` > all `high` > `medium` > `low`

Example structure:

```python
priority = 0.7 * p_ensemble          + 0.1 * min(rule_partial_streak, 5)          + 0.1 * min(forecast_trend_up_days, 5)          + 0.05 * min(anom_high_streak, 5)          + bucket_weight[bucket]
```

Where `bucket_weight` might be `{low: 0.0, medium: 1.0, high: 2.0, critical: 3.0}`.

This is **not a model**; it is deterministic ordering logic.

---

## 7. Analyst Notes

Each alert should include an `analyst_notes` field composed from:

- Detector outputs
- Logistic probability and comparison to configured thresholds
- Meta trends and streaks

Examples:

- “Rule-based Scenario-1 chain detected today (after-hours + USB + exfil host).”
- “ML detector probability = 0.83, consistent with prior exfil windows.”
- “Forecast risk trending upward for 4 consecutive days.”
- “Anomaly detector elevated for 3 of the last 5 days (supporting evidence only).”
- “User has been high severity for 6 days in the last 10.”

Notes are built in code as a small list of strings that can be joined in the UI.

---

## 8. Alert Schema (Runtime Output)

Each emitted alert row should contain at least:

- **Core identifiers**
  - `day`
  - `user_key`

- **Detectors**
  - `rules_full`
  - `rules_near`
  - `ml_max`
  - `forecast_max`
  - `anomaly_max` (and any anomaly flags)

- **Ensemble & severity**
  - `p_ensemble`
  - `severity_bucket` (`critical`, `high`, `medium`, `low`)
  - `priority_score`

- **Meta snapshot (selected)**
  - `forecast_trend_up_days`
  - `rule_partial_streak`
  - `anom_high_streak`
  - `days_in_high_or_above`
  - `on_watchlist`
  - `watchlist_since_day` (if applicable)

- **Analyst-facing**
  - `analyst_notes` (list or string)

This schema feeds directly into the API/UI.

---

## 9. Implementation Phases

To avoid thrash, work in small, ordered steps:

### Phase 1 – Logistic ensemble integration
1. Extract coefficients and thresholds from `ensemble_study_summary.md`.
2. Create a small config file with weights and thresholds.
3. Implement a pure function:
   - `compute_logistic_ensemble(prong_scores, config) -> p_ensemble`.
4. Add `p_ensemble` to alerts (no buckets, no meta yet).
5. Sanity-check with a small run and compare to notebook samples.

### Phase 2 – Meta state plumbing
1. Define a `UserMeta` struct/class with the fields listed in Section 4.
2. In `run_loop`, create an in-memory `user_meta` dict keyed by `user_key`.
3. Implement `update_user_meta(meta_prev, detectors, p_ensemble) -> meta_now`.
4. Store a subset of `meta_now` in each alert for inspection.
5. Run a short simulation to verify meta fields behave as expected over multiple days.

### Phase 3 – Severity buckets
1. Implement `assign_severity_bucket(detectors, p_ensemble, meta_now)` using the logic in Section 5.
2. Add `severity_bucket` to alerts.
3. Check bucket distributions and example users to ensure behavior matches expectations.

### Phase 4 – Priority and notes
1. Implement `compute_priority_score(p_ensemble, meta_now, bucket)`.
2. Implement `build_analyst_notes(detectors, p_ensemble, meta_now, bucket)`.
3. Add `priority_score` and `analyst_notes` to alerts.
4. Verify the UI can sort by `severity_bucket` then `priority_score`, and display notes in the details view.

### Phase 5 – Clean-up & documentation
1. Remove or fully deprecate the previous bucket-risk fusion model from the runtime path.
2. Keep its notebook as a “failed experiment” with a short explanation.
3. Document the final ensemble and meta logic in the report, referencing:
   - Ensemble performance (ROC, PR-AUC).
   - Coefficients and interpretation.
   - Severity mapping and anomaly constraints.
   - How meta state supports triage and interpretability.

---

## 10. Guardrails (Things NOT To Do)

To keep future work aligned:

- Do **not**:
  - Train a second-level classifier on top of `p_ensemble` + meta.
  - Re-introduce a black-box “risk bucket” model.
  - Allow anomaly alone to promote users into `high` or `critical`.
  - Change logistic ensemble inputs without re-running the ensemble study.

- Do:
  - Keep the ensemble logistic regression as the **canonical fusion model**.
  - Use meta strictly as context, trend tracking, and tie-breaking.
  - Keep severity logic deterministic and documented.
  - Treat this file as the reference plan for future changes.

---

## 11. How Future Chats Should Use This

When asking for help in future sessions, reference this file as:

> “The ensemble & meta roadmap markdown we created for 6019.”

Assistants should:

- Respect this architecture unless explicitly instructed otherwise.
- Help implement or refine steps **within this framework**.
- Avoid suggesting new learned meta-layers unless the user explicitly wants to revisit the design.

This keeps the project grounded, debuggable, and aligned with the agreed final direction.
