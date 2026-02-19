# Pipeline Threshold & Ensemble Overhaul  
*(Summary of Issues, Evidence, and Planned Changes)*

This document summarizes what the current detection pipeline is doing, what the
analysis notebook revealed, and what changes will be made to stabilize scoring,
reduce noise, and correctly escalate true malicious chains. It is meant to be
readable by someone who does **not** know the internals of the project.

The evidence in this document comes from `notebooks/analysis/thresholds_ensemble_prechecks.ipynb` outputs:

- Lead-time histograms for each detector (days between first alert and
  `exfil_start`).
- Score distributions split by insider vs non-insider for each detector.

---

## 1. What’s Working Today

### 1.1 Rule Detector (Scenario-1)

**Evidence**

- Lead-time histogram: most insiders get their first rule alert between 0–3
  days before exfiltration, with a small tail out to ~14 days.
- Score distribution:
  - Non-insiders: score is always **0.30** (near-miss only).
  - Insiders: mixture of **0.30** (near-miss), **0.80** (full chain),
    and **0.95** (post-departure chain). Mean ≈ **0.57**.

**Interpretation**

- Rules give **clean “this is a data theft chain” labels** when they fire.
- Non-insiders essentially never get full-chain scores; they only hit the
  0.30 “prep” band.
- Lead times are realistic: rules fire close to the real exfil events.

**Conclusion**

The S1 rule detector is doing its job:
- high precision on full chains,
- clear semantics (near-miss vs chain vs post-departure),
- good human-readable summaries.

We are not planning major changes to the rule logic in this pass.

---

### 1.2 ML Detector (Frozen Scenario-1)

**Evidence**

- Score distribution (insiders only):
  - Mean ≈ **0.87**
  - 25th percentile ≈ **0.78**
  - Median ≈ **0.91**
  - 75th percentile ≈ **0.98**
  - Min ≈ **0.51**, max ≈ **0.996**
- Lead-time histogram: almost all insiders get their first ML alert at
  **0 days before exfil** (on the exfil day), with a few at **3** and **7–8**
  days early.

**Interpretation**

- ML scores are **high and sharp** around true exfil windows.
- There is no evidence of score collapse or weird scaling: probabilities
  are already in a sensible 0.5–1.0 range for insiders.
- ML behaves like a **strong detection prong**, not a long-horizon forecaster.

**Conclusion**

The ML model is a reliable “is this exfil today?” detector. The main task is
to integrate it cleanly into the ensemble and avoid letting noisier prongs
drown it out.

---

### 1.3 Anomaly Detector (IF + Boost/Damp)

**Evidence**

Score distribution:

- **Non-insiders**
  - Count ≈ 75k windows
  - Mean ≈ **0.61**, std ≈ 0.14
  - 25th ≈ **0.51**, median ≈ **0.58**, 75th ≈ **0.71**
  - Range ≈ **0.40–1.0**

- **Insiders**
  - Count ≈ 593 windows
  - Mean ≈ **0.87**, std ≈ 0.14
  - 25th ≈ **0.79**, median ≈ **0.92**, 75th ≈ **1.0**
  - Range ≈ **0.41–1.0**

Lead-time histogram:

- Most insiders see their first anomaly alert **0–10 days** before exfil.
- A few insiders have anomaly alerts **60–70 days** before exfil.

**Interpretation**

- Anomaly scores are **higher for insiders on average**, but:
  - Non-insiders regularly reach 0.70+.
  - Both groups share the same top end near 1.0.
- Lead times are short and plausible for most users, but a small number of
  insiders have very early spikes that do not match the S1 chain.

**Conclusion**

The anomaly detector is useful **as supporting evidence**:
- it highlights “weird” behavior near exfil,
- but raw scores alone are noisy and overlap heavily with non-insiders.

Anomaly should not be allowed to escalate users on its own.

---

### 1.4 Forecast Detector (Scenario-1)

**Evidence**

Score distribution:

- **Non-insiders**
  - Count ≈ 1,976 windows
  - Mean ≈ **0.63**, std ≈ 0.11
  - 25th ≈ **0.54**, median ≈ **0.59**, 75th ≈ **0.68**
  - Range ≈ **0.50–0.95**

- **Insiders**
  - Count ≈ 225 windows
  - Mean ≈ **0.78**, std ≈ 0.15
  - 25th ≈ **0.64**, median ≈ **0.81**, 75th ≈ **0.92**
  - Range ≈ **0.50–0.99**

Lead-time histogram:

- Most insiders get their first forecast alert in the **0–30 day** range.
- A small but important subset gets alerts **100–200+ days** before exfil.
- The worst case is roughly **350 days** before exfil_start.

**Interpretation**

- Forecast does have signal: insiders are shifted upward (≈0.78 vs 0.63).
- But the **current threshold of 0.6 is basically below the non-insider mean**,
  so a typical safe user is “watchlist-worthy” by design.
- Some insiders are flagged **hundreds of days early**, which is not a useful
  operational horizon.

**Conclusion**

Forecast is too sensitive and too confident. It needs:

- a higher threshold,
- a reasonable maximum horizon,
- and a strictly “watchlist, not escalation” role.

---

## 2. What’s NOT Working

### 2.1 Detector Score Scales & Roles Are Misaligned

The current ensemble logic treats all non-rule scores as if they live on a
comparable scale. The notebook shows they don’t:

- **ML** (insiders only): tight band in **[0.5 – 1.0]**, designed as a
  *binary detector* for exfil.
- **Anomaly**: both insiders and non-insiders occupy **[0.4 – 1.0]**, with
  heavy overlap and a non-insider mean ≈ **0.61**.
- **Forecast**: non-insiders centered around **0.63**, insiders around **0.78**,
  both in **[0.5 – 1.0]**.

When you feed these raw numbers straight into an ensemble, you get:

- Forecast and anomaly frequently larger than ML outside the true exfil window.
- ML’s clean “this is the exfil day” signal overshadowed by noisier prongs.

### 2.2 Forecast Is Over-Sensitive and Over-Horizon

From the lead-time histogram:

- Most insiders get their first forecast alert near the exfil period,
  which is good.
- But some insiders trigger **100–350 days** before exfil, which is
  meaningless as a practical warning.

Combined with the score distribution:

- A threshold of **0.6** is below the *average* non-insider forecast score.
- That means “typical safe user” is **on the watchlist** unless we filter
  aggressively later.

### 2.3 Anomaly Over-Contributes to Noise

From the anomaly stats:

- Non-insiders sit around **0.61** with a 75th percentile of **0.71**.
- Insiders sit higher (mean ≈ 0.87), but both groups reach 1.0.

In practice:

- Many non-insiders regularly cross **0.7–0.8**, which looks “scary” if you
  read the score literally.
- If the ensemble lets anomaly add large boosts, these safe spikes can push
  alerts over escalation thresholds, especially when ML is quiet.

### 2.4 Near-Miss Rule Behavior Is Underweighted

From per-user timelines:

- Near-miss (`rules:s1_near_miss`) cases often coincide with:
  - medium-high forecast scores,
  - medium anomaly scores,
  - and rising ML later.
- Today, near-misses often end up with ensemble scores that **do not cross
  any escalation threshold**, even when all three non-rule detectors are
  pointing in the same direction.

Operationally, this means “prep phase” insiders are visible only as weak,
non-escalated alerts.

---

## 3. Planned Changes (Detector-Level)

### 3.1 ML (Scenario-1 Detector)

Role: **Primary exfil-day detector.**

Planned changes:

- Keep the existing model and probabilities (they already behave well).
- Use ML as the **main numeric backbone** of the ensemble:
  - “How likely is it that this is the exfil day?”

### 3.2 Anomaly (Isolation Forest)

Role: **Supporting evidence only.**

Planned changes:

- Keep raw anomaly score and z-score logic, but:
  - Treat anomaly as a **bonus**, not a driver.
  - Cap anomaly’s contribution so it can nudge the ensemble by at most
    a small amount (e.g., +0.05) even for very high z-scores.
- Strengthen early filters:
  - If base anomaly score < **0.5** and `z_max < 2.0`, drop the alert.
  - Require stronger combinations (e.g., anomaly ≥ 0.8 **and** some ML or
    rule support) before we even emit an alert to the sidebar.

Goal: keep the insight (“this is weird”) without drowning the analyst
in anomaly-only noise.

### 3.3 Forecast (Scenario-1, 7-day Horizon)

Role: **Watchlist / early-warning only.**

Planned changes:

- Raise the watchlist threshold from **0.60 → at least 0.75**:
  - puts it above the non-insider median and close to the insider median.
- Add a simple horizon rule:
  - Ignore or heavily damp forecast scores that are more than **30 days**
    before a plausible exfil window (based on lead-time histogram).
- Enforce a strict rule that **forecast alone cannot escalate**:
  - Forecast can put users on the watchlist,
  - but escalation requires either strong ML or a rule hit.

Goal: forecast highlights “who we should watch,” but does **not** generate
standalone “drop everything” alerts from vague long-horizon guesses.

---

## 4. Planned Changes (Ensemble Logic)

### 4.1 Forecast Detector Role & Thresholding (Revised)

Earlier drafts explored adding a fixed “30-day horizon clamp” to suppress extremely early forecast scores.  
After deeper analysis of the forecasting model and its statistical behavior, we determined that such a clamp is not appropriate for a real-time detection system.

The Scenario-1 forecasting detector is trained entirely on **14-day behavioral windows**, labeled positive when the window ends within seven days before exfiltration.  
During inference, the model has **no knowledge of ground-truth exfil dates** and no ability to calculate “how many days early” a prediction is.  
Introducing a rule like *ignore any forecast more than 30 days before exfiltration* would leak hindsight into the operational pipeline and artificially censor legitimate statistical patterns learned by the model.

**Because of this, the runtime system does not implement a horizon clamp.**

Instead, we enforce the intended operational role of the forecast detector through thresholding and carefully controlled ensemble behavior:

1. **Forecast is strictly watchlist-only.**  
   Forecast probabilities represent early-warning signals, not confirmations of exfiltration.  
   Forecast scores can never escalate a user by themselves in the ensemble.

2. **The forecast watchlist threshold is increased to 0.75.**  
   Pre-check distributions showed:
   - Non-insiders: mean ≈ 0.63  
   - Insiders: mean ≈ 0.78  
   Raising the threshold creates a clean separation between noisy early predictions and meaningful early-warning signals.

3. **Forecast influence requires ML agreement.**  
   Even very high forecast scores (≥ 0.90) are only allowed to contribute to escalation if the ML detector shows at least mild support (ML ≥ 0.30).  
   This prevents long-lead false positives from triggering critical alerts and ensures forecast acts purely as context.

These changes preserve the strengths of the forecasting model—useful early lead time—while preventing it from overpowering the alerting pipeline or producing unrealistic escalations.

### 4.2 Base Score

1. Start from ML and forecast:

   ```python
   base = max(ml_score, forecast_score or 0.0)
   ```

2. If anomaly has **supporting context** (for example, anomaly ≥ 0.8 **and**
   `ml_score >= 0.5` or there is any rule hit present), then nudge the base
   slightly, but keep a hard ceiling so anomaly cannot dominate:

   ```python
   base = min(base + 0.05, 0.95)
   ```

Otherwise, anomaly does **not** influence the base score.

---

### 4.3 Rule-Driven Overrides

Rules remain the primary source of truth for Scenario‑1:

- **Post-departure chain** (`s1_chain_post_departure`):

  ```python
  ensemble_score = 1.0
  escalated = True
  rule_case = "s1_chain_post_departure"
  ```

- **Full chain while employed** (`s1_chain`):

  ```python
  ensemble_score = max(base, 0.95)
  escalated = True
  rule_case = "s1_chain"
  ```

- **Near-miss** (`s1_near_miss`):

  - If other detectors agree (`base >= 0.7`), treat as almost a full exfil:

    ```python
    ensemble_score = max(base, 0.9)
    escalated = True
    ```

  - Otherwise, keep as a strong but **non‑escalated** alert:

    ```python
    ensemble_score = max(base, 0.5)
    escalated = False
    ```

If none of the S1 rule cases fire, `rule_case` stays `"none"` and we fall
back to detector-only logic.

---

### 4.4 Non‑Rule Escalation

When there are **no rule hits** on a given day:

1. Start from the base score computed in 4.2.
2. Define support signals:

   ```python
   strong_ml = ml_score >= 0.75
   strong_forecast = (forecast_score or 0.0) >= 0.90
   ```

3. Apply escalation rules:

   - If `strong_ml` and `base >= 0.85`, escalate (ML is strongly confident).
   - Else if `strong_forecast` **and** `ml_score >= 0.30`, escalate
     (forecast is screaming and ML is at least mildly supportive).
   - Otherwise, keep the alert non‑escalated.

In all cases, anomaly **never** escalates by itself; it is strictly
supporting evidence.

---

## 5. Figures to Include as Evidence

From the analysis notebook, paste these into this document (or reference
them by filename) so the design choices are backed by actual plots.

1. **Lead-time histograms (four panels)**  
   - Forecast, anomaly, ML, and rules.  
   - Show that:
     - ML and rules fire near exfil (roughly 0–10 days).
     - Anomaly mostly fires near exfil but has a few long‑lead outliers.
     - Forecast sometimes fires hundreds of days early.

2. **Score distribution plots or tables**  
   - Anomaly: insiders vs non‑insiders (means ≈ 0.87 vs 0.61).  
   - Forecast: insiders vs non‑insiders (means ≈ 0.78 vs 0.63).

---

## 6. Results After Threshold & Ensemble Overhaul

This section summarizes what changed in the live pipeline after the new thresholds and ensemble logic were wired into `run_loop` and the UI, and what stayed the same.

### 6.1 Alert volume & coverage

Using the post‑checks notebook (`thresholds_ensemble_postchecks.ipynb`) on the updated alerts:

- **Total alerts** dropped slightly:
  - Before: ~78,658 alerts
  - After:  ~76,659 alerts  
  This is the expected reduction from tightening anomaly and forecast behavior.
- **Insider coverage stayed perfect:**
  - Insiders hit: **29 / 29** (before and after)
  - Insider user recall: **1.0** in both pre‑ and post‑checks.
- **Non‑insider surface shrank:**
  - Total non‑insider users with any alert went from **1,573 → 1,377**.
  - **Non‑insider user FPR** ("any escalated alert on a safe user") remains **0.0**.

Net effect: we cut low‑value noise while keeping complete coverage on the Scenario‑1 insiders.

### 6.2 Detector behavior after changes

- **Rules**
  - Behavior and lead‑time patterns are unchanged.
  - Full chains and post‑departure chains still map directly to high ensemble scores and escalations.

- **ML detector**
  - ML still acts as the primary exfil‑day signal.
  - Its scores now drive the base of the ensemble more cleanly, instead of being overshadowed by anomaly or forecast.

- **Anomaly detector**
  - Early drop and boost/damp logic remove most of the non‑insider “medium‑high but boring” spikes.
  - Anomaly now behaves as intended: a supportive nudge (+0.05 max) when ML/forecast/rules are already pointing at the same user, instead of generating standalone escalations.

- **Forecast detector**
  - Raising the watchlist threshold to **0.75** trimmed a large number of weak forecast hits on safe users.
  - Forecast is now strictly **watchlist‑only** and only helps escalation when ML shows at least mild support.
  - Lead‑time histograms still show useful early warning (0–30 days) without letting very early, uncertain scores dominate alerting.

### 6.3 Ensemble & UI impact

- Ensemble **user‑level behavior** is stable:
  - All 29 Scenario‑1 insiders are still escalated at least once.
  - No non‑insider users are escalated.
- The score bands now better reflect intent:
  - Rules anchor the top end (0.8–1.0) for clear exfil chains.
  - ML dominates the mid‑high range when it is confident.
  - Anomaly and forecast show up as context and small nudges, not as primary drivers.
- In the UI, this translates to:
  - A smaller, more meaningful set of users in the sidebar.
  - Watchlist users whose forecast scores are actually interesting, instead of “everyone with a 0.6+ blip.”

**Bottom line:** the pipeline now keeps 100% insider coverage, removes a noticeable amount of noise from safe users, and enforces clear roles for each prong (rules as ground truth, ML as backbone, anomaly/forecast as context) without any brittle, hindsight‑based horizon hacks.