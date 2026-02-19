# Scenario-1 UI Story Roadmap

Purpose: keep backend, UI, AG, and future-chat-you all pointed at the **same vision**:

> For each user, tell a clear timeline story of Scenario-1 risk:
> how bad they are now, how they got here, what the detectors saw, and how early the system could have known.

## 0. Non-Negotiable Invariants

1. Ensemble = logistic oracle
2. One canonical alert schema
3. RiskMeta is the single summary
4. Forecast watch windows come from alerts

## 1. Backend Contract Checklist

### 1.1 `/api/users`
Sidebar rows must expose:
- user identity
- alert counts
- severity and ensemble
- priority score
- analyst_notes

### 1.2 `/api/users/{user_key}/alerts`
All alerts normalized; no detector tabs in API.

### 1.3 `/api/alerts/{alert_id}` and `/api/alerts/{alert_id}/window`

## 2. Backend Implementation Phases

- Phase B1: finalize RiskMeta & watch windows
- Phase B2: finalize analyst_notes
- Phase B3: enrich normalize_alert

## 3. Minimal UI Proof-Of-Life Changes

Small JS tweaks to prove backend correctness before AG rewrites UI.

# Scenario-1 UI Story Roadmap

Purpose: keep backend, UI, AG, and future-chat-you all pointed at the **same vision**:

> For each user, tell a clear **timeline story** of Scenario-1 risk:
> how bad they are now, how they got here, what the detectors saw, and how early the system could have known.

---

## 0. Non-Negotiable Invariants

These are contracts. AG and future-you are **not** allowed to change them without a conscious decision:

1. **Ensemble = logistic oracle**
   - Inputs (per day, per user):
     - `rules_full_flag` (0/1)
     - `rules_near_flag` (0/1)
     - `ml_max` (0–1)
     - `anomaly_max` (0–1)
     - `forecast_max` (0–1)
   - Output:
     - `ensemble_score ∈ [0,1]`
     - `escalated: bool`
     - `severity ∈ {"low", "medium", "high", "critical"}`
   - Lives in backend (`compute_ensemble` in `app.py`), **not** re‑implemented in JS.

2. **One canonical alert schema**
   - All detectors write raw events into NDJSON.
   - Backend normalizes them into a single per‑alert schema for the UI.
   - UI only sees **normalized** alerts, never raw detector lines.

3. **RiskMeta is the single per-user risk summary**
   - Derived from normalized alerts for that user.
   - Used to rank users, generate analyst notes, and drive watchlist / wall-of-shame.

4. **Forecast watch windows come from alerts**
   - Forecast detector produces `forecast` alerts with scores.
   - Backend aggregates these into per-user watch windows and watchlist entries.
   - UI never recomputes watch logic on its own.

---

## 1. Backend Data Model Contracts

### 1.1 RiskMeta (per user)

`RiskMeta` is computed in the backend (Python) and stored in memory alongside alerts.

Minimum fields:

- `user_key: str`
- `days_high_or_above: int`  
  Number of days where `severity ∈ {"high", "critical"}`.
- `last_severity: str | null`  
  Severity on the latest alert for this user, or `null` if no alerts.
- `max_ensemble: float`  
  Max ensemble score across all alerts for this user.
- `ever_escalated: bool`  
  Has this user **ever** hit `escalated=True`.
- `max_forecast: float`  
  Max forecast score across all alerts for this user.
- `first_forecast_day: str | null`  
  First day where `forecast_score ≥ FORECAST_WATCHLIST_THRESHOLD`.
- `first_escalation_day: str | null`  
  First day where `escalated=True`.
- `priority_score: float`  
  Monotone score (0–1) representing “how much should the analyst care, relative to others”.

Implementation note: `RiskMeta` is recomputed in backend after alerts are loaded, using only normalized alerts.

---

### 1.2 Normalized Alert Schema

`normalize_alert(raw_record)` converts raw NDJSON from `run_loop` into one canonical alert dict.

Minimum fields:

- Identity:
  - `alert_id: str` (stable identifier: e.g., hash of user_key + day + detector + reason).
  - `day: str` (YYYY-MM-DD)
  - `user_key: str`
  - `user_name: str`
  - `user_role: str | null`
- Ensemble:
  - `ensemble_score: float`
  - `severity: str` (`"low" | "medium" | "high" | "critical"`)
  - `escalated: bool`
  - `ensemble_explanation: dict`  
    Contains component weights / scores (already implemented).
- Per-prong scores:
  - `rule_hits: List[str]`  
    e.g., `"s1_chain"`, `"s1_near_miss"`, etc.
  - `ml_score: float | null`
  - `anomaly_score: float | null`
  - `forecast_score: float | null`
- Narrative:
  - `rule_human_summary: str | null`  
    From `build_s1_human_summary`.
  - `detector_label: str`  
    e.g., `"Rule-Based"`, `"ML Model"`, `"Anomaly"`, `"Forecast"`, `"Ensemble"`.
  - `alert_narrative: str`  
    Short paragraph describing “why this alert exists”.

The UI should treat this schema as read‑only **truth**.

---

## 2. Backend API Contracts

These are the endpoints AG is allowed to depend on. Shapes must stay stable.

### 2.1 `GET /api/users`

Purpose: provide rows for the left sidebar (both Alerts and Watchlist tabs).

Query params:

- `tab`: one of `"all"`, `"escalated"`, `"watchlist"`, `"wall_of_shame"` (optional future).

Per-user JSON object must expose:

- Identity:
  - `user_key`
  - `name`
  - `role`
  - `is_terminated: bool`
- Counts:
  - `total_alerts: int`
  - `escalated_alerts: int`
- Risk & ranking:
  - `severity: str` (usually `RiskMeta.last_severity` or fallback)
  - `max_ensemble: float`
  - `max_forecast: float`
  - `risk_days_high_or_above: int`
  - `risk_ever_escalated: bool`
  - `priority_score: float`
- Narrative:
  - `analyst_notes: str`

Filtering / sorting rules:

- `tab=all` → active users only (`is_terminated == False`), sorted by `priority_score desc`.
- `tab=escalated` → `escalated_alerts > 0`, sorted by `priority_score desc`.
- `tab=watchlist` → users with `max_forecast ≥ FORECAST_WATCHLIST_THRESHOLD`, sorted by `max_forecast desc`.
- `tab=wall_of_shame` (optional later) → `is_terminated == True` and `risk_ever_escalated == True`, sorted by `max_ensemble desc`.

---

### 2.2 `GET /api/users/{user_key}/alerts`

Purpose: full alert **timeline** for that user. No per-detector tabs in the API; tabs are a pure UI filtering concern.

Response: list of normalized alerts (see schema above), sorted **descending by day**.

UI uses this list to:

- Draw per-user trendlines (sparklines) for ensemble / forecast / ml / anomaly.
- Populate the Rules / ML / Anomaly / Forecast tables via client-side filtering.

---

### 2.3 `GET /api/alerts/{alert_id}`

Purpose: fetch a single normalized alert for the modal.

Response: the same object as in `/api/users/{user_key}/alerts`, plus any extra evidence payload.

---

### 2.4 `GET /api/alerts/{alert_id}/window`

Purpose: show the 14-day window that produced this alert.

Response:

```json
{
  "rows": [
    {
      "day": "YYYY-MM-DD",
      "logon_after_hours_rate": float,
      "device_after_hours_rate": float,
      "file_after_hours_rate": float,
      "device_n_usb_connects": int,
      "http_n_wikileaks": int
    },
    ...
  ]
}
```

UI renders this in the bottom window table.

---

### 2.5 `GET /api/state`

- Returns `current_day` and `paused` flag for the simulation status bar.

### 2.6 `GET /api/heartbeat`

- Returns small timeseries for the top chart.
- When `sidebarTab=alerts`: counts per day per detector (`rule_count`, `ml_count`, `anomaly_count`).
- When `sidebarTab=watchlist`:
  - With `user_key`: per-user history (`forecast_max`, `count` per day).
  - Without `user_key`: global watchlist aggregate (`count`, `forecast_hit_count`).

---

## 3. Backend Implementation Phases

These are **backend-only** tasks that should be complete before AG is allowed to redesign layout.

### Phase B1 – Finalize RiskMeta & watch windows

- Ensure `compute_watch_windows()` builds `STATE.watch_windows_by_user` from normalized alerts.
- Ensure `recompute_risk_meta()` runs after alerts + watch windows are loaded and populates all `RiskMeta` fields listed above.

### Phase B2 – Finalize analyst_notes

Implement `_build_analyst_notes(user_key, visible_alerts, risk_meta)` in `app.py` so it:

- Looks at:
  - last alert day & severity,
  - `risk_days_high_or_above`, `max_ensemble`, `ever_escalated`,
  - whether rules fired, whether forecast scores are elevated.
- Returns a short one- or two-sentence summary, for example:

> "Last Scenario-1 alert on 2010-07-09 (high). 3 high-risk days so far; max ensemble 0.88. Pattern shows repeated after-hours + USB activity and rising ML risk."

`/api/users` must include `analyst_notes` for each user.

### Phase B3 – Enrich normalize_alert

Extend `normalize_alert(raw_record)` to:

- Set `detector_label` consistently.
- Set `alert_narrative` (start with `human_summary` or `ensemble_explanation["human_readable_summary"]`).
- Ensure all fields in the normalized schema are populated or set to `null`.

Once B1–B3 are done, the backend contract is stable enough for UI rewrites.

---

## 4. Minimal UI Proof-Of-Life Changes (You, not AG)

Goal: prove the backend contracts are working **without** large redesign.

These are small JS tweaks in `src/ui/static/app.js`:

1. **Sidebar uses analyst_notes and priority**
   - For each user row, use:
     - severity dot (from `severity`),
     - name + user_key,
     - badges for `total_alerts` and `escalated_alerts`,
     - optional hover tooltip or small muted line from `analyst_notes`.
   - Sorting logic: use `priority_score` from server; do not re-sort in JS.

2. **Detail panel uses normalized alerts only**
   - `/api/users/{user}/alerts` becomes the single source of truth.
   - Tabs (`Rules`, `ML`, `Anomaly`, `Forecast`) simply filter the same array by detector scores / hits.

3. **Modal uses alert_narrative and ensemble_explanation**
   - Header shows user, role, day, severity, ensemble_score.
   - Body uses `alert_narrative` and `ensemble_explanation.components` to render Detector Findings.

These changes confirm backend data is shaped correctly before AG touches layout.

---

## 5. Future UI Vision (For AG)

Once backend phases B1–B3 and minimal UI tweaks are stable, AG can implement the richer UI:

1. **Per-user trend story**
   - Combined sparkline for ensemble score over time.
   - Overlaid forecast sparkline.
   - Markers for:
     - first forecast day,
     - first escalation day,
     - termination day,
     - strong rule-chain days.

2. **Watchlist view**
   - Left sidebar: sorted by `max_forecast`.
   - Summary strip per user: max forecast, first forecast, first escalation, lead time.

3. **Wall of Shame (optional)**
   - Separate tab driven by `tab=wall_of_shame` on `/api/users`.
   - Shows confirmed exfiltrators and how early the system could have flagged them (lead time, missed chances, etc.).

All of this must be built **only** on the contracts in this document, not by re-inventing ensemble or risk logic in JS.