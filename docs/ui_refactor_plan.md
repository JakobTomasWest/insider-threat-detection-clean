# Scenario-1 UI Refactor & Backend Prep Guide

**Goal:** Keep the whole stack (backend, UI, AG, future-you) locked on one vision:

> For each user, tell a clear Scenario-1 risk story:  
> who is most dangerous *right now*, how they got there, and what evidence each detector saw.

This file is your guardrail. Paste chunks of it into new chats when you feel things drifting.

---

## 0. Non-negotiable invariants

These are *facts* of the system. Do not let any future prompt contradict them.

1. **Ensemble = Logistic Oracle**
   - You already wired logistic fusion of prong scores.
   - Ensemble score is a probability in `[0,1]` and is the **only** scalar severity.
   - No bucket-risk fusion, no weird extra meta models.

2. **Single canonical alert schema**
   - Every alert surfaced to the UI must normalize into the same shape, regardless of detector.
   - At minimum each alert has:
     - `alert_id`
     - `day`
     - `user_key`
     - `user_name`
     - `user_role`
     - `detector` (e.g. `"rules" | "ml" | "anomaly" | "forecast"`)
     - `rule_hits` (list of strings, may be empty)
     - `rules_score`, `ml_score`, `anomaly_score`, `forecast_score`
     - `ensemble_score`
     - `escalated` (bool)
     - `ensemble_explanation` (components per prong)
     - `rule_human_summary` (if any)

3. **RiskMeta is the per-user summary brain**
   - Per user, you maintain:
     - `severity_bucket` (`"low" | "medium" | "high" | "critical"`)
     - `priority_score` (float in `[0,1]`, monotonic with ensemble; used for sorting)
     - `analyst_notes` (list of dated notes summarizing their risk story)
   - This object is what the hero panel, case notes, and sidebar all lean on.

4. **Forecast watch windows come from alerts**
   - Watchlist / “forecasted users” is derived from forecast alerts + RiskMeta.
   - No separate random watchlist logic.

---

## 1. UX vision (what the UI must feel like)

### 1.1 High-level layout

Screen has three main regions:

1. **Hero Panel (Top)**

   Purpose: Immediately answer “who do I care about *right now*?”

   - Shows a **scrollable list of highest-risk users**, system-wide.
   - Sorted by `severity_bucket` then `priority_score` (descending).
   - For each user:
     - Full name
     - User ID
     - Severity badge (color by bucket)
     - Key mini-facts (optional):
       - max forecast score
       - first escalated day
       - first forecast day
   - Clicking a user in the hero:
     - Selects that user
     - Updates user-specific section & risk timeline (same as selecting in sidebar).

   This panel **replaces** the old “Alert Volume (7-Day Window)” chart as the main hero.

2. **Left Sidebar**

   Purpose: “Roster of active employees with alert counts.”

   - Shows **current active employees only**, *not* everyone who ever existed.
   - Backed by LDAP (or equivalent) `first_seen` / `last_seen`:
     - `is_active == true` means `current_day` is within employment range.
   - Layout:
     - Name
     - `(user_key)` in muted text
     - Badge: total alerts
     - Badge: escalated alerts (if > 0)
   - Clicking a user here:
     - Selects them (same effect as clicking in hero).
     - Updates user header + risk timeline + case notes hooks.

3. **User Section (Middle + Bottom)**

   When a user is selected (from hero or sidebar):

   ```text
   [User Header]
     User: Dora Amelia Spears          [Case Notes ▸]

   [User Risk Timeline Graph]
     - X-axis: time
     - Y-axis: score in [0,1]
     - Lines: rules_score, ml_score, anomaly_score, forecast_score, ensemble_score
     - Dots: days with alerts (clickable → Alert Details modal)



[Alert Details Modal]
  - Same modal style you already have.
  - Triggered by clicking any dot / row.

```
• The old per-detector bottom table (“Rule-Based / ML / Anomaly / Forecast tabs”) is replaced by this multi-line risk timeline.
• Graph shows history of detector scores + ensemble for this user.

4. Case Notes View (Hero Panel replacement)
Clicking Case Notes in the user header:
• Temporarily replaces the Hero Panel with a Case Notes panel for that specific user.
• User Risk Timeline graph stays visible below.
Case Notes panel:
• Title: Case notes for Dora Amelia Spears
• Content: list of notes from risk_meta.analyst_notes, for example:
• 2010-07-05: Forecast model first predicted high risk (p=0.82).
• 2010-07-09: Full Scenario-1 chain (AH+USB+Wikileaks) while employed.
• 2010-07-15: Post-departure chain confirmed; escalated to Data Theft.
• ✕ Close button restores Hero Panel.
Analyst workflow:
1. Look at Hero Panel → pick hottest users.
2. Click user → see Risk Timeline.
3. Click Case Notes → read story + dates.
4. Click dots on the timeline at those dates → open Alert Details modal.
```

---

## 2. Backend prep: contracts before AG touches the UI

You must get these API contracts into a stable, documented shape before you ask AG to do frontend surgery.

## 2.1 Define / update data structures

### 2.1.1 RiskMeta (per user)
Python concept (pseudo‑schema):

```python
risk_meta = {
    "user_key": str,
    "severity_bucket": "low" | "medium" | "high" | "critical",
    "priority_score": float,  # 0–1, used for sorting and hero-panel ranking

    "analyst_notes": [
        {
            "day": "YYYY-MM-DD",
            "kind": "forecast_spike" | "full_chain" | "near_miss" | "post_departure_chain",
            "message": str,  # plain‑English story
        },
        ...
    ],

    # optional quick fields that are very useful for UI:
    "max_forecast": float | None,
    "first_forecast_day": "YYYY-MM-DD" | None,
    "first_escalation_day": "YYYY-MM-DD" | None,
}
```

Where it can live:
- Option A: built on the fly from alerts when the API starts.
- Option B: stored as its own table after offline processing.

For AG / UI, the only thing that matters is that backend can answer:
- “Give me RiskMeta for all users.”
- “Give me RiskMeta for user X.”

### 2.1.2 Normalized alert record
Every alert used by the UI should look like:

```python
{
    "alert_id": str,
    "day": "YYYY-MM-DD",
    "user_key": str,
    "user_name": str,
    "user_role": str,

    "detector": "rules" | "ml" | "anomaly" | "forecast",
    "rule_hits": list[str],

    "rules_score": float | 0.0,
    "ml_score": float | 0.0,
    "anomaly_score": float | 0.0,
    "forecast_score": float | 0.0,
    "ensemble_score": float,

    "escalated": bool,

    "ensemble_explanation": {
        "components": {
            "rule": {
                "score": float,
                "fired": bool,
                "hits": list[str],
            },
            "ml": {
                "score": float,
            },
            "anomaly": {
                "score": float | None,
            },
            "forecast": {
                "score": float | None,
            },
        },
    },

    "rule_human_summary": str | None,
}
```

You mostly have this already in app.py → keep everything consistent with this contract.

---

## 3. Backend implementation steps (before AG)

Phase B1 – RiskMeta + severity buckets

1. Define severity buckets (thresholds):  
   Example (you can tune later):

   - critical: user’s max `ensemble_score` >= 0.90  
   - high: max `ensemble_score` >= 0.75  
   - medium: max `ensemble_score` >= 0.50  
   - low: otherwise  

   Severity is per user, based on their worst recent state.

2. Compute per-user aggregates from alerts:  
   From your alerts table:

   - `max_ensemble`
   - `max_forecast`
   - earliest day `forecast_score >= 0.6` (or whatever threshold)
   - earliest day `escalated == True`

   Use these to set:

   - `severity_bucket` from thresholds above.
   - `priority_score` (e.g. combine `max_ensemble` with recency if you want).

3. Build `analyst_notes`:  
   For each user, add 2–5 simple notes if applicable:

   - When forecast first went above 0.6:
     - `kind="forecast_spike"`
     - message like `Forecast model first predicted high exfil risk (~0.82)`

   - When rule near-miss fired:
     - `kind="near_miss"`

   - When full S1 chain fired:
     - `kind="full_chain"`

   - When post-departure chain fired:
     - `kind="post_departure_chain"`

   Keep it simple and readable. This is literally what shows in the Case Notes panel.

4. Expose helper functions:

```python
def get_risk_meta_for_user(user_key: str) -> dict: ...
def get_all_risk_meta() -> list[dict]: ...
```

These are internal helpers used by the API handlers.

Phase B2 – API contracts

### 3.2.1 GET `/api/users`

Purpose: populate Hero Panel and Sidebar.

Response: list of users like:

```json
{
  "user_key": "das1320",
  "name": "Dora Amelia Spears",
  "user_role": "Technician",
  "total_alerts": 33,
  "escalated_alerts": 7,
  "severity_bucket": "high",
  "priority_score": 0.92,
  "max_forecast": 0.99,
  "is_active": true
}
```

Query params to support:

- `tab=alerts|watchlist` (you already have this pattern)
- `active_only=true` (used by sidebar to hide terminated staff)

Hero Panel:

- Uses all users, sorted by `severity_bucket` then `priority_score`.

Sidebar:

- Uses same data but only where `is_active == true`.

### 3.2.2 GET `/api/users/{user_key}/alerts`

Purpose: user-specific risk timeline + Case Notes.

Response:

```json
{
  "user_key": "das1320",
  "user_name": "Dora Amelia Spears",
  "user_role": "Technician",
  "risk_meta": {/* RiskMeta object from above */ },
  "alerts": [
    {
      "alert_id": "das1320_2010-07-09_rules",
      "day": "2010-07-09",
      "detector": "rules",
      "rule_hits": ["s1_chain"],
      "rules_score": 0.80,
      "ml_score": 0.00,
      "anomaly_score": 0.00,
      "forecast_score": 0.97,
      "ensemble_score": 0.86,
      "escalated": true,
      "ensemble_explanation": { "...": "..." },
      "rule_human_summary": "Likely data theft: ..."
    }
    // more alerts
  ]
}
```

UI uses:

- `alerts[]`:
  - build trendline data per day and per detector.
  - know which points on the timeline are clickable.

- `risk_meta`:
  - show Case Notes.
  - maybe show severity / key stats in header.

### 3.2.3 GET `/api/alerts/{alert_id}` and `/api/alerts/{alert_id}/window`

Keep as-is, but make sure they still match what the modal expects:

- Alert details endpoint returns:
  - `user_name`, `user_role`
  - `day`
  - `ensemble_score`
  - `escalated`
  - `ensemble_explanation`
  - `rule_human_summary` (optional)

- Window endpoint returns 14-day table rows for the window panel.

Phase B3 – Minimal backend sanity checks

Before touching JS:

1. Call `/api/users` (browser or curl):

   - Verify objects have:
     - `severity_bucket`
     - `priority_score`
     - `total_alerts`, `escalated_alerts`
     - `max_forecast`
     - `is_active`

2. Call `/api/users/{user_key}/alerts` for a known S1 insider:

   - Verify:
     - `risk_meta` present
     - `risk_meta.analyst_notes` non-empty and sane
     - alerts contain full prong scores + ensemble_score.

3. Call `/api/alerts/{alert_id}` for one alert:

   - Make sure the modal still renders correctly with that payload.

If any of these are missing, fix backend. Do not let AG improvise the API.

---

## 4. Minimal JS proof-of-life (you, not AG)

Once backend is solid, you do tiny JS changes to prove the contracts before you unleash AG.

File: `src/ui/static/app.js`

1.	In app.js, where you fetch /api/users/{user_key}/alerts:
	•	Attach risk_meta to state:

```js
const res = await fetch(`/api/users/${userKey}/alerts`);
const data = await res.json();
state.currentAlerts = data.alerts;
state.currentUserRiskMeta = data.risk_meta;
```

2. Add a dumb Case Notes toggle:
•	In global state:

```js
state.showCaseNotes = false;
```

•	In user header:
```html
User: Dora Amelia Spears <button onclick="toggleCaseNotes()">Case Notes</button>
```

•	Implement toggleCaseNotes() to flip the boolean and call a render function.
•	At the top region, temporarily replace the existing heartbeat panel with:
```js
function renderHeroOrCaseNotes() {
    const hero = document.getElementById('heroPanel'); // existing heartbeat container
    if (!hero) return;

    if (!state.showCaseNotes) {
        hero.innerHTML = "<div>Hero placeholder – AG will build this.</div>";
    } else {
        const rm = state.currentUserRiskMeta;
        if (!rm || !rm.analyst_notes) {
            hero.innerHTML = "<div>No case notes for this user yet.</div>";
            return;
        }
        hero.innerHTML = `
            <div class="case-notes-header">
                <span>Case notes for ${document.getElementById('selectedUserKey').textContent}</span>
                <button onclick="toggleCaseNotes()">✕ Close</button>
            </div>
            <ul>
                ${rm.analyst_notes.map(n => `<li>${n.day}: ${n.message}</li>`).join('')}
            </ul>
        `;
    }
}
```
This doesn’t have to be pretty; it just proves data is flowing.


3. Build userTimelineData for the future graph:
	•	Inside renderUserAlerts() (or equivalent), after fetching alerts:

```js
state.userTimelineData = state.currentAlerts.map(a => ({
    day: a.day,
    rules_score: a.rules_score || 0,
    ml_score: a.ml_score || 0,
    anomaly_score: a.anomaly_score || 0,
    forecast_score: a.forecast_score || 0,
    ensemble_score: a.ensemble_score || 0,
    alert_id: a.alert_id,
    escalated: a.escalated
}));
console.log("Timeline data", state.userTimelineData);
```
You don’t need to draw the graph yourself. Just confirm the array looks correct.

Once those are working, stop editing app.js by hand. Backend is ready, JS state is in place, and AG can now be told exactly what to do.

---

## 5. Draft AG prompt snippets (for later)

These are **starting points**. Future-you / future-chat can refine them before sending to AG.

### 5.1 Hero Panel prompt

> I have a FastAPI backend exposing /api/users with this JSON shape:
```js
{
  "user_key": "das1320",
  "name": "Dora Amelia Spears",
  "user_role": "Technician",
  "total_alerts": 33,
  "escalated_alerts": 7,
  "severity_bucket": "high",
  "priority_score": 0.92,
  "max_forecast": 0.99,
  "is_active": true
}
```
>The existing dashboard has a “heartbeat” alert volume chart rendered inside a container with <canvas id="heartbeatCanvas">.
I want to replace that chart with a Hero Panel that shows a scrollable list of highest-risk users:
	•	sorted by severity_bucket (critical > high > medium > low) then priority_score descending,
	•	shows name, (user_key), a colored severity badge, total alerts, escalated alerts.

When I click a user in the hero, it must call the existing selectUser(user_key) function. Do not change the signature of selectUser.
Keep the overall layout and CSS tokens; only change the contents of the heartbeat panel and the JS that populates it.

### 5.2 User Risk Timeline prompt

> The endpoint /api/users/{user_key}/alerts returns this structure:
```js
{
  "user_key": "das1320",
  "user_name": "Dora Amelia Spears",
  "user_role": "Technician",
  "risk_meta": { ... },
  "alerts": [
    {
      "alert_id": "das1320_2010-07-09_rules",
      "day": "2010-07-09",
      "rules_score": 0.80,
      "ml_score": 0.00,
      "anomaly_score": 0.00,
      "forecast_score": 0.97,
      "ensemble_score": 0.86,
      "escalated": true
    }
  ]
}
```
> In app.js I already compute state.userTimelineData as:
```js
{ day, rules_score, ml_score, anomaly_score, forecast_score, ensemble_score, alert_id, escalated }
```
>I want you to:
	•	Replace the existing per-detector bottom tables with a multi-line sparkline chart for the selected user.
	•	X-axis: day (time).
	•	Y-axis: score in [0,1].
	•	Lines: rules_score, ml_score, anomaly_score, forecast_score, ensemble_score.
	•	Each alert day should be a clickable dot on the chart that calls loadAlertDetails(alert_id).

Keep the existing Alert Details modal and endpoints unchanged. Only modify app.js and the HTML in the user section as needed.

### 5.3 Case Notes toggle prompt

> In app.js I store state.currentUserRiskMeta, which includes:
```js
{
  "user_key": "das1320",
  "analyst_notes": [
    { "day": "2010-07-05", "kind": "forecast_spike", "message": "..." },
    { "day": "2010-07-09", "kind": "full_chain", "message": "..." }
  ]
}
```
>I want the user header to include a “Case Notes” link. When clicked:
	•	It sets state.showCaseNotes = true.
	•	The top Hero Panel area is replaced with a Case Notes panel for the selected user:
	•	Title: Case notes for <user_name>.
	•	List of day: message.
	•	A ✕ Close button that sets state.showCaseNotes = false and restores the Hero Panel.

The user risk timeline graph should remain visible below the panel.
Implement this by updating the state and render functions in app.js only, do not change any backend routes.