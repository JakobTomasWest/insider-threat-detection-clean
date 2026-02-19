# UI Handshake (MVP0-PR4)

## Purpose
This document explains how to run the MVP0-PR4 web UI and API layer so teammates can safely test their detectors and visualize alerts without touching the core pipeline.

## Run Instructions

### 1. Generate alerts
- Refer to SETUP.md (#7 Running the detector loop and UI)

### 2. Start the FastAPI server
- Refer to SETUP.md (#8 Web UI)

### 3. What you’ll see
- **Top chart:** live heartbeat showing alert counts per day for each detector (rules, anomaly, ml)
- **Recent alerts table:** a list of the most recent alert entries from `alerts.ndjson`, refreshing every 5 seconds.

### 4. API Endpoints
| Endpoint | Description | Returns |
|-----------|--------------|----------|
| `/alerts/heartbeat` | Counts of alerts per day per detector | `{labels: [...], datasets: [...]}` |
| `/alerts/recent?n=200` | Most recent N alerts | `list[dict]` (each dict = alert record) |
| `/window/{user_key}?end_day=YYYY-MM-DD` | 14‑day feature window for a user | `{user_key, end_day, rows}` |

### 5. Detector Contract 
Each detector module exports:
```python
def check(window_df, day, user_key) -> list[dict]:
    # Input: DataFrame of up to 14 days for one user
    # Output: list of alert dicts with keys:
    # day, user_key, detector, reason, score, evidence
```
Returned alerts are automatically appended to the shared `alerts.ndjson` file and displayed in the UI.

### 7. Notes
- The UI auto‑refreshes every 5 seconds.
- Only the three detectors (rules, anomaly, ml) are charted.
- Parquet output can be added later at `out/<release>/alerts/alerts.parquet`.
- No authentication or persistent storage — this is a demo/test surface.
- Shortcuts: `make run-loop …`, `make ui`, or `source scripts/dev_cheats.sh` then `run_loop …`, `run_ui`.
