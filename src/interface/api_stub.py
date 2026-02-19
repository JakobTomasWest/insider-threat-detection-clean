from fastapi import FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from pathlib import Path
import json
from collections import defaultdict
import duckdb

app = FastAPI(title="MVP0 Heartbeat UI")

TEMPLATE = Path(__file__).parent / "templates" / "index.html"
ALERTS = Path("out/mvp0/alerts_ndjson/alerts.ndjson")
REL = Path("release.txt").read_text().strip() if Path("release.txt").exists() else "r5.1"
DAILY = Path(f"out/{REL}/features_v2/daily_user/daily_user.parquet")

@app.get("/", response_class=HTMLResponse)
def home(_: Request):
    return TEMPLATE.read_text()

@app.get("/alerts/heartbeat")
def heartbeat():
    counts = defaultdict(lambda: defaultdict(int))
    ALLOWED = {"rules", "anomaly", "ml"}  # keep chart clean

    if ALERTS.exists():
        with ALERTS.open() as f:
            for line in f:
                try:
                    a = json.loads(line)
                    d = a.get("day")
                    det = a.get("detector")
                    if d and det and det in ALLOWED:
                        counts[det][d] += 1
                except Exception:
                    continue

    days = sorted({d for detmap in counts.values() for d in detmap.keys()})
    # show a recent window so the axis doesn't get crushed
    days = days[-60:]

    # stable legend order
    order = ["rules", "anomaly", "ml"]
    datasets = []
    for det in order:
        dmap = counts.get(det, {})
        datasets.append({
            "label": det,
            "data": [dmap.get(day, 0) for day in days]
        })
    return {"labels": days, "datasets": datasets}

@app.get("/alerts/recent")
def recent(n: int = 200):
    """Return the last n alerts (filtered to known detectors)."""
    out = []
    if ALERTS.exists():
        with ALERTS.open() as f:
            for line in f:
                try:
                    a = json.loads(line)
                    if a.get("detector") in {"rules", "anomaly", "ml"}:
                        out.append(a)
                except Exception:
                    continue
    return out[-n:]

@app.get("/window/{user_key}")
def window(user_key: str, end_day: str):  # end_day = "YYYY-MM-DD"
    """Return the 14-day window rows for a user ending on end_day."""
    if not DAILY.exists():
        return {"user_key": user_key.lower(), "end_day": end_day, "rows": []}
    con = duckdb.connect(database=":memory:")
    try:
        con.execute("PRAGMA threads=auto")
    except Exception:
        pass
    q = f"""
      SELECT *
      FROM read_parquet('{DAILY.as_posix()}')
      WHERE lower(user_key) = lower('{user_key}')
        AND day >= TIMESTAMP '{end_day} 00:00:00' - INTERVAL 13 DAY
        AND day <  TIMESTAMP '{end_day} 00:00:00' + INTERVAL 1 DAY
      ORDER BY day
    """
    rows = con.execute(q).fetch_arrow_table().to_pylist()
    keep = {"day","logon_after_hours_rate","device_after_hours_rate",
            "file_after_hours_rate","http_after_hours_rate","email_after_hours_rate",
            "device_n_usb_connects","http_n_wikileaks"}
    rows = [{k:v for k,v in r.items() if k in keep} for r in rows]
    return {"user_key": user_key.lower(), "end_day": end_day, "rows": rows}