"""
Backend for Scenario-1 Insider Threat Dashboard.

This module does three things:
  - Loads raw alerts from run_loop (NDJSON)
  - Normalizes + scores them (ensemble, forecast watch windows)
  - Serves them over FastAPI endpoints for the UI
"""

import json
import uuid
import asyncio
from pathlib import Path
from typing import List, Dict, Any, Optional
from dataclasses import dataclass, field
from collections import defaultdict
from datetime import datetime, timedelta, date

from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
import uvicorn
import math

app = FastAPI(title="Insider Threat Detector + Forecaster")

# ------ ref1924 demo alerts file (last run 12/9) ------
# ALERTS_PATH = Path("out/mvp0/alerts_ndjson/alerts_demo_ref1924_window.ndjson")
#ALERTS_PATH = Path("out/mvp0/alerts_test_summaries/alerts.ndjson")

# full run_loop (last run 12/5)
ALERTS_PATH = Path("out/mvp0/alerts_ndjson/alerts.ndjson")

# --- Configuration ---
# ALERTS_PATH = Path("out/mvp0/alerts_ndjson/alerts_debug.ndjson")

# ALERTS_PATH = Path("out/mvp0/alerts_ndjson/alerts_demo_2010-07-14_plus.ndjson")
# ALERTS_PATH = Path("out/mvp0/alerts_ndjson_debug/alerts.ndjson")
# ALERTS_PATH = Path("data/debug/alerts_aug2010_debug.ndjson")
# ALERTS_PATH = Path("out/r5.2/alerts_full/alerts.ndjson")
# ALERTS_PATH = Path("out/mvp0/alerts_ndjson/alerts_demo.ndjson")
# ALERTS_PATH = Path("out/mvp0/alerts_ndjson/alerts_demo_2011-02-01_plus.ndjson")
# ALERTS_PATH = Path("out/mvp0/alerts_ndjson/alerts_demo_2011-02-01_plus.ndjson")
# ALERTS_PATH = Path("out/mvp0/alerts_ndjson/alerts_full_backup.ndjson")
# ALERTS_PATH = Path("out/mvp0/alerts_ndjson/old_runloop_alerts.ndjson")


TICK_SECONDS = 1.0  # seconds per day in simulation


# Minimum forecast_score to treat a user as "on the watchlist"
# Raised based on precheck stats: non-insider mean ≈ 0.63, insider mean ≈ 0.78.
FORECAST_WATCHLIST_THRESHOLD = 0.75

# Forecast horizon (in days) for Scenario-1 forecast model
FORECAST_HORIZON_DAYS = 7

# Sidebar filtering thresholds
ANOMALY_SIDEBAR_THR = 0.75   # anomaly must be this strong to matter on its own
ML_SIDEBAR_THR = 0.75        # "strong" ML signal for sidebar membership

# Logistic ensemble weights (from ensemble_study)
LOGIT_W0 = -3.634722125361524
LOGIT_W_RULES_FULL = 2.553178
LOGIT_W_RULES_NEAR = -1.092871
LOGIT_W_ML_MAX = 4.776795
LOGIT_W_ANOMALY_MAX = 1.240133
LOGIT_W_FORECAST_MAX = 1.117140


# --- Forecast Watch Type ---
@dataclass
class ForecastWatch:
    """
    One forecast watch window for a single user.

    This is a logical "state machine snapshot":
      - start_day: first day forecast_score crossed the watchlist threshold
      - expiry_day: start_day + FORECAST_HORIZON_DAYS - 1
      - max_score: max forecast_score within the window
      - resolved: True if an escalated alert happened within [start_day, expiry_day]
      - escalation_day: day of that escalation (if any)
      - lead_time_days: escalation_day - start_day, in days (if resolved)
    """
    user_key: str
    start_day: str
    expiry_day: str
    max_score: float
    resolved: bool = False
    escalation_day: Optional[str] = None
    lead_time_days: Optional[int] = None

@dataclass
class UserMeta:
    name: str
    role: str
    last_seen: str
    is_terminated: bool = False

@dataclass
class RiskMeta:
    """
    Per-user risk summary used to drive the hero panel, sidebar, and case notes.

    This is a concrete implementation of the RiskMeta concept from the UI refactor plan.
    """
    user_key: str = ""
    severity_bucket: str = "low"  # "low" | "medium" | "high" | "critical"
    priority_score: float = 0.0   # 0–1, used for sorting users by risk

    # Analyst-facing story for this user.
    analyst_notes: List[Dict[str, Any]] = field(default_factory=list)

    # Quick stats that are convenient for the UI.
    max_ensemble: float = 0.0
    max_forecast: Optional[float] = None
    first_forecast_day: Optional[str] = None
    first_escalation_day: Optional[str] = None
    termination_day: Optional[str] = None  # ISO "YYYY-MM-DD"

# --- State ---
class AppState:
    alerts: List[Dict[str, Any]] = []
    alerts_by_user: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    alerts_by_day: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    # Per-user forecast watch windows (rebuilt from alerts by a small state machine)
    # Key: user_key -> List[ForecastWatch]
    watch_windows_by_user: Dict[str, List[ForecastWatch]] = defaultdict(list)
    
    # LDAP Metadata: user_key -> UserMeta
    user_meta: Dict[str, UserMeta] = {}
    # Lightweight per-user risk meta (populated from alerts in later phases)
    risk_meta_by_user: Dict[str, RiskMeta] = defaultdict(RiskMeta)

    earliest_day: str = ""
    latest_day: str = ""
    current_day: str = ""

    paused: bool = False

STATE = AppState()

# --- RiskMeta aggregation helpers (Phase B1 from ui_refactor_plan.md) ---

def _severity_bucket_from_max(max_ensemble: float) -> str:
    """
    Map a user's max ensemble score to a severity bucket.

    Thresholds follow the example from the UI refactor plan:

      - critical: max_ensemble >= 0.90
      - high:     max_ensemble >= 0.75
      - medium:   max_ensemble >= 0.50
      - low:      otherwise
    """
    if max_ensemble >= 0.90:
        return "critical"
    if max_ensemble >= 0.75:
        return "high"
    if max_ensemble >= 0.50:
        return "medium"
    return "low"

def _severity_from_forecast(max_forecast: float) -> str:
    """
    Map a user's max forecast score to a severity bucket.

    Design choice (matching your examples):
      - critical: max_forecast >= 0.70   (e.g., 75% -> CRITICAL)
      - medium:   max_forecast >= 0.50   (e.g., 54% -> MEDIUM)
      - low:      otherwise
    """
    if max_forecast >= 0.70:
        return "critical"
    if max_forecast >= 0.50:
        return "medium"
    return "low"


def _severity_rank(sev: str) -> int:
    """Numeric rank so we can take the max of ensemble vs forecast severity."""
    return {"low": 0, "medium": 1, "high": 2, "critical": 3}.get(sev, 0)

def _build_analyst_notes_for_user(
    user_key: str,
    alerts: List[Dict[str, Any]],
    termination_day: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Build a small set of analyst_notes entries for a user from their alerts.

    Notes follow the patterns described in the UI refactor plan:

      - forecast_spike: first day forecast_score crosses ~0.6
      - near_miss:      rule near-miss fires
      - full_chain:     full Scenario-1 chain fires
      - post_departure_chain: post-departure chain fires

    In addition, we synthesize a high-level "summary" note that captures:
      - last alert day + severity
      - number of high/critical days and max ensemble score
      - whether the user has ever had an escalated alert
      - which prongs (rules / forecast / anomaly) have been active
    """
    notes: List[Dict[str, Any]] = []
    if not alerts:
        return notes

    # Always process in time order.
    alerts_sorted = sorted(alerts, key=lambda a: a.get("day", ""))

    # -----------------------------
    # Aggregated metrics (from old _build_analyst_notes)
    # -----------------------------
    last_alert = alerts_sorted[-1]
    last_day = last_alert.get("day")
    last_sev = last_alert.get("severity") or "low"

    # Unique days with high/critical severity
    high_days = {
        a.get("day")
        for a in alerts_sorted
        if (a.get("severity") in ("high", "critical"))
    }
    days_high_or_above = len({d for d in high_days if d})

    ever_escalated = any(a.get("escalated") for a in alerts_sorted)
    max_ensemble = max((a.get("ensemble_score") or 0.0) for a in alerts_sorted)

    has_rule = any((a.get("rule_hits") or []) for a in alerts_sorted)
    has_forecast = any(
        (a.get("forecast_score") or 0.0) >= FORECAST_WATCHLIST_THRESHOLD
        for a in alerts_sorted
    )
    has_anom = any(a.get("anomaly_score") is not None for a in alerts_sorted)

    # -----------------------------
    # Event-style notes (timeline)
    # -----------------------------
    saw_forecast_spike = False
    saw_near_miss = False
    saw_full_chain = False
    saw_post_dep = False

    seen_rule_events = set()

    for a in alerts_sorted:
        day = a.get("day")
        rule_hits = a.get("rule_hits") or []
        forecast_score = a.get("forecast_score")
        ensemble_score = a.get("ensemble_score", 0.0)
        rule_timeline = a.get("rule_timeline") or []

        # Forecast spike: first time forecast crosses ~0.6
        if (not saw_forecast_spike) and (forecast_score is not None) and (forecast_score >= 0.60):
            saw_forecast_spike = True
            notes.append(
                {
                    "day": day,
                    "kind": "forecast_spike",
                    "message": f"Forecast model first predicted elevated exfil risk ({forecast_score*100:.2f}% likelihood).",
                }
            )

        # Rule-based patterns from Scenario-1:
        if "s1_near_miss" in rule_hits and not saw_near_miss:
            saw_near_miss = True
            notes.append(
                {
                    "day": day,
                    "kind": "near_miss",
                    "message": "Possible preparation for data theft after-hours activity and USB use increased but no data-leak site access yet.",
                }
            )

        if "s1_chain" in rule_hits and not saw_full_chain:
            saw_full_chain = True
            notes.append(
                {
                    "day": day,
                    "kind": "full_chain",
                    "message": "Pre-departure data theft detected.",
                }
            )

        if "s1_chain_post_departure" in rule_hits and not saw_post_dep:
            saw_post_dep = True
            notes.append(
                {
                    "day": day,
                    "kind": "post_departure_chain",
                    "message": "Post-departure data theft detected.",
                }
            )

        # Ingest rule timeline events
        for event in rule_timeline:
            e_day = event.get("day")
            e_kind = event.get("kind")
            e_msg = event.get("message")

            if not (e_day and e_kind and e_msg):
                continue

            # Deduplicate
            key = (e_day, e_kind)
            if key in seen_rule_events:
                continue
            seen_rule_events.add(key)

            notes.append(
                {
                    "day": e_day,
                    "kind": f"rule_{e_kind}",
                    "message": e_msg,
                }
            )

    # -----------------------------
    # High-level summary note (old sidebar triage logic)
    # -----------------------------
    summary_parts: List[str] = []
    if last_day:
        summary_parts.append(f"Last Rule-based alert on {last_day} ({last_sev}).")

    if days_high_or_above > 0:
        summary_parts.append(
            f"{days_high_or_above} day(s) with high/critical alerts; max ensemble {max_ensemble:.2f}."
        )
    else:
        summary_parts.append(f"Max ensemble {max_ensemble:.2f} so far.")

    if ever_escalated:
        summary_parts.append("Has at least one escalated alert.")

    prongs: List[str] = []
    if has_rule:
        prongs.append("rule chain activity")
    if has_forecast:
        prongs.append("elevated forecast scores")
    if has_anom:
        prongs.append("anomalous behavior")
    if prongs:
        summary_parts.append("Signals: " + ", ".join(prongs) + ".")

    if summary_parts and last_day:
        notes.append(
            {
                "day": last_day,
                "kind": "summary",
                "message": " ".join(summary_parts),
            }
        )

    # Add termination note if applicable (kept commented-out unless you want it)
    # if termination_day:
    #     notes.append(
    #         {
    #             "day": termination_day,
    #             "kind": "termination",
    #             "message": "Employment ended.",
    #         }
    #     )

    # Ensure final analyst_notes is sorted ascending by day
    notes.sort(key=lambda x: x["day"])

    return notes


def _compute_risk_meta_from_alerts(user_key: str, alerts: List[Dict[str, Any]]) -> RiskMeta:
    """
    Compute RiskMeta for a user based on a specific set of alerts.
    
    Used by:
      - rebuild_risk_meta (global state at startup)
      - get_user_alerts (per-request, time-filtered state)
    """
    if not alerts:
        return RiskMeta(user_key=user_key)

    alerts_sorted = sorted(alerts, key=lambda a: a.get("day", ""))

    max_ensemble = max((a.get("ensemble_score") or 0.0) for a in alerts_sorted)
    max_forecast = max(
        (
            (a.get("forecast_score") or 0.0)
            for a in alerts_sorted
            if a.get("forecast_score") is not None
        ),
        default=0.0,
    )

    first_forecast_day: Optional[str] = None
    first_escalation_day: Optional[str] = None

    for a in alerts_sorted:
        day = a.get("day")

        if first_forecast_day is None:
            fs = a.get("forecast_score")
            if fs is not None and fs >= 0.60:
                first_forecast_day = day

        if first_escalation_day is None and a.get("escalated"):
            first_escalation_day = day

    # --- Severity + priority: ensemble + forecast ---
    # Ensemble-driven severity (rules + ML + anomaly + forecast via logistic)
    sev_ens = _severity_bucket_from_max(max_ensemble)

    # Forecast-only severity: let strong forecast risk upgrade the bucket
    sev_fore = "low"
    if max_forecast > 0.0:
        sev_fore = _severity_from_forecast(max_forecast)

    # Take the more severe of the two, WITH CONSTRAINT:
    # Only let forecast promote severity if ensemble shows at least some support (>= 0.3)
    severity_bucket = sev_ens
    if _severity_rank(sev_fore) > _severity_rank(sev_ens):
        # Forecast wants to upgrade severity
        if max_ensemble >= 0.3:
            # Ensemble shows at least MEDIUM evidence -> allow forecast to promote
            severity_bucket = sev_fore
        else:
            # Ensemble too weak -> cap forecast promotion at MEDIUM
            # This prevents pure forecast false positives from reaching CRITICAL
            severity_bucket = sev_fore if sev_fore != "critical" else "medium"

    # Priority: use whichever signal is stronger
    priority_score = max(max_ensemble, max_forecast or 0.0)

    # Populate termination_day from UserMeta
    termination_day: Optional[str] = None
    user_meta = STATE.user_meta.get(user_key)
    if user_meta and user_meta.is_terminated:
        termination_day = user_meta.last_seen

    analyst_notes = _build_analyst_notes_for_user(
        user_key,
        alerts_sorted,
        termination_day=termination_day,
    )

    return RiskMeta(
        user_key=user_key,
        severity_bucket=severity_bucket,
        priority_score=priority_score,
        analyst_notes=analyst_notes,
        max_ensemble=max_ensemble,
        max_forecast=max_forecast if max_forecast > 0.0 else None,
        first_forecast_day=first_forecast_day,
        first_escalation_day=first_escalation_day,
        termination_day=termination_day,
    )


def rebuild_risk_meta() -> None:
    """
    Build RiskMeta objects for all users from STATE.alerts_by_user.

    Backend implementation of Phase B1 in the UI refactor plan:
      - compute max_ensemble / max_forecast
      - severity_bucket  (from ensemble + forecast)
      - priority_score   (max of ensemble / forecast)
      - analyst_notes
      - first_forecast_day / first_escalation_day
    """
    STATE.risk_meta_by_user = defaultdict(RiskMeta)

    for user_key, alerts in STATE.alerts_by_user.items():
        rm = _compute_risk_meta_from_alerts(user_key, alerts)
        STATE.risk_meta_by_user[user_key] = rm

    print(f"[app] rebuild_risk_meta: built {len(STATE.risk_meta_by_user)} users")


def get_risk_meta_for_user(user_key: str) -> RiskMeta:
    """Helper for future API handlers / AG prompts."""
    return STATE.risk_meta_by_user.get(user_key, RiskMeta(user_key=user_key))


def get_all_risk_meta() -> List[RiskMeta]:
    """Return RiskMeta for all users as a list."""
    return [rm for _, rm in STATE.risk_meta_by_user.items()]

# --- Helpers ---
def next_day(day_str: str) -> str:
    """Increment YYYY-MM-DD by 1 day."""
    d = datetime.strptime(day_str, "%Y-%m-%d")
    d += timedelta(days=1)
    return d.strftime("%Y-%m-%d")

def _to_date(d: Any) -> Optional[date]:
    """Safely convert string/datetime/date to date object."""
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    if isinstance(d, str):
        try:
            return datetime.strptime(d[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None

def _check_terminated(current_day: Any, last_seen: Any) -> bool:
    """Return True if current_day > last_seen (comparing as dates)."""
    cd = _to_date(current_day)
    ls = _to_date(last_seen)
    if cd and ls:
        return cd > ls
    return False

def _read_release_tag() -> str:
    """Read release.txt from repo root."""
    try:
        return Path("release.txt").read_text().strip()
    except FileNotFoundError:
        return "r5.1"

def _daily_user_parquet(rel: str) -> Path:
    """Resolve daily_user.parquet path."""
    p = Path(f"out/{rel}/features_v2/daily_user/daily_user.parquet")
    if p.exists():
        return p
    return Path(f"out/{rel}/features_v1/daily_user/daily_user.parquet")

import duckdb

def fetch_window(user_key: str, center_day: str, window_days: int = 14) -> List[Dict[str, Any]]:
    """Fetch 14-day window for user centered on day."""
    rel = _read_release_tag()
    parquet_path = _daily_user_parquet(rel)
    
    if not parquet_path.exists():
        return []
        
    # Calculate start day: center_day - (window_days - 1)
    center_dt = datetime.strptime(center_day, "%Y-%m-%d")
    start_dt = center_dt - timedelta(days=window_days - 1)
    start_day = start_dt.strftime("%Y-%m-%d")
    
    # End day for query (exclusive): center_day + 1 day
    end_dt = center_dt + timedelta(days=1)
    end_day = end_dt.strftime("%Y-%m-%d")
    
    query = f"""
        SELECT * FROM '{parquet_path}'
        WHERE user_key = ?
          AND day >= ?
          AND day < ?
        ORDER BY day ASC
    """
    
    try:
        # Use a transient connection
        con = duckdb.connect(":memory:")
        rows = con.execute(query, [user_key, start_day, end_day]).fetchall()
        cols = [d[0] for d in con.description]
        con.close()
        
        result = []
        for row in rows:
            result.append(dict(zip(cols, row)))
        return result
    except Exception as e:
        print(f"Error fetching window: {e}")
        return []
    
def _logistic_prong_score(
    rules_full_flag: float,
    rules_near_flag: float,
    ml_max: float,
    anomaly_max: float,
    forecast_max: float,
) -> float:
    """Logistic ensemble over prong scores."""
    logit = (
        LOGIT_W0
        + LOGIT_W_RULES_FULL * rules_full_flag
        + LOGIT_W_RULES_NEAR * rules_near_flag
        + LOGIT_W_ML_MAX * ml_max
        + LOGIT_W_ANOMALY_MAX * anomaly_max
        + LOGIT_W_FORECAST_MAX * forecast_max
    )
    return 1.0 / (1.0 + math.exp(-logit))

def compute_ensemble(
    rule_hits: List[str],
    ml_score: float,
    anomaly_score: Optional[float] = None,
    anomaly_severity: Optional[str] = None,
    forecast_score: Optional[float] = None,
):
    """
    Scenario-1 ensemble logic (logistic oracle).

    - Logistic regression over:
        rules_full, rules_near, ml_max, anomaly_max, forecast_max
    - Full S1 chains still act as hard overrides.
    - Anomaly is just another feature in the logistic (no heroics).
    """
    # -----------------------------
    # 1) Build feature inputs
    # -----------------------------
    has_chain_post = "s1_chain_post_departure" in rule_hits
    has_chain = "s1_chain" in rule_hits
    has_near = "s1_near_miss" in rule_hits

    rules_full_flag = 1.0 if (has_chain or has_chain_post) else 0.0
    rules_near_flag = 1.0 if has_near else 0.0

    ml_max = float(ml_score)
    anom_max = float(anomaly_score or 0.0)
    fore_max = float(forecast_score or 0.0)

    # Core logistic probability
    p_ensemble = _logistic_prong_score(
        rules_full_flag,
        rules_near_flag,
        ml_max,
        anom_max,
        fore_max,
    )

    ensemble_score = p_ensemble
    escalated = False
    rule_case = "none"

    # -----------------------------
    # 2) Rule-dominated overrides
    # -----------------------------
    if has_chain_post:
        rule_case = "s1_chain_post_departure"
        ensemble_score = 1.0
        escalated = True
    elif has_chain:
        rule_case = "s1_chain"
        # Treat completed chain as very high risk; ensure we don't undershoot.
        ensemble_score = max(p_ensemble, 0.95)
        escalated = True
    elif has_near:
        rule_case = "s1_near_miss"
        # Near-miss: lean on logistic, but be willing to escalate when strong.
        if p_ensemble >= 0.6:
            escalated = True

    # -----------------------------
    # 3) Severity buckets
    # -----------------------------
    # Default severity by ensemble_score
    if has_chain_post or has_chain:
        severity = "critical"
    elif ensemble_score >= 0.70:
        severity = "critical"
    elif ensemble_score >= 0.50:
        severity = "high"
    elif ensemble_score >= 0.30:
        severity = "medium"
    else:
        severity = "low"

    escalated = severity in ("high", "critical")

    # -----------------------------
    # 4) Explanation metadata
    # -----------------------------
    anomaly_desc = "No anomaly score"
    if anomaly_score is not None:
        if anomaly_severity:
            anomaly_desc = f"Anomaly {anomaly_severity}: {anomaly_score:.4f}"
        else:
            anomaly_desc = f"Anomaly score: {anomaly_score:.4f}"

    forecast_desc = "No forecast score"
    if forecast_score is not None:
        forecast_desc = f"Forecast score: {forecast_score:.4f}"

    # Decide which prong drove the score for the human summary
    dominant = "ML"
    best_ml = ml_max
    best_anom = anom_max
    best_fore = fore_max

    if fore_max >= max(best_ml, best_anom):
        dominant = "forecast"
    elif anom_max >= max(best_ml, best_fore):
        dominant = "anomaly"

    driver = rule_case if rule_case != "none" else dominant

    explanation = {
        "ensemble_score": ensemble_score,
        "escalated": escalated,
        "severity": severity,
        "rule_hits": rule_hits,
        "components": {
            "rule": {
                "fired": bool(rule_hits),
                "hits": rule_hits,
                "weight": (
                    1.0
                    if rule_case in ("s1_chain_post_departure", "s1_chain")
                    else (0.5 if rule_case == "s1_near_miss" else 0.0)
                ),
                "contribution_description": f"Rule case: {rule_case}",
            },
            "ml": {
                "score": ml_score,
                "weight": 1.0,
                "contribution_description": f"Raw ML score: {ml_score:.4f}",
            },
            "anomaly": {
                "score": anomaly_score,
                "severity": anomaly_severity,
                "weight": 1.0 if anomaly_score is not None else 0.0,
                "contribution_description": anomaly_desc,
            },
            "forecast": {
                "score": forecast_score,
                "weight": 1.0 if forecast_score is not None else 0.0,
                "contribution_description": forecast_desc,
            },
        },
        "rule_case": rule_case,
        "human_readable_summary": (
            f"Ensemble {ensemble_score:.2f} "
            f"({'Escalated' if escalated else 'Non-escalated'}). "
            f"Driven by {driver}."
        ),
    }

    return ensemble_score, escalated, severity, explanation

def normalize_alert(raw_record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Normalize raw alert record (one detector per line) to canonical schema.

    Expected raw_record schema from run_loop:
      - day: YYYY-MM-DD
      - user_key: str
      - detector: "rules" | "anomaly" | "ml" | "loop"
      - reason: e.g. "rules:s1_chain_post_departure"
      - score: float (rules/anomaly only)
      - evidence: dict (rules only)
      - rules_score / anomaly_score on ml records (stub)

    We treat each raw record as one normalized alert and derive:
      - rule_hits   from rules.reason / evidence
      - ml_score    from score or rules_score (for now stub)
      - anomaly_score from score on anomaly alerts
    """
    user_key = raw_record.get("user_key", "unknown").lower()
    day = raw_record.get("day", "unknown")
    det = raw_record.get("detector", None)

    rule_hits: List[str] = []
    
    # Initialize scores from raw record (preserving context)
    ml_score = float(raw_record.get("ml_score") or 0.0)
    anomaly_score = float(raw_record.get("anomaly_score") or 0.0) if raw_record.get("anomaly_score") is not None else None
    forecast_score = float(raw_record.get("forecast_score") or 0.0) if raw_record.get("forecast_score") is not None else None
    rules_score = float(raw_record.get("rules_score") or 0.0)
    
    anomaly_severity: Optional[str] = None
    
    # Preserve rule timeline if present
    rule_timeline = raw_record.get("rule_timeline") or []

    # Rules detector: extract scenario hits from reason/evidence
    if det == "rules":
        reason = str(raw_record.get("reason", ""))
        if ":" in reason:
            _, suffix = reason.split(":", 1)
            rule_hits.append(suffix)
        elif reason:
            rule_hits.append(reason)

        evidence = raw_record.get("evidence", {}) or {}
        for k, v in evidence.items():
            if isinstance(v, bool) and v and k.startswith("s1_") and k not in rule_hits:
                rule_hits.append(k)

        # Rules detector score is the rules prong score for this alert
        rules_score = float(raw_record.get("score") or 0.0)

        # Do NOT zero out ml_score here; rely on what came in from raw_record.
        # If raw_record has no ml_score, it defaults to 0.0 above.

    # Anomaly detector: use score as anomaly_score and extract severity from evidence
    elif det == "anomaly":
        anomaly_score = float(raw_record.get("score") or 0.0)
        evidence = raw_record.get("evidence", {}) or {}
        anomaly_severity = evidence.get("severity")

    # ML detector (stub currently): try to read a score if present
    elif det == "ml":
        # If your real ml.check writes `score`, read it here.
        if "score" in raw_record:
            ml_score = float(raw_record.get("score") or 0.0)
        elif "rules_score" in raw_record:
            # Temporary: treat rules_score as ml_score until real ML wiring.
            ml_score = float(raw_record.get("rules_score") or 0.0)

        # Capture per-day max rules_score propagated by run_loop
        if "rules_score" in raw_record:
            rules_score = float(raw_record.get("rules_score") or 0.0)

    elif det == "forecast":
        if "score" in raw_record:
            forecast_score = float(raw_record.get("score") or 0.0)
    # Skip pure loop/heartbeat entries; caller already filters detector == "loop",
    # but this keeps things robust.
    else:
        # Unknown detector, keep defaults
        pass

    ensemble_score, escalated, severity, explanation = compute_ensemble(
        rule_hits,
        ml_score,
        anomaly_score,
        anomaly_severity,
        forecast_score,
    )

    # Propagate rule-based human summary if present
    rule_human_summary = raw_record.get("human_summary")

    return {
        "alert_id": str(uuid.uuid4()),
        "day": day,
        "user_key": user_key,
        "scenario_id": "SCENARIO_1",
        "rule_hits": rule_hits,
        "rules_score": rules_score,
        "ml_score": ml_score,
        "anomaly_score": anomaly_score,
        "forecast_score": forecast_score,
        "ensemble_score": ensemble_score,
        "severity": severity,
        "escalated": escalated,
        "ensemble_explanation": explanation,
        "rule_human_summary": rule_human_summary,
        "rule_timeline": rule_timeline,
    }


# --- Forecast Watch Window Helpers ---
def _build_watch_windows_for_user(
    user_key: str, alerts: List[Dict[str, Any]]
) -> List[ForecastWatch]:
    """
    Build forecast watch windows for a single user.

    Rules:
      - A window starts on the first day forecast_score >= FORECAST_WATCHLIST_THRESHOLD.
      - It expires after FORECAST_HORIZON_DAYS if no escalation.
      - If escalation happens within [start_day, expiry_day], mark resolved and compute lead_time_days.
      - If no escalation occurs by expiry_day, the window expires and is treated as an incorrect forecast.

    This reconstructs the forecast state machine purely from normalized alerts.
    """
    if not alerts:
        return []

    watches: List[ForecastWatch] = []

    # Sort alerts by day ascending
    sorted_alerts = sorted(alerts, key=lambda a: a["day"])
    current: Optional[ForecastWatch] = None

    for a in sorted_alerts:
        day_str = a["day"]
        d = datetime.strptime(day_str, "%Y-%m-%d")
        fs = a.get("forecast_score")
        escalated = bool(a.get("escalated"))

        # 1) If we have an active watch and we've moved past expiry with no resolution, close it
        if current is not None:
            expiry_dt = datetime.strptime(current.expiry_day, "%Y-%m-%d")
            if d > expiry_dt and not current.resolved:
                watches.append(current)
                current = None

        # 2) Forecast hit: start or update a watch
        if fs is not None and fs >= FORECAST_WATCHLIST_THRESHOLD:
            if current is None:
                start_dt = d
                expiry_dt = start_dt + timedelta(days=FORECAST_HORIZON_DAYS)
                current = ForecastWatch(
                    user_key=user_key,
                    start_day=start_dt.strftime("%Y-%m-%d"),
                    expiry_day=expiry_dt.strftime("%Y-%m-%d"),
                    max_score=float(fs),
                )
            else:
                current.max_score = max(current.max_score, float(fs))

        # 3) Escalation: if within current window, resolve it and compute lead time
        if current is not None and not current.resolved and escalated:
            start_dt = datetime.strptime(current.start_day, "%Y-%m-%d")
            expiry_dt = datetime.strptime(current.expiry_day, "%Y-%m-%d")
            if start_dt <= d <= expiry_dt:
                current.resolved = True
                current.escalation_day = day_str
                current.lead_time_days = (d - start_dt).days

    # Push last window if any (resolved or expired)
    if current is not None:
        watches.append(current)

    return watches


def compute_watch_windows() -> None:
    """
    Compute forecast watch windows for all users from STATE.alerts_by_user
    and store them in STATE.watch_windows_by_user.

    This is the forecast watchlist state machine for the current alerts snapshot.
    """
    STATE.watch_windows_by_user = defaultdict(list)
    for user_key, alerts in STATE.alerts_by_user.items():
        windows = _build_watch_windows_for_user(user_key, alerts)
        if windows:
            STATE.watch_windows_by_user[user_key] = windows

def load_ldap_metadata():
    """Load LDAP metadata from parquet."""
    path = Path("out/r5.2/ldap_v3_full/ldap_asof_by_month.parquet")
    if not path.exists():
        print(f"WARNING: LDAP data not found at {path}")
        return

    print(f"Loading LDAP metadata from {path}...")
    try:
        con = duckdb.connect(":memory:")
        # We want the latest entry for each user to get their most recent role/status
        # But actually the requirement says: "Build STATE.user_meta[user_key] = {name, role, last_seen}"
        # And "surface last_seen if STATE.current_day > last_seen. If current_day exceeds last_seen, mark user as Terminated."
        # So we just need the static map first.
        # Let's just select distinct user_key, employee_name, role, last_seen.
        # Since it's "asof_by_month", a user might appear multiple times. We probably want the latest one?
        # Or maybe just distinct on user_key order by last_seen desc.
        
        query = f"""
            SELECT user_key, employee_name, role, MAX(last_seen) as last_seen
            FROM '{path}'
            GROUP BY user_key, employee_name, role
            ORDER BY last_seen DESC
        """
        # Note: A user might change roles, so grouping by role might give duplicates.
        # Let's do a window function or just distinct on user_key in python.
        
        rows = con.execute(f"SELECT * FROM '{path}'").fetchall()
        cols = [d[0] for d in con.description]
        con.close()
        
        # Process in python to get latest per user
        temp_meta = {}
        for row in rows:
            r = dict(zip(cols, row))
            uk = r.get("user_key", "").lower()
            if not uk: continue
            
            # If we already have this user, check if this row is newer
            ls = r.get("last_seen", "0000-00-00")
            if uk in temp_meta:
                if ls > temp_meta[uk]["last_seen"]:
                    temp_meta[uk] = r
            else:
                temp_meta[uk] = r
        
        # Compute global max last_seen
        if temp_meta:
            # Ensure we compare comparable types. Convert all to string first if needed, or rely on duckdb types.
            # But let's just make sure we store strings in UserMeta.
            pass

        for uk, r in temp_meta.items():
            raw_ls = r.get("last_seen")
            if isinstance(raw_ls, datetime):
                ls = raw_ls.strftime("%Y-%m-%d")
            elif isinstance(raw_ls, date):
                ls = raw_ls.strftime("%Y-%m-%d")
            else:
                ls = str(raw_ls) if raw_ls else "9999-99-99"
                
            # Update r with string version for global max computation
            r["last_seen_str"] = ls

        # Compute global max from strings
        if temp_meta:
            global_max_last_seen = max(r["last_seen_str"] for r in temp_meta.values())
        else:
            global_max_last_seen = "9999-99-99"

        for uk, r in temp_meta.items():
            ls = r["last_seen_str"]
            # Terminated only if strictly earlier than global max
            is_terminated = (ls < global_max_last_seen)
            
            STATE.user_meta[uk] = UserMeta(
                name=r.get("employee_name", "Unknown"),
                role=r.get("role", "Unknown"),
                last_seen=ls,
                is_terminated=is_terminated
            )
            
        print(f"Loaded metadata for {len(STATE.user_meta)} users. Global max last_seen: {global_max_last_seen}")
        
    except Exception as e:
        print(f"Error loading LDAP metadata: {e}")

def recompute_risk_meta() -> None:
    """
    Rebuild per-user risk metadata from STATE.alerts_by_user.

    This is a lightweight summary used to guide priority and analyst notes.
    """
    for user_key, alerts in STATE.alerts_by_user.items():
        if not alerts:
            continue

        rm = STATE.risk_meta_by_user[user_key]

        # Distinct days with high/critical severity
        days_high = {
            a["day"]
            for a in alerts
            if a.get("severity") in ("high", "critical")
        }

        # Last alert by day
        last_alert = max(alerts, key=lambda a: a["day"])
        last_sev = last_alert.get("severity")

        max_ensemble = max(a["ensemble_score"] for a in alerts)
        ever_escalated = any(a.get("escalated") for a in alerts)

        rm.days_high_or_above = len(days_high)
        rm.last_severity = last_sev
        rm.max_ensemble = float(max_ensemble)
        rm.ever_escalated = bool(ever_escalated)

def load_data():
    """Load and process alerts."""
    STATE.alerts = []
    STATE.alerts_by_user = defaultdict(list)
    STATE.alerts_by_day = defaultdict(list)
    STATE.risk_meta_by_user = defaultdict(RiskMeta)
    
    if not ALERTS_PATH.exists():
        print(f"WARNING: {ALERTS_PATH} not found.")
        return

    print(f"Loading alerts from {ALERTS_PATH}...")
    min_day = "9999-99-99"
    max_day = "0000-00-00"
    
    with ALERTS_PATH.open("r") as f:
        for line in f:
            line = line.strip()
            if not line: continue
            try:
                raw = json.loads(line)
                if raw.get("detector") == "loop": continue # Skip heartbeats
                
                alert = normalize_alert(raw)
                STATE.alerts.append(alert)
                STATE.alerts_by_user[alert["user_key"]].append(alert)
                STATE.alerts_by_day[alert["day"]].append(alert)
                
                if alert["day"] > max_day:
                    max_day = alert["day"]
                if alert["day"] < min_day:
                    min_day = alert["day"]
            except json.JSONDecodeError:
                pass
    
    if min_day == "9999-99-99":
        min_day = datetime.now().strftime("%Y-%m-%d")
        max_day = min_day

    STATE.earliest_day = min_day
    STATE.latest_day = max_day
    STATE.current_day = min_day

    print(f"Loaded {len(STATE.alerts)} alerts.")
    print(f"Timeline: {STATE.earliest_day} -> {STATE.latest_day}")
    print(f"Starting at: {STATE.current_day}")

    load_ldap_metadata()

    # Build per-user RiskMeta (severity buckets, priority_score, analyst_notes, etc.).
    rebuild_risk_meta()

    # After loading alerts, compute forecast watch windows per user (forecast state machine).
    compute_watch_windows()
    print(f"Forecast watch windows users: {len(STATE.watch_windows_by_user)}")

    # Initialize risk meta entries for all users (fields will be populated in later phases)
    for uk in STATE.alerts_by_user.keys():
        _ = STATE.risk_meta_by_user[uk]

    # Populate RiskMeta from full alert history
    recompute_risk_meta()

async def tick_loop():
    """Background task to advance time."""
    while True:
        if not STATE.paused and STATE.current_day < STATE.latest_day:
            STATE.current_day = next_day(STATE.current_day)
            print(f"Tick: {STATE.current_day}")
        await asyncio.sleep(TICK_SECONDS)

@app.on_event("startup")
async def startup():
    load_data()
    asyncio.create_task(tick_loop())

# --- Endpoints ---
@app.get("/api/heartbeat")
async def get_heartbeat(
    days: int = 7,
    scope: str = "alerts",
    user_key: Optional[str] = None,
):
    """
    Return alert counts per day for the most recent N days (respecting current_day).

    Modes:
      - scope="alerts", user_key=None   -> global view (existing behavior)
      - scope="watchlist", user_key=None -> aggregate over watchlist users only
      - user_key="<user>"               -> per-user view
    """
    end_day = STATE.current_day

    # Get all unique days sorted and <= end_day
    all_days = sorted(STATE.alerts_by_day.keys())
    valid_days = [d for d in all_days if d <= end_day]
    window_days = valid_days[-days:] if days > 0 else valid_days

    # Per-user heartbeat mode
    if user_key is not None:
        user_key = user_key.lower()
        result = []
        for d in window_days:
            # Only this user's alerts on day d
            alerts = [a for a in STATE.alerts_by_day[d] if a["user_key"] == user_key]

            count = len(alerts)
            escalated_count = sum(1 for a in alerts if a["escalated"])
            rule_count = sum(1 for a in alerts if len(a["rule_hits"]) > 0)
            ml_count = sum(1 for a in alerts if a["ml_score"] > 0.5)
            anomaly_count = sum(
                1
                for a in alerts
                if a["anomaly_score"] is not None and a["anomaly_score"] > 0.5
            )
            forecast_max = max(
                (
                    (a.get("forecast_score") or 0.0)
                    for a in alerts
                    if a.get("forecast_score") is not None
                ),
                default=0.0,
            )

            result.append(
                {
                    "day": d,
                    "count": count,
                    "escalated_count": escalated_count,
                    "rule_count": rule_count,
                    "ml_count": ml_count,
                    "anomaly_count": anomaly_count,
                    "forecast_max": forecast_max,
                }
            )
        return result

    # Watchlist aggregate mode
    if scope == "watchlist":
        # Determine which users are currently on the watchlist (reuse same threshold)
        watchlist_users: set[str] = set()
        for u, alerts in STATE.alerts_by_user.items():
            visible_alerts = [a for a in alerts if a["day"] <= end_day]
            if not visible_alerts:
                continue
            max_forecast = max(
                (
                    (a.get("forecast_score") or 0.0)
                    for a in visible_alerts
                    if a.get("forecast_score") is not None
                ),
                default=0.0,
            )
            if max_forecast >= FORECAST_WATCHLIST_THRESHOLD:
                watchlist_users.add(u)

        result = []
        for d in window_days:
            day_alerts = STATE.alerts_by_day[d]
            alerts = [a for a in day_alerts if a["user_key"] in watchlist_users]

            count = len(alerts)
            escalated_count = sum(1 for a in alerts if a["escalated"])

            # Count how many distinct users had a forecast hit that day
            forecast_hit_users = {
                a["user_key"]
                for a in alerts
                if (a.get("forecast_score") or 0.0) >= FORECAST_WATCHLIST_THRESHOLD
            }
            forecast_hit_count = len(forecast_hit_users)

            result.append(
                {
                    "day": d,
                    "count": count,
                    "escalated_count": escalated_count,
                    "forecast_hit_count": forecast_hit_count,
                }
            )
        return result

    # Default: global Alerts mode (existing behavior)
    result = []
    for d in window_days:
        alerts = STATE.alerts_by_day[d]

        count = len(alerts)
        escalated_count = sum(1 for a in alerts if a["escalated"])

        rule_count = sum(1 for a in alerts if len(a["rule_hits"]) > 0)
        ml_count = sum(1 for a in alerts if a["ml_score"] > 0.5)
        anomaly_count = sum(
            1
            for a in alerts
            if a["anomaly_score"] is not None and a["anomaly_score"] > 0.5
        )

        result.append(
            {
                "day": d,
                "count": count,
                "escalated_count": escalated_count,
                "rule_count": rule_count,
                "ml_count": ml_count,
                "anomaly_count": anomaly_count,
            }
        )

    return result

def _build_analyst_notes(
    user_key: str,
    visible_alerts: List[Dict[str, Any]],
    rm: RiskMeta,
) -> str:
    """Short human-readable summary for sidebar triage."""
    if not visible_alerts:
        return ""

    # Last visible alert by day
    last_alert = max(visible_alerts, key=lambda a: a["day"])
    last_day = last_alert["day"]
    last_sev = last_alert.get("severity") or "low"

    has_rule = any(len(a["rule_hits"]) > 0 for a in visible_alerts)
    has_forecast = any(
        (a.get("forecast_score") or 0.0) >= FORECAST_WATCHLIST_THRESHOLD
        for a in visible_alerts
    )
    has_anom = any(a.get("anomaly_score") is not None for a in visible_alerts)

    parts: List[str] = []

    # Core timeline + severity
    parts.append(f"Last S1 alert on {last_day} ({last_sev}).")

    # History of badness
    if rm.days_high_or_above > 0:
        parts.append(
            f"{rm.days_high_or_above} day(s) with high/critical alerts; "
            f"max ensemble {rm.max_ensemble:.2f}."
        )
    else:
        parts.append(f"Max ensemble {rm.max_ensemble:.2f} so far.")

    # Escalation history
    if rm.ever_escalated:
        parts.append("Has at least one escalated alert.")

    # Prong summary
    prongs: List[str] = []
    if has_rule:
        prongs.append("rule chain activity")
    if has_forecast:
        prongs.append("elevated forecast scores")
    if has_anom:
        prongs.append("anomalous behavior")
    if prongs:
        parts.append("Signals: " + ", ".join(prongs) + ".")

    return " ".join(parts)

@app.get("/api/users")
async def get_users(tab: str = Query("all")):
    """
    Return users for the sidebar / hero panel.

    tab:
      - "all"       -> Alerts sidebar, all users who have alerts
      - "escalated" -> Alerts sidebar, escalated users only
      - "watchlist" -> Watchlist sidebar (forecast-driven)
    """
    end_day = STATE.current_day
    users: List[Dict[str, Any]] = []

    for user_key, alerts in STATE.alerts_by_user.items():
        # Only consider alerts up to the current simulated day
        visible_alerts = [a for a in alerts if a["day"] <= end_day]
        if not visible_alerts:
            continue

        total_alerts = len(visible_alerts)
        escalated_alerts = sum(1 for a in visible_alerts if a.get("escalated"))

        # Per-day ensemble / forecast maxima (ONLY from visible alerts)
        max_ensemble = max((a.get("ensemble_score") or 0.0) for a in visible_alerts)
        max_forecast = max(
            (
                (a.get("forecast_score") or 0.0)
                for a in visible_alerts
                if a.get("forecast_score") is not None
            ),
            default=0.0,
        )

        # Severity from *visible* ensemble
        sev_ens = _severity_bucket_from_max(max_ensemble)

        # Severity upgrade from *visible* forecast
        sev_fore = "low"
        if max_forecast > 0.0:
            sev_fore = _severity_from_forecast(max_forecast)

        # Take the more severe of ensemble vs forecast (for *this day*)
        severity_bucket = sev_ens
        if _severity_rank(sev_fore) > _severity_rank(sev_ens):
            severity_bucket = sev_fore

        # Priority: make strong forecast OR strong ensemble float to the top
        priority_score = max(max_ensemble, max_forecast or 0.0)

        # LDAP metadata (name, role, termination)
        meta = STATE.user_meta.get(user_key)
        name = meta.name if meta else user_key
        role = meta.role if meta else ""
        is_terminated = meta.is_terminated if meta else False
        termination_day = meta.last_seen if (meta and meta.is_terminated) else None

        row = {
            "user_key": user_key,
            "name": name,
            "user_role": role,
            "is_terminated": is_terminated,
            "termination_day": termination_day,
            "severity_bucket": severity_bucket,
            "priority_score": priority_score,
            "max_ensemble": max_ensemble,
            "max_forecast": max_forecast if max_forecast > 0.0 else None,
            "total_alerts": total_alerts,
            "escalated_alerts": escalated_alerts,
        }
        users.append(row)

    # Sidebar filters
    if tab == "escalated":
        users = [u for u in users if u["escalated_alerts"] > 0]
    elif tab == "watchlist":
        users = [
            u
            for u in users
            if (u["max_forecast"] or 0.0) >= FORECAST_WATCHLIST_THRESHOLD
        ]

    # Sort: severity (critical > high > medium > low), then priority_score
    severity_order = {"critical": 3, "high": 2, "medium": 1, "low": 0}
    users.sort(
        key=lambda u: (
            -(severity_order.get(u["severity_bucket"], 0)),
            -(u["priority_score"] or 0.0),
        )
    )

    return users

@app.get("/api/users/{user_key}/alerts")
async def get_user_alerts(user_key: str):
    """
    Return alerts and RiskMeta for a given user, respecting the current visible day.

    Response shape:

    {
      "user_key": "das1320",
      "user_name": "Dora Amelia Spears",
      "user_role": "Technician",
      "risk_meta": { ... },      # RiskMeta dict
      "alerts": [ { ... }, ... ] # normalized alerts
    }
    """
    uk = user_key.lower()

    if uk not in STATE.alerts_by_user:
        raise HTTPException(status_code=404, detail="User not found")

    end_day = STATE.current_day
    all_alerts = [
        a for a in STATE.alerts_by_user[uk]
        if a.get("day", "") <= end_day
    ]

    # No visible alerts yet
    if not all_alerts:
        meta = STATE.user_meta.get(uk)
        user_name = meta.name if meta else uk
        user_role = meta.role if meta else "Unknown"
        # Compute RiskMeta on the fly for empty alerts (returns empty/default RiskMeta)
        rm = _compute_risk_meta_from_alerts(uk, [])
        return {
            "user_key": uk,
            "user_name": user_name,
            "user_role": user_role,
            "risk_meta": rm.__dict__,
            "alerts": [],
        }

    # Group alerts by day and aggregate per-detector scores
    by_day: Dict[str, Dict[str, Any]] = {}
    severity_rank = {"low": 0, "medium": 1, "high": 2, "critical": 3}
    
    # Compute RiskMeta strictly from visible alerts
    rm = _compute_risk_meta_from_alerts(uk, all_alerts)

    for a in all_alerts:
        day = a["day"]
        existing = by_day.get(day)
        if existing is None:
            # Seed with this alert's values
            by_day[day] = {
                "day": day,
                "user_key": uk,
                "alert_id": a["alert_id"],
                "rule_hits": list(a.get("rule_hits") or []),
                "rules_score": float(a.get("rules_score") or 0.0),
                "ml_score": float(a.get("ml_score") or 0.0),
                "anomaly_score": a.get("anomaly_score"),
                "forecast_score": a.get("forecast_score"),
                "ensemble_score": float(a.get("ensemble_score") or 0.0),
                "severity": a.get("severity") or "low",
                "escalated": bool(a.get("escalated")),
                "after_termination": False,
            }
            
            # Check after_termination
            if rm.termination_day:
                # Strict greater than: alert day > termination day
                if day > rm.termination_day:
                    by_day[day]["after_termination"] = True

            continue

        # Merge this alert into the existing per-day record

        # 1) Union rule hits
        for h in a.get("rule_hits") or []:
            if h not in existing["rule_hits"]:
                existing["rule_hits"].append(h)

        # 2) Max per-detector scores
        existing["rules_score"] = max(
            existing["rules_score"], float(a.get("rules_score") or 0.0)
        )
        existing["ml_score"] = max(
            existing["ml_score"], float(a.get("ml_score") or 0.0)
        )

        anom = a.get("anomaly_score")
        if anom is not None:
            if existing["anomaly_score"] is None:
                existing["anomaly_score"] = float(anom)
            else:
                existing["anomaly_score"] = max(
                    float(existing["anomaly_score"]), float(anom)
                )

        fore = a.get("forecast_score")
        if fore is not None:
            if existing["forecast_score"] is None:
                existing["forecast_score"] = float(fore)
            else:
                existing["forecast_score"] = max(
                    float(existing["forecast_score"]), float(fore)
                )

        # 3) Ensemble score and primary alert_id for the day
        ens = float(a.get("ensemble_score") or 0.0)
        if ens > existing["ensemble_score"]:
            existing["ensemble_score"] = ens
            existing["alert_id"] = a["alert_id"]
            existing["severity"] = a.get("severity") or existing["severity"]

        # 4) Escalation + severity bucket
        existing["escalated"] = existing["escalated"] or bool(a.get("escalated"))
        sev = a.get("severity") or "low"
        if severity_rank.get(sev, 0) > severity_rank.get(existing["severity"], 0):
            existing["severity"] = sev

    # Turn the grouped dict into a sorted list by day
    day_records = [by_day[d] for d in sorted(by_day.keys())]

    # Attach LDAP metadata
    meta = STATE.user_meta.get(uk)
    user_name = meta.name if meta else uk
    user_role = meta.role if meta else "Unknown"

    rm = _compute_risk_meta_from_alerts(uk, all_alerts)

    return {
        "user_key": uk,
        "user_name": user_name,
        "user_role": user_role,
        "risk_meta": rm.__dict__,
        "alerts": day_records,
    }

@app.get("/api/alerts/{alert_id}")
async def get_alert(alert_id: str):
    """
    Return details for a single alert, but with ML/anomaly/forecast scores
    promoted to the per-day max for that user.

    This fixes cases where the "canonical" alert for the day is a rules
    record (ml_score = 0), even though an ML alert fired the same day.
    """
    base = None
    for a in STATE.alerts:
        if a["alert_id"] == alert_id:
            base = a
            break

    if base is None:
        raise HTTPException(status_code=404, detail="Alert not found")

    # Work on a copy so we don't mutate STATE.alerts in place
    alert = dict(base)
    user_key = alert.get("user_key")
    day = alert.get("day")

    # Look at *all* alerts for this user on this day
    day_alerts = [
        x for x in STATE.alerts_by_user.get(user_key, [])
        if x.get("day") == day
    ]

    if day_alerts:
        ml_max = max((x.get("ml_score") or 0.0) for x in day_alerts)
        anom_max = max(
            (x.get("anomaly_score") or 0.0)
            for x in day_alerts
            if x.get("anomaly_score") is not None
        ) if any(x.get("anomaly_score") is not None for x in day_alerts) else None
        fore_max = max(
            (x.get("forecast_score") or 0.0)
            for x in day_alerts
            if x.get("forecast_score") is not None
        ) if any(x.get("forecast_score") is not None for x in day_alerts) else None

        # Update top-level fields
        alert["ml_score"] = ml_max
        alert["anomaly_score"] = anom_max
        alert["forecast_score"] = fore_max

        # Patch the ensemble_explanation components so the modal cards use the
        # per-day max scores, but *do not* touch ensemble_score / severity.
        expl = alert.get("ensemble_explanation")
        if expl and isinstance(expl, dict):
            comps = expl.get("components") or {}
            if "ml" in comps:
                comps["ml"]["score"] = ml_max
            if "anomaly" in comps:
                comps["anomaly"]["score"] = anom_max
            if "forecast" in comps:
                comps["forecast"]["score"] = fore_max

    # Attach LDAP metadata for modal header
    meta = STATE.user_meta.get(user_key)
    alert["user_name"] = meta.name if meta else user_key
    alert["user_role"] = meta.role if meta else "Unknown"

    return alert

@app.get("/api/alerts/{alert_id}/window")
async def get_alert_window(alert_id: str):
    """Return 14-day feature window for the alert."""
    alert = None
    for a in STATE.alerts:
        if a["alert_id"] == alert_id:
            alert = a
            break
            
    if not alert:
        raise HTTPException(status_code=404, detail="Alert not found")
        
    rows = fetch_window(alert["user_key"], alert["day"], window_days=14)
    
    return {
        "alert_id": alert_id,
        "user_key": alert["user_key"],
        "center_day": alert["day"],
        "window_days": 14,
        "rows": rows
    }

class PauseRequest(BaseModel):
    paused: bool

@app.post("/api/state/pause")
async def set_pause(req: PauseRequest):
    STATE.paused = req.paused
    return {"paused": STATE.paused, "current_day": STATE.current_day}

@app.get("/api/state")
async def get_state():
    return {
        "paused": STATE.paused,
        "current_day": STATE.current_day,
        "earliest_day": STATE.earliest_day,
        "latest_day": STATE.latest_day
    }

@app.get("/api/forecast/summary")
async def get_forecast_summary():
    """
    Global forecast performance summary for Watchlist / forecast UI.

    Two views:
      1) Horizon-based (uses ForecastWatch windows):
           - total_forecasted_users
           - correct_forecasts
           - best_lead_time_days
           - median_lead_time_days

      2) Early-warning view (ignores the formal forecast horizon):
           - best_early_warning_lead_days
           - median_early_warning_lead_days
           - best_early_warning_user
           - best_early_warning_first_forecast
           - best_early_warning_escalation
    """
    end_day = STATE.current_day

    # -------------------------
    # 1) Horizon-based metrics (ForecastWatch state machine)
    # -------------------------
    per_user: Dict[str, Dict[str, Any]] = {}
    all_lead_times: List[int] = []

    best_window: Optional[ForecastWatch] = None
    best_user: Optional[str] = None
    best_window_lead: Optional[int] = None  # track visible lead for best_window

    for user_key, windows in STATE.watch_windows_by_user.items():
        # Only consider windows that start on or before the current visible day
        valid_windows = [w for w in windows if w.start_day <= end_day]
        if not valid_windows:
            continue

        # 1) Look for resolved windows whose escalation is actually visible (no time travel)
        resolved_visible: List[tuple[ForecastWatch, int]] = []
        for w in valid_windows:
            esc = w.escalation_day
            lt = w.lead_time_days
            if esc is None or lt is None:
                continue
            if esc > end_day:
                # Escalation happens after the current UI day → hide it
                continue
            if lt < 0:
                continue
            resolved_visible.append((w, int(lt)))

        if resolved_visible:
            # User has at least one correct forecast window.
            # Choose the one with the largest lead time for metrics.
            chosen, lt_int = max(resolved_visible, key=lambda t: t[1])
            per_user[user_key] = {
                "first_forecast": chosen.start_day,
                "first_escalation": chosen.escalation_day,
                "lead_time_days": lt_int,
            }
            all_lead_times.append(lt_int)

            if best_window_lead is None or lt_int > best_window_lead:
                best_window = chosen
                best_user = user_key
                best_window_lead = lt_int
        else:
            # No resolved window yet for this user (within visible time).
            # Still count them as "forecasted", but with no escalation / lead time.
            earliest = sorted(valid_windows, key=lambda w: w.start_day)[0]
            per_user[user_key] = {
                "first_forecast": earliest.start_day,
                "first_escalation": None,
                "lead_time_days": None,
            }
    total_forecasted_users = len(per_user)
    correct_forecasts = sum(
        1
        for s in per_user.values()
        if s["first_escalation"] is not None and s["lead_time_days"] is not None
    )

    if all_lead_times:
        sorted_times = sorted(all_lead_times)
        best_lead_time_days = sorted_times[-1]
        n = len(sorted_times)
        mid = n // 2
        if n % 2 == 1:
            median_lead_time_days = float(sorted_times[mid])
        else:
            median_lead_time_days = 0.5 * (sorted_times[mid - 1] + sorted_times[mid])
    else:
        best_lead_time_days = 0
        median_lead_time_days = 0.0

    if best_window is not None and best_window_lead is not None:
        best_first_forecast = best_window.start_day
        best_escalation = best_window.escalation_day
    else:
        best_first_forecast = None
        best_escalation = None
        best_user = None

    # -------------------------
    # 2) Early-warning metrics (ignore formal horizon, but still respect current_day)
    # -------------------------
    early_leads: List[int] = []
    best_early_lead: Optional[int] = None
    best_early_user: Optional[str] = None
    best_early_first_forecast: Optional[str] = None
    best_early_escalation: Optional[str] = None

    # Use the same threshold as the watchlist to define a "strong" forecast.
    forecast_thr = FORECAST_WATCHLIST_THRESHOLD

    for user_key, alerts_all in STATE.alerts_by_user.items():
        # Only consider alerts up to the visible day, so no time travel
        alerts = [a for a in alerts_all if a["day"] <= end_day]
        if not alerts:
            continue

        # Find earliest escalation day for this user
        esc_days = [a["day"] for a in alerts if a.get("escalated")]
        if not esc_days:
            continue
        esc_day = min(esc_days)
        esc_dt = datetime.strptime(esc_day, "%Y-%m-%d")

        # Find earliest forecast >= threshold on or before the escalation day
        candidate_forecasts = [
            a["day"]
            for a in alerts
            if (a.get("forecast_score") or 0.0) >= forecast_thr and a["day"] <= esc_day
        ]
        if not candidate_forecasts:
            continue

        first_forecast = min(candidate_forecasts)
        ff_dt = datetime.strptime(first_forecast, "%Y-%m-%d")
        lead = (esc_dt - ff_dt).days

        if lead < 0:
            continue

        early_leads.append(lead)

        if best_early_lead is None or lead > best_early_lead:
            best_early_lead = lead
            best_early_user = user_key
            best_early_first_forecast = first_forecast
            best_early_escalation = esc_day

    if early_leads:
        sorted_early = sorted(early_leads)
        best_early_warning_lead_days = int(sorted_early[-1])
        n = len(sorted_early)
        mid = n // 2
        if n % 2 == 1:
            median_early_warning_lead_days = float(sorted_early[mid])
        else:
            median_early_warning_lead_days = 0.5 * (
                sorted_early[mid - 1] + sorted_early[mid]
            )
    else:
        best_early_warning_lead_days = 0
        median_early_warning_lead_days = 0.0
        best_early_user = None
        best_early_first_forecast = None
        best_early_escalation = None

    return {
        # Horizon-based metrics (used by current UI)
        "total_forecasted_users": total_forecasted_users,
        "correct_forecasts": correct_forecasts,
        "best_lead_time_days": int(best_lead_time_days),
        "best_lead_time_user": best_user,
        "best_lead_time_first_forecast": best_first_forecast,
        "best_lead_time_escalation": best_escalation,
        "median_lead_time_days": median_lead_time_days,
        # Early-warning view
        "best_early_warning_lead_days": int(best_early_warning_lead_days),
        "best_early_warning_user": best_early_user,
        "best_early_warning_first_forecast": best_early_first_forecast,
        "best_early_warning_escalation": best_early_escalation,
        "median_early_warning_lead_days": median_early_warning_lead_days,
    }

# --- Static ---
app.mount("/static", StaticFiles(directory="src/ui/static"), name="static")

@app.get("/")
async def index():
    return FileResponse("src/ui/static/index.html")

if __name__ == "__main__":
    uvicorn.run("src.ui.app:app", host="127.0.0.1", port=8000, reload=True)
