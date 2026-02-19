"""
Rule-based detector (Scenario 1) — MVP0 PR#3 (tightened).

Chain definition (strict, minimal):
  - After-hours activity signal in prior 7 days (≥ 0.5 on any *_after_hours_rate)
  - USB connect signal in prior 7 days
      * Prefer SAME-DAY AH+USB co-occurrence; otherwise require persistence (≥2 AH days AND ≥2 USB days in 7d)
  - Wikileaks HTTP in last 1 days (http_n_wikileaks > 0)  # S1 only

Rationale:
  The naive AH(any) + USB(any) lit up far too many users. We now require either
  same-day AH+USB OR stronger persistence (≥2 AH days AND ≥2 USB days).
"""

from __future__ import annotations
from typing import List, Dict
import pandas as pd


def _num(ser: pd.Series | None, default_len: int) -> pd.Series:
    """
    Coerce a possibly object-typed or missing series to float without
    triggering pandas' future downcasting warnings. Returns float64 series.
    """
    if ser is None:
        return pd.Series([0.0] * default_len, dtype="float64")
    out = pd.to_numeric(pd.Series(ser), errors="coerce").astype("float64")
    return out.fillna(0.0)

AH_RATE_THR = 0.5
MIN_EVENTS_PER_DOMAIN = 3
MIN_COMBINED_AH_EVENTS = 3
MIN_WINDOW_DAYS = 3

POST_DEP_EVENT_COLS = [
    "logon_n_events_post_departure",
    "device_n_events_post_departure",
    "file_n_events_post_departure",
    "http_n_events_post_departure",
    "email_n_events_post_departure",
]

AH_COLS = [
    "logon_after_hours_rate",
    "device_after_hours_rate",
    "file_after_hours_rate",
    "http_after_hours_rate",
    "email_after_hours_rate",
]
COUNT_COLS = {
    "logon_after_hours_rate":  "logon_n_logon",
    "device_after_hours_rate": "device_n_device_events",
    "file_after_hours_rate":   "file_n_file_events",
    "http_after_hours_rate":   "http_n_http",
    "email_after_hours_rate":  "email_n_email_sent",
}
EMAIL_AH_COUNT = "email_n_after_hours"

USB_COL = "device_n_usb_connects"
WL_COL  = "http_n_wikileaks"
WL_LOOKBACK_DAYS = 1

# V2 columns
V2_COLS = [
    "ah_rate_1d",
    "ah_rate_trend",
    "ah_rate_baseline",
    "usb_count_1d",
    "usb_count_trend",
    "usb_count_baseline",
    "ah_novel",
    "usb_novel",
]

def _clip_last_n_days(df: pd.DataFrame, n: int) -> pd.DataFrame:
    if "day" not in df.columns or df.empty:
        return df.tail(n)
    dser = pd.to_datetime(df["day"])
    last = dser.max()
    first = last - pd.Timedelta(days=n - 1)
    return df[(dser >= first) & (dser <= last)]

def _ah_signal_day(row: pd.Series) -> bool:
    est_ah = 0.0
    for rate_col in AH_COLS:
        if rate_col not in row.index:
            continue
        cnt_col = COUNT_COLS.get(rate_col)
        cnt = float(row.get(cnt_col, 0) or 0)
        rate = float(row.get(rate_col, 0) or 0)
        if rate_col == "email_after_hours_rate":
            email_ah = float(row.get(EMAIL_AH_COUNT, 0) or 0)
            est = email_ah if email_ah > 0 else rate * cnt
        else:
            est = rate * cnt
        if cnt >= MIN_EVENTS_PER_DOMAIN and rate >= AH_RATE_THR:
            est_ah += est
    return est_ah >= MIN_COMBINED_AH_EVENTS

def build_s1_human_summary(rule_case: str, evidence: dict) -> str:
    """
    Plain-language explanation for Scenario 1 (data theft) alerts.

    rule_case:
      - "s1_chain_post_departure"
      - "s1_chain"
      - "s1_near_miss"
      - "none"
    """
    ah_days = evidence.get("ah_sig_days_7d", 0)
    usb_days = evidence.get("usb_days_7d", 0)
    usb_total = evidence.get("usb_total_7d", 0)
    same_day = evidence.get("same_day_ah_usb_7d", 0)
    wl_days = evidence.get("wikileaks_days_N", 0)
    post_dep = evidence.get("post_departure_today", False)

    if rule_case == "s1_chain_post_departure":
        return (
            "Post-departure data theft pattern over the last 7 days. "
            "After-hours logins, USB activity, and traffic to a known data-leak site "
            "continue after the user left the organization."
        )

    if rule_case == "s1_chain":
        return (
            "Likely data theft over the last 7 days. "
            f"Heavy after-hours activity on {ah_days} day(s). "
            f"USB device use on {usb_days} day(s), {usb_total} total connections. "
            f"Same-day after-hours + USB on {same_day} day(s). "
            f"Traffic to a known data-leak site on {wl_days} day(s)."
        )

    if rule_case == "s1_near_miss":
        return (
            "Possible preparation for data theft. "
            "After-hours activity and USB use have increased in the last 7 days. "
            f"USB used {usb_total} times across {usb_days} day(s). "
            "No traffic to known data-leak sites has been observed yet."
        )

    # fallback (shouldn’t usually be hit)
    return (
        "Unusual pattern of after-hours access and USB use that may relate to data theft."
    )

def build_s1_rule_timeline(rule_case: str, last7: pd.DataFrame, evidence: dict) -> List[Dict]:
    """
    Builds a structured timeline of events for the last 7 days.
    """
    timeline = []
    seen = set()

    # Iterate over last7 rows in ascending day
    for _, row in last7.sort_values("day").iterrows():
        day_str = str(pd.to_datetime(row["day"]).date())
        
        # Check per-day flags
        if row.get("_ah_sig", False):
            key = (day_str, "after_hours")
            if key not in seen:
                timeline.append({
                    "day": day_str,
                    "kind": "after_hours",
                    "message": "After-hours activity above normal levels."
                })
                seen.add(key)

        usb_count = float(row.get(USB_COL, 0) or 0)
        if usb_count > 0:
            key = (day_str, "usb")
            if key not in seen:
                timeline.append({
                    "day": day_str,
                    "kind": "usb",
                    "message": f"USB device connected ({int(usb_count)} events) during this day."
                })
                seen.add(key)
        
        # _wl is on lastN, but we can check if this row has it if it's in last7
        # However, the prompt says "If _wl is true". _wl is computed on lastN.
        # But last7 is a subset of lastN usually (or vice versa depending on lookback).
        # The prompt says "Iterate over last7 rows... If _wl is true".
        # Let's check if _wl is in last7 columns.
        if "_wl" in row and row["_wl"]:
             key = (day_str, "leak_site")
             if key not in seen:
                 timeline.append({
                     "day": day_str,
                     "kind": "leak_site",
                     "message": "Traffic to a known data-leak site (wikileaks.org)."
                 })
                 seen.add(key)

    # Post-departure special event
    if rule_case == "s1_chain_post_departure":
        # Add a final event on evidence["window_end"] (or day)
        # The prompt says "evidence['window_end'] (or day)"
        # Let's use evidence["window_end"] if available, else the last day in timeline or today
        
        # We need a date for this event.
        # If we have evidence["window_end"], use that.
        evt_day = evidence.get("window_end")
        if evt_day:
             # Ensure it's just the date part YYYY-MM-DD
             evt_day = str(pd.to_datetime(evt_day).date())
        else:
             # Fallback to last day in last7
             evt_day = str(pd.to_datetime(last7["day"].max()).date())

        key = (evt_day, "post_departure_chain")
        if key not in seen:
            timeline.append({
                "day": evt_day,
                "kind": "post_departure_chain",
                "message": "Data-theft pattern continues after the user left the organization."
            })
            seen.add(key)
            
    return sorted(timeline, key=lambda x: x["day"])

def check(window_df: pd.DataFrame, *, day: str, user_key: str) -> List[Dict]:
    if window_df is None or len(window_df) == 0:
        return []

    df = window_df.copy()
    keep = ["day"] \
        + AH_COLS \
        + list(COUNT_COLS.values()) \
        + [EMAIL_AH_COUNT, USB_COL, WL_COL] \
        + V2_COLS \
        + POST_DEP_EVENT_COLS
    df = df[[c for c in keep if c in df.columns]].sort_values("day")
    if df.empty or "day" not in df.columns:
        return []

    last7 = _clip_last_n_days(df, 7).reset_index(drop=True)
    lastN = _clip_last_n_days(df, WL_LOOKBACK_DAYS).reset_index(drop=True)

    if last7["day"].nunique() < MIN_WINDOW_DAYS:
      return []

    # Per-day flags
    last7["_ah_sig"] = last7.apply(_ah_signal_day, axis=1)           # any domain AH rate >= 0.5
    _usb_ser = _num(last7.get(USB_COL), len(last7))
    last7["_usb"] = _usb_ser > 0
    
    # We need _wl on last7 for the timeline if we iterate last7.
    # The original code computed _wl on lastN.
    # Let's compute _wl on last7 as well for the timeline.
    _wl_ser_7 = _num(last7.get(WL_COL), len(last7))
    last7["_wl"] = _wl_ser_7 > 0

    _wl_ser = _num(lastN.get(WL_COL), len(lastN))
    lastN["_wl"] = _wl_ser > 0
    wl_today = bool(int(lastN["_wl"].sum()) > 0)

    # Co-occurrence and persistence
    same_day_ah_usb = int((last7["_ah_sig"] & last7["_usb"]).sum())
    ah_days_7d      = int(last7["_ah_sig"].sum())
    usb_days_7d     = int(last7["_usb"].sum())
    usb_total_7d    = int(_num(last7.get(USB_COL), len(last7)).sum())
    wl_days_N = int(lastN["_wl"].sum())

    # Novelty features
    if "ah_novel" in last7:
        ah_novel_7d = int(_num(last7.get("ah_novel"), len(last7)).sum())
    else:
        ah_novel_7d = 0

    if "usb_novel" in last7:
        usb_novel_7d = int(_num(last7.get("usb_novel"), len(last7)).sum())
    else:
        usb_novel_7d = 0

    novel_ah_usb = (ah_novel_7d > 0) and (usb_novel_7d > 0)

    # Host requirement (S1)
    has_wl_host = wl_today

    # Strengthened AH+USB gate:
    #   same-day overlap OR persistence on BOTH sides (>=2 AH days AND >=2 USB days)
    strong_ah_usb = (same_day_ah_usb > 0) or ((ah_days_7d >= 2) and (usb_days_7d >= 2))

    MIN_USB_7D = 4
    last_day = last7.iloc[-1]
    latest_usb = int(_num(last7.get(USB_COL), len(last7)).iloc[-1])

    # Post-departure: any domain logging events marked post_departure today
    post_departure_events_today = 0.0
    for col in POST_DEP_EVENT_COLS:
        if col in last_day.index:
            post_departure_events_today += float(last_day.get(col, 0.0) or 0.0)
    is_post_departure_today = post_departure_events_today > 0.0

    has_chain = strong_ah_usb and novel_ah_usb and has_wl_host
    has_chain_post_departure = has_chain and is_post_departure_today

    near_miss = (
        strong_ah_usb
        and novel_ah_usb
        and (usb_total_7d >= MIN_USB_7D)
        and (latest_usb > 0)
        and not has_wl_host
    )

    evidence = {
        "ah_sig_days_7d": ah_days_7d,
        "usb_days_7d": usb_days_7d,
        "usb_total_7d": usb_total_7d,
        "same_day_ah_usb_7d": same_day_ah_usb,
        "wikileaks_days_N": wl_days_N,
        "wl_lookback_days": WL_LOOKBACK_DAYS,
        "window_start": str(pd.to_datetime(df["day"]).min()),
        "window_end": str(pd.to_datetime(df["day"]).max()),
        "min_window_days": MIN_WINDOW_DAYS,
        "ah_method": "combined(rate*count, domain mins)",
        "ah_novel_7d": ah_novel_7d,
        "usb_novel_7d": usb_novel_7d,
        "post_departure_today": bool(is_post_departure_today),
        "post_departure_events_today": float(post_departure_events_today),
    }

    # Decide which S1 rule case (if any) fired
    rule_case = "none"
    if has_chain_post_departure:
        rule_case = "s1_chain_post_departure"
    elif has_chain:
        rule_case = "s1_chain"
    elif near_miss:
        rule_case = "s1_near_miss"

    # Build a plain-language summary if any rule fired
    human_summary = None
    rule_timeline = []
    if rule_case != "none":
        human_summary = build_s1_human_summary(rule_case, evidence)
        rule_timeline = build_s1_rule_timeline(rule_case, last7, evidence)

    alerts: List[Dict] = []
    if has_chain_post_departure:
        alerts.append({
            "day": day,
            "user_key": user_key,
            "detector": "rules",
            "reason": "rules:s1_chain_post_departure",
            "score": 0.95,
            "evidence": evidence,
            "human_summary": human_summary,
            "rule_timeline": rule_timeline,
        })
    elif has_chain:
        alerts.append({
            "day": day,
            "user_key": user_key,
            "detector": "rules",
            "reason": "rules:s1_chain",
            "score": 0.8,
            "evidence": evidence,
            "human_summary": human_summary,
            "rule_timeline": rule_timeline,
        })
    elif near_miss:
        alerts.append({
            "day": day,
            "user_key": user_key,
            "detector": "rules",
            "reason": "rules:s1_near_miss",
            "score": 0.3,
            "evidence": evidence,
            "human_summary": human_summary,
            "rule_timeline": rule_timeline,
        })

    return alerts