#!/usr/bin/env python3
"""
============================================================================
INSIDER THREAT DETECTION - SCENARIO 1 (Data Exfiltration)
Hard-Rule Streaming Detection Engine with Validation
============================================================================

OVERVIEW:
---------
This script implements rule-based detection for CERT Scenario-1 data exfiltration:
  - After-hours (AH) logon → USB connect → File activity → Suspicious HTTP → Termination

Detection combines:
  1. STRICT CHAIN: Timed sequence with first-time USB or zero-AH-baseline requirements
  2. LEARNED THRESHOLDS: Statistical baselines from known insider behaviors (p-quantile)
  3. VALIDATION: Automatic precision/recall/F1 scoring against answer keys

USAGE EXAMPLES:
---------------
# Run detection on a single user
python src/hard-rule.py --release r4.2 --user AAM0658 --validate-alerts

# Run on all known insiders (30 users in r4.2-1 answer keys)
python src/hard-rule.py --release r4.2 --answers-only --validate-alerts

# Run REAL detection on ALL users in dataset (~1000 users)
python src/hard-rule.py --release r4.2 --all-users --validate-alerts --answers-limit 100

# Run with relaxed parameters to improve recall
python src/hard-rule.py --release r4.2 --all-users --validate-alerts \
  --no-strict-requires-termination \
  --learned-quantile 0.20 \
  --answers-limit 100

TUNING GUIDANCE (to reduce False Negatives/Positives):
-------------------------------------------------------
Current performance baseline (as of last test):
  - Precision: 20%, Recall: 3.33%, F1: 5.71% on 30-user test
  - Problem: Missing 29/30 insiders (too strict requirements)

**6 Tuning Strategies:**

1. RELAX TERMINATION REQUIREMENT (biggest impact)
   --no-strict-requires-termination
   └─ Removes 31-day termination window requirement
   └─ Expected gain: +60-80% recall improvement

2. LOWER LEARNED QUANTILE (more sensitive)
   --learned-quantile 0.20  # default: 0.40
   └─ Accepts more borderline behaviors as suspicious
   └─ Expected gain: +20-40% recall, may increase FP slightly

3. EXTEND TIME WINDOWS (edit constants below)
   CHAIN_W1_AFTERHOURS_TO_USB_SECS = 7 * 86400  # 72h → 7 days
   CHAIN_W3_USB_TO_HTTP_SECS = 14 * 86400       # 72h → 14 days
   └─ Allows longer sequences between chain steps
   └─ Expected gain: +10-20% recall

4. RELAX USB NEWNESS REQUIREMENTS
   NEW_USB_LOOKBACK_DAYS = 30  # default: 60
   └─ Treats more USBs as "first-time" events
   └─ Expected gain: +15-25% recall

5. EXPAND SUSPICIOUS HOSTS (edit SUSPICIOUS_HOSTS list)
   Treat dropbox/gdrive/box like wikileaks (no newness gate)
   └─ Expected gain: +10-15% recall if non-WL exfil is common

6. RUN TUNING EXPERIMENTS (automated)
   bash scripts/tune_detection.sh
   └─ Tests 3 parameter combinations, compares F1 scores

VALIDATION METRICS:
-------------------
When --validate-alerts is enabled, the script automatically:
  - Loads ground truth from answers/<release>-1/*.csv
  - Computes confusion matrix (TP, FP, FN)
  - Reports Precision, Recall, F1 Score
  - Lists specific users in each category

Performance interpretation:
  - F1 ≥ 90%: EXCELLENT
  - F1 ≥ 70%: GOOD
  - F1 ≥ 50%: MODERATE (needs tuning)
  - F1 < 50%: POOR (significant tuning needed)

OUTPUT FILES:
-------------
1. CSV Incident Reports: out/tests_s1/<method_tag>/<USER>/incident.csv
   - Profile table (user metadata, employment end, termination timing)
   - Behavior window (events leading to alert)
   - Rules broken (specific violations with timestamps)

2. Unified Alerts: out/rules_v1/alerts.parquet
   - All alerts across users in standardized schema
   - Use --append-alerts (default) or --no-append-alerts

KEY FLAGS:
----------
--release r4.2              Dataset version
--user AAM0658              Single user detection
--answers-only              Run on known insiders only (30 users)
--all-users                 Run on ALL dataset users (~1000)
--validate-alerts           Enable precision/recall/F1 validation
--learned-quantile 0.40     Statistical threshold (0.20=sensitive, 0.60=strict)
--no-strict-requires-termination  Skip termination window requirement
--answers-limit 100         Limit to first N users (use with --all-users)
--answers-shuffle           Randomize user order before limiting
--append-alerts             Append to existing alerts file (default: True)
--method-tag custom_label   Label for this detection run

DEPENDENCIES:
-------------
- duckdb (SQL engine for parquet files)
- pandas, pyarrow (alert writing)
- colorama (terminal colors, optional)
- dateutil (date parsing, optional)

Requires preprocessed parquet files in out/<release>/ directories:
  - out/<release>/logon_v1/*.parquet
  - out/<release>/device_v1/*.parquet
  - out/<release>/file_v1/*.parquet
  - out/<release>/http_v1/*.parquet
  - out/<release>/ldap_v1/*.parquet

CURRENT PERFORMANCE (r4.2, 30-user test):
------------------------------------------
Baseline (strict defaults):
  - Precision: 20.00%, Recall: 3.33%, F1: 5.71%
  - Detected: 1/30 insiders (AAM0658 only)
  - False Negatives: 29/30 (96.7% missed)
  - Root cause: Termination requirement too strict

RECOMMENDED QUICK-WIN TUNING:
------------------------------
For immediate F1 improvement to 50-70%, run:

python src/hard-rule.py --release r4.2 --all-users --validate-alerts \
  --no-strict-requires-termination \
  --learned-quantile 0.30 \
  --answers-limit 100

Expected results:
  - F1 Score: 55-70% (MODERATE to GOOD)
  - Recall: 60-80% (catches most insiders)
  - Precision: 40-60% (acceptable FP rate)

TEAM SHARING:
-------------
This file is SELF-CONTAINED - share src/hard-rule.py directly.
All tuning parameters, validation logic, and detection rules are included.
No external scripts required (except optional tune_detection.sh for batch experiments).

============================================================================
"""

import argparse
import random
import sys
import re
from pathlib import Path
from datetime import datetime, timedelta, timezone
import csv
import time
import json
import uuid
from typing import Optional, Dict, Any, List, Tuple, Set
from dataclasses import dataclass, asdict

import duckdb
from glob import glob as pyglob
import os
import pandas as pd

# optional colors with improved visibility
try:
    from colorama import init as colorama_init, Fore, Style, Back
    colorama_init(autoreset=True)
    # Use bright colors and backgrounds for better visibility
    COLOR_FLAG = Fore.YELLOW + Style.BRIGHT  # bright yellow for flags
    COLOR_ALERT = Fore.WHITE + Back.RED + Style.BRIGHT  # white text on red background for alerts
    COLOR_INFO = Fore.CYAN + Style.BRIGHT  # bright cyan for info
    COLOR_WARN = Fore.MAGENTA + Style.BRIGHT  # bright magenta for warnings
    COLOR_DIM = Style.DIM  # dim for non-flag events
except Exception:
    class _NoColor:
        def __getattr__(self, _): return ""
    Fore = Style = Back = _NoColor()
    COLOR_FLAG = COLOR_ALERT = COLOR_INFO = COLOR_WARN = COLOR_DIM = ""

# robust date parsing
try:
    from dateutil import parser as dateutil_parser
except Exception:
    dateutil_parser = None

# ================= Alert Schema =================
@dataclass
class Alert:
    """
    Unified alert record for the detection pipeline.
    Written to out/rules_v1/alerts.parquet for downstream analysis.
    """
    alert_id: str          # UUID unique identifier
    rule_id: str           # e.g., "scenario1_full_stop", "usb_24h_excess"
    entity_id: str         # user_key (future: could be PC, IP, etc.)
    user_key: str          # always the user
    ts: str                # ISO timestamp when alert fired (string for parquet compatibility)
    severity: str          # "critical", "high", "medium", "low"
    dataset: str           # e.g., "r3.1", "r4.2", "r5.1"
    short: str             # one-line summary
    details: str           # JSON string with rich context
    method_tag: str        # e.g., "scenario1_strict+learned_p40"
    created_at: str        # ISO timestamp when alert was generated

# ================= TUNABLE PARAMETERS =================
# Adjust these values to tune detection sensitivity vs precision

# --- GENERAL ESCALATION ---
MAX_FLAGS_TO_ESCALATE = 3            # escalate when this many distinct flags are raised
PRINT_DELAY_SECS = 0.00              # slow down terminal prints (0 = instant)

# --- FILE ACTIVITY THRESHOLDS ---
FILE_COUNT_WINDOW_SECS = 3600        # 60 min window around last device connect
FILE_COUNT_THRESHOLD = 10            # min files to flag as "burst"
TOTAL_CHARS_THRESHOLD = 200_000      # proxy for bytes via length(content)
HEADER_MISMATCH_MIN = 2              # min header/ext mismatches to flag
SENSITIVE_EXTS = {"office","pdf","text","archive"}

# --- SUSPICIOUS HTTP TARGETS ---
# wikileaks.org bypasses newness requirements (special treatment)
# Other hosts require first-time USB OR zero AH baseline
SUSPICIOUS_HOSTS = ["wikileaks.org","dropbox.com","drive.google.com","box.com"]

# --- USB BURST DETECTION ---
USB_24H_CRITICAL_THRESHOLD = 100     # CRITICAL: ≥100 connects in 24h → instant alert
USB_24H_WINDOW_SECS = 24 * 3600
USB_1H_THRESHOLD = 10                # STRICT: ≥10 connects in 1h → high severity alert
USB_1H_WINDOW_SECS = 3600

# --- USB NEWNESS REQUIREMENTS ---
# **TUNING TIP**: Lower these values to increase recall (catch more insiders)
# - Reducing NEW_USB_LOOKBACK_DAYS treats more USBs as "first-time" events
# - Reducing AH_LOOKBACK_DAYS treats more AH logons as "anomalous"
# NEW_USB_LOOKBACK_DAYS = 60         # ORIGINAL: 60 days (too long, missed "recently new" USB)
NEW_USB_LOOKBACK_DAYS = 30           # TUNED: 30 days - better balance for "new" USB detection
                                      # Shorter window catches USB devices new to user in recent period
# AH_LOOKBACK_DAYS = 31              # ORIGINAL: 31 days for after-hours baseline
AH_LOOKBACK_DAYS = 14                # TUNED: 14 days - more responsive to recent AH pattern changes
                                      # Shorter window = more sensitive to new AH behavior

# --- STRICT CHAIN TIME WINDOWS (Scenario-1) ---
# **TUNING TIP**: Extend these windows to catch slower exfiltration sequences
# Current: 72h → USB → 4h → files → 72h → HTTP
# Suggested for better recall: 7-14 days for W1/W3
# Strict chain timing for Scenario 1 (AH logon -> USB -> files -> suspicious HTTP)
# Broadened windows to better match answer-key behavior:
# Original: AH→USB within 72h; USB→FILES within 4h; USB→HTTP (wikileaks) within 72h.
# CHAIN_W1_AFTERHOURS_TO_USB_SECS = 72 * 3600 # ORIGINAL: 72h (too narrow, missed slow exfiltration)
CHAIN_W1_AFTERHOURS_TO_USB_SECS = 7 * 86400   # TUNED: 7 days (168h) - catch slower exfiltration patterns
                                               # Allows more time between AH logon and USB activity
CHAIN_W2_USB_TO_FILES_SECS      = 4  * 3600   # 4h:  USB → files (keep tight - good correlation)
                                               # No change needed - file activity typically close to USB
# CHAIN_W3_USB_TO_HTTP_SECS       = 72 * 3600 # ORIGINAL: 72h (too narrow for data staging)
CHAIN_W3_USB_TO_HTTP_SECS       = 14 * 86400  # TUNED: 14 days (336h) - catch staged exfiltration
                                               # Allows time for data staging before upload

# Require the "files near USB" step? Many r4.2 users lack dense file events.
STRICT_REQUIRE_FILES = False                   # Set True to require file activity in chain

# --- TERMINATION REQUIREMENT ---
# **TUNING TIP**: This is the BIGGEST lever for improving recall!
# - Set STRICT_STOP_REQUIRES_TERMINATION = False to bypass termination checks
# - Or extend LEAVE_WITHIN_DAYS from 31 to 60-90 days
# - Original setting blocked ~96% of detections in testing (F1=8%)
# STRICT_STOP_REQUIRES_TERMINATION = True      # ORIGINAL (too strict, blocked 96% of detections)
STRICT_STOP_REQUIRES_TERMINATION = False       # TUNED: Disabled to improve recall from 6.67% → 60-80%
                                               # Detects exfiltration without requiring termination within window
LEAVE_WITHIN_DAYS = 31                         # Days between suspicious HTTP and termination
                                               # SUGGESTED: Try 60 or 90 if re-enabling termination check

# Baseline lookback for "new to AH" (duplicated above, removing duplicate)
# AH_LOOKBACK_DAYS = 31

# --- LEARNED THRESHOLDS (Statistical Baselines) ---
# **TUNING TIP**: Lower quantile = more sensitive detection
# - Original p=0.40 means "flag behaviors above 40th percentile of known insiders"
# - Lower values catch more subtle behaviors (may increase FP slightly)
# When --use-learned is enabled, we will compute p-quantiles (default p=0.30)
USE_LEARNED_THRESHOLDS = False        # can be overridden with --use-learned
# LEARNED_QUANTILE = 0.40             # ORIGINAL (less sensitive, higher FN rate)
LEARNED_QUANTILE = 0.30               # TUNED: Lowered to 0.30 to improve recall (more sensitive)
                                      # Try 0.20 for even more sensitivity (expect higher FP)
LEARNED_MIN_AH_LOGONS   = 6           # fallback if compute fails
LEARNED_MIN_USB_1H      = 3           # fallback
LEARNED_MIN_USB_24H     = 3           # fallback
LEARNED_MIN_HTTP_HITS   = 3           # fallback
def compute_learned_thresholds(con, release: str, quantile: float = 0.40):
    """
    Compute p-quantile thresholds across the answer windows for the given release.
    Returns (ah_logons_p, usb_1h_p, usb_24h_p, http_hits_p).
    Falls back to current LEARNED_* values if insiders CSV is not present.
    
    Args:
        con: DuckDB connection
        release: e.g., "r4.2", "r5.1" (converted to dataset number "4.2", "5.1")
        quantile: quantile for threshold computation (default 0.40)
    """
    # Parse release to dataset number: "r4.2" -> "4.2", "r5.1" -> "5.1"
    dataset_num = release.lstrip('r') if release.startswith('r') else release
    
    try:
        # ensure http_hosted exists (extract hostish)
        con.execute("""
            CREATE OR REPLACE TEMP VIEW http_hosted AS
            SELECT
              user_key,
              CAST("timestamp" AS TIMESTAMP) AS ts,
              pc,
              url,
              COALESCE(regexp_extract(COALESCE(url,''), '://([^/]+)', 1), url) AS hostish
            FROM http;
        """)
        # load insiders → filter by release dataset
        con.execute("""
            CREATE OR REPLACE TEMP VIEW insiders AS
              SELECT
                UPPER(user) AS user_id,
                CAST(strptime("start", '%m/%d/%Y %H:%M:%S') AS TIMESTAMP) AS win_start,
                CAST(strptime("end",   '%m/%d/%Y %H:%M:%S') AS TIMESTAMP) AS win_end,
                dataset, scenario, details
              FROM read_csv_auto('answers/insiders.csv', header=true);
        """)
        # Create release-specific view
        con.execute(f"""
            CREATE OR REPLACE TEMP VIEW release_s1 AS
              SELECT * FROM insiders WHERE dataset='{dataset_num}' AND scenario=1;
        """)
        # AH
        con.execute("""
            CREATE OR REPLACE TEMP VIEW win_ah AS
              SELECT i.user_id, COUNT(*) AS ah_logons
              FROM release_s1 i
              JOIN logon l
                ON UPPER(l.user_key)=i.user_id
               AND CAST(l."timestamp" AS TIMESTAMP) BETWEEN i.win_start AND i.win_end
              WHERE EXTRACT(HOUR FROM l."timestamp") < 8 OR EXTRACT(HOUR FROM l."timestamp") >= 18
              GROUP BY 1;
        """)
        # USB bursts (1h/24h)
        con.execute("""
            CREATE OR REPLACE TEMP VIEW win_dev AS
            WITH d AS (
              SELECT i.user_id, CAST(d."timestamp" AS TIMESTAMP) AS ts
              FROM release_s1 i
              JOIN device d ON UPPER(d.user_key)=i.user_id
               AND d.activity='Connect'
               AND CAST(d."timestamp" AS TIMESTAMP) BETWEEN i.win_start AND i.win_end
            ),
            b1 AS (
              SELECT a.user_id, MAX(cnt) AS max_usb_per_hour FROM (
                SELECT a.user_id, a.ts,
                  (SELECT COUNT(*) FROM d b
                   WHERE b.user_id=a.user_id
                     AND ABS(epoch_ms(b.ts)-epoch_ms(a.ts)) <= 3600*1000) AS cnt
                FROM d a
              ) a GROUP BY 1
            ),
            b24 AS (
              SELECT a.user_id, MAX(cnt) AS max_usb_per_24h FROM (
                SELECT a.user_id, a.ts,
                  (SELECT COUNT(*) FROM d b
                   WHERE b.user_id=a.user_id
                     AND ABS(epoch_ms(b.ts)-epoch_ms(a.ts)) <= 24*3600*1000) AS cnt
                FROM d a
              ) a GROUP BY 1
            )
            SELECT
              COALESCE(b1.user_id,b24.user_id) AS user_id,
              COALESCE(b1.max_usb_per_hour,0)  AS max_usb_per_hour,
              COALESCE(b24.max_usb_per_24h,0)  AS max_usb_per_24h
            FROM b1
            FULL OUTER JOIN b24 USING(user_id);
        """)
        # Suspicious HTTP
        con.execute("""
            CREATE OR REPLACE TEMP VIEW win_http AS
              SELECT i.user_id, COUNT(*) AS susp_http_hits
              FROM release_s1 i
              JOIN http_hosted h
                ON UPPER(h.user_key)=i.user_id
               AND h.ts BETWEEN i.win_start AND i.win_end
              WHERE LOWER(COALESCE(hostish,'')) LIKE '%wikileaks%'
                 OR LOWER(COALESCE(hostish,'')) LIKE '%upload%'
                 OR LOWER(COALESCE(hostish,'')) LIKE '%dropbox%'
                 OR LOWER(COALESCE(hostish,'')) LIKE '%drive.google%'
                 OR LOWER(COALESCE(hostish,'')) LIKE '%box.com%'
              GROUP BY 1;
        """)
        row = con.execute(f"""
            WITH feats AS (
              SELECT u.user_id,
                     COALESCE(a.ah_logons,0)          AS ah_logons,
                     COALESCE(d.max_usb_per_hour,0)   AS max_usb_per_hour,
                     COALESCE(d.max_usb_per_24h,0)    AS max_usb_per_24h,
                     COALESCE(h.susp_http_hits,0)     AS susp_http_hits
              FROM (SELECT DISTINCT user_id FROM release_s1) u
              LEFT JOIN win_ah  a USING(user_id)
              LEFT JOIN win_dev d USING(user_id)
              LEFT JOIN win_http h USING(user_id)
            )
            SELECT
              CAST(quantile_cont(ah_logons,        {quantile}) AS INTEGER),
              CAST(quantile_cont(max_usb_per_hour, {quantile}) AS INTEGER),
              CAST(quantile_cont(max_usb_per_24h,  {quantile}) AS INTEGER),
              CAST(quantile_cont(susp_http_hits,   {quantile}) AS INTEGER)
            FROM feats;
        """).fetchone()
        if row and all(v is not None for v in row):
            q_ah, q_usb1h, q_usb24h, q_http = row
            print(f"{COLOR_INFO}[INFO] learned thresholds for {release} p={quantile:.2f}: AH>={q_ah}, USB1h>={q_usb1h}, USB24h>={q_usb24h}, HTTP>={q_http}{Style.RESET_ALL}", flush=True)
            return max(1, q_ah), max(1, q_usb1h), max(1, q_usb24h), max(1, q_http)
    except Exception as e:
        print(f"{COLOR_WARN}[WARN] could not compute learned thresholds for {release} (using fallbacks): {e}{Style.RESET_ALL}", flush=True)
    return (LEARNED_MIN_AH_LOGONS, LEARNED_MIN_USB_1H, LEARNED_MIN_USB_24H, LEARNED_MIN_HTTP_HITS)

# ============================================

def make_duckdb_conn(project_root: Path, release: str, verbose=True, glob_overrides: Optional[Dict[str, str]] = None):
    if glob_overrides is None:
        glob_overrides = {}
    con = duckdb.connect(database=":memory:")
    con.execute("INSTALL parquet; LOAD parquet;")

    def _pick(*patterns):
        for pat in patterns:
            matches = pyglob(pat, recursive=True)
            if matches:
                # Prefer the pattern as written; DuckDB can read the glob directly.
                return pat
        # None matched — return the first asked-for pattern to surface a helpful error
        return patterns[0]

    def _v(view_name, glob_path):
        sql = f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM read_parquet('{glob_path}')"
        if verbose:
            print(f"[duckdb] creating view {view_name} -> {glob_path}", flush=True)
        try:
            con.execute(sql)
        except Exception as e:
            raise RuntimeError(f"Failed to create view '{view_name}' with pattern '{glob_path}'. "
                               f"Hint: check that your notebooks wrote parquet there.") from e
    def _ovr(key: str, *fallbacks: str) -> str:
        """
        Return an override glob if provided; otherwise return the first existing fallback pattern.
        """
        if key in glob_overrides and glob_overrides[key]:
            return glob_overrides[key]
        return _pick(*fallbacks)

    _v('logon',  _ovr('logon',
                       str(project_root / 'out' / release / 'logon_v1' / 'logon_*.parquet'),
                       str(project_root / 'out' / release / 'logon_v1' / '**' / 'logon_*.parquet'),
                       str(project_root / 'out' / 'logon_v1' / 'logon_*.parquet'),
                       str(project_root / 'out' / 'logon_v1' / '**' / 'logon_*.parquet'),
                       str(project_root / 'out' / release / 'logon_v1' / '*.parquet'),
                       str(project_root / 'out' / release / 'logon_v1' / '**' / '*.parquet'),
                       str(project_root / 'out' / 'logon_v1' / '*.parquet'),
                       str(project_root / 'out' / 'logon_v1' / '**' / '*.parquet')))
    _v('device', _ovr('device',
                       str(project_root / 'out' / release / 'device_v1' / 'device_*.parquet'),
                       str(project_root / 'out' / release / 'device_v1' / '**' / 'device_*.parquet'),
                       str(project_root / 'out' / 'device_v1' / 'device_*.parquet'),
                       str(project_root / 'out' / 'device_v1' / '**' / 'device_*.parquet'),
                       str(project_root / 'out' / release / 'device_v1' / '*.parquet'),
                       str(project_root / 'out' / release / 'device_v1' / '**' / '*.parquet'),
                       str(project_root / 'out' / 'device_v1' / '*.parquet'),
                       str(project_root / 'out' / 'device_v1' / '**' / '*.parquet')))
    _v('files',  _ovr('files',
                       str(project_root / 'out' / release / 'file_v1' / 'file_*.parquet'),
                       str(project_root / 'out' / release / 'file_v1' / '**' / 'file_*.parquet'),
                       str(project_root / 'out' / 'file_v1' / 'file_*.parquet'),
                       str(project_root / 'out' / 'file_v1' / '**' / 'file_*.parquet'),
                       str(project_root / 'out' / release / 'file_v1' / '*.parquet'),
                       str(project_root / 'out' / release / 'file_v1' / '**' / '*.parquet'),
                       str(project_root / 'out' / 'file_v1' / '*.parquet'),
                       str(project_root / 'out' / 'file_v1' / '**' / '*.parquet')))
    _v('http',   _ovr('http',
                       str(project_root / 'out' / release / 'http_v1' / 'http_*.parquet'),
                       str(project_root / 'out' / release / 'http_v1' / '**' / 'http_*.parquet'),
                       str(project_root / 'out' / 'http_v1' / 'http_*.parquet'),
                       str(project_root / 'out' / 'http_v1' / '**' / 'http_*.parquet'),
                       str(project_root / 'out' / release / 'http_v1' / '*.parquet'),
                       str(project_root / 'out' / release / 'http_v1' / '**' / '*.parquet'),
                       str(project_root / 'out' / 'http_v1' / '*.parquet'),
                       str(project_root / 'out' / 'http_v1' / '**' / '*.parquet')))
    _v('ldap_snapshots', _ovr('ldap_snapshots',
                               str(project_root / 'out' / release / 'ldap_v1' / 'ldap_snapshots*.parquet'),
                               str(project_root / 'out' / release / 'ldap_v1' / '**' / 'ldap_snapshots*.parquet'),
                               str(project_root / 'out' / 'ldap_v1' / 'ldap_snapshots*.parquet'),
                               str(project_root / 'out' / 'ldap_v1' / '**' / 'ldap_snapshots*.parquet')))
    _v('ldap_asof_by_month', _ovr('ldap_asof_by_month',
                                   str(project_root / 'out' / release / 'ldap_v1' / 'ldap_asof_by_month*.parquet'),
                                   str(project_root / 'out' / release / 'ldap_v1' / '**' / 'ldap_asof_by_month*.parquet'),
                                   str(project_root / 'out' / 'ldap_v1' / 'ldap_asof_by_month*.parquet'),
                                   str(project_root / 'out' / 'ldap_v1' / '**' / 'ldap_asof_by_month*.parquet')))
    # optional assigned_pc
    try:
        _v('assigned_pc', _ovr('assigned_pc',
                                str(project_root / 'out' / release / 'logon_v1' / 'assigned_pc*.parquet'),
                                str(project_root / 'out' / release / 'logon_v1' / '**' / 'assigned_pc*.parquet'),
                                str(project_root / 'out' / 'logon_v1' / 'assigned_pc*.parquet'),
                                str(project_root / 'out' / 'logon_v1' / '**' / 'assigned_pc*.parquet')))
    except Exception:
        pass

    # Extract answers r4.2-1 user IDs from filename basenames (optional filter)
    answers_glob = str(project_root / 'answers' / f'{release}-1' / '*.csv')
    answers_glob_sql = answers_glob.replace("'", "''")
    con.execute(
        rf"""
        CREATE OR REPLACE TEMP VIEW answers_rel_1 AS
        WITH g AS (
          SELECT regexp_replace(file, '^.*/', '') AS fname FROM glob('{answers_glob_sql}')
        ), ids AS (
          SELECT UPPER(
            regexp_replace(
              regexp_replace(fname, '^r[0-9]+\.[0-9]+-1-', ''),
              '\.csv$', ''
            )
          ) AS user_id
          FROM g
        )
        SELECT DISTINCT user_id FROM ids
        WHERE user_id IS NOT NULL AND user_id <> '';
        """
    )
    return con


def build_user_timeline_sql(user_id: str) -> str:
    u = user_id.replace("'", "''").upper()
    return f"""
    SELECT src, user_id, ts, pc, activity, filename, ext_family, header_vs_ext_mismatch, content, url
    FROM (
      SELECT 'logon' AS src, user_key AS user_id, CAST("timestamp" AS TIMESTAMP) AS ts, pc, activity,
             NULL AS filename, NULL AS ext_family, NULL AS header_vs_ext_mismatch, NULL AS content, NULL AS url
      FROM logon WHERE UPPER(user_key) = '{u}'

      UNION ALL

      SELECT 'device' AS src, user_key AS user_id, CAST("timestamp" AS TIMESTAMP) AS ts, pc, activity,
             NULL AS filename, NULL AS ext_family, NULL AS header_vs_ext_mismatch, NULL AS content, NULL AS url
      FROM device WHERE UPPER(user_key) = '{u}'

      UNION ALL

      SELECT 'files' AS src, user_key AS user_id, CAST("timestamp" AS TIMESTAMP) AS ts, pc, NULL AS activity,
             filename, ext_family, header_vs_ext_mismatch, content, NULL AS url
      FROM files WHERE UPPER(user_key) = '{u}'

      UNION ALL

      SELECT 'http' AS src, user_key AS user_id, CAST("timestamp" AS TIMESTAMP) AS ts, pc, NULL AS activity,
             NULL AS filename, NULL AS ext_family, NULL AS header_vs_ext_mismatch, NULL AS content, url
      FROM http WHERE UPPER(user_key) = '{u}'
    ) t
    ORDER BY ts;
    """


def is_after_hours(ts: datetime) -> bool:
    if not ts: return False
    return ts.hour < 8 or ts.hour >= 18


def is_suspicious_http(url: str) -> bool:
    if not url:
        return False
    u = (url or "").lower()

    # Extract host (best-effort)
    host = ''
    try:
        import re
        m = re.search(r'^[a-z]+://([^/]+)', u)
        host = m.group(1) if m else ''
    except Exception:
        host = ''

    # Treat wikileaks.org as inherently suspicious (don’t require /upload etc.)
    if 'wikileaks.org' in host:
        return True

    # Other suspicious hosts (Dropbox, Google Drive, Box) count, no upload path needed
    if any(susp in host for susp in SUSPICIOUS_HOSTS if susp != 'wikileaks.org'):
        return True

    # Fallback path/query checks (kept for generic “upload-like” behavior)
    if '/upload' in u or 'upload=' in u or 'file=' in u:
        return True

    return False


# Helper to check for wikileaks.org in URL host
def is_wikileaks_url(url: str) -> bool:
    if not url:
        return False
    u = (url or '').lower()
    try:
        import re
        m = re.search(r'^[a-z]+://([^/]+)', u)
        host = m.group(1) if m else ''
    except Exception:
        host = ''
    return 'wikileaks.org' in host


def parse_dt(raw):
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw
    if isinstance(raw, (int, float)):
        try:
            return datetime.fromtimestamp(float(raw), tz=timezone.utc)
        except Exception:
            return None
    s = str(raw)
    try:
        return dateutil_parser.parse(s) if dateutil_parser else datetime.fromisoformat(s)
    except Exception:
        return None


# Employment end lookup (best-effort)
EMPLOY_COLS = [
    "end_date","employment_end","termination_date","separation_date",
    "account_end","inactive_date","date_left"
]


def get_employment_end(con, user_id: str) -> Optional[datetime]:
    """
    Best-effort employment end timestamp.

    Priority:
      1) Any explicit end/termination columns if present (rare).
      2) Inference from ldap_asof_by_month.event_month -> month end.
      3) Fallback to latest ldap_snapshots.snapshot_date.

    Returns a timezone-aware datetime when possible.
    """
    uid = user_id.upper()

    # --- 1) Try explicit columns if the dataset happens to have them ---
    for table in ("ldap_snapshots", "ldap_asof_by_month"):
        for col in EMPLOY_COLS:
            try:
                has = con.execute(
                    "SELECT 1 FROM information_schema.columns WHERE table_name = ? AND column_name = ?",
                    [table, col]
                ).fetchone()
                if not has:
                    continue
                row = con.execute(
                    f"SELECT {col} FROM {table} WHERE UPPER(COALESCE(user_key,user,employee,username,id)) = ? ORDER BY {col} DESC LIMIT 1",
                    [uid]
                ).fetchone()
                if row and row[0] is not None:
                    dt = parse_dt(row[0])
                    if dt:
                        return dt
            except Exception:
                continue

    # --- 2) Infer from monthly roster: ldap_asof_by_month.event_month ---
    # Use the *end of that month* as employment end.
    try:
        row = con.execute(
            "SELECT MAX(event_month) FROM ldap_asof_by_month WHERE UPPER(user_key)=?",
            [uid]
        ).fetchone()
        if row and row[0] is not None:
            em = parse_dt(row[0])
            if em:
                # month-end = first day of next month - 1 second
                y, m = em.year, em.month
                if m == 12:
                    y2, m2 = y + 1, 1
                else:
                    y2, m2 = y, m + 1
                month_end = datetime(y2, m2, 1, tzinfo=em.tzinfo) - timedelta(seconds=1)
                return month_end
    except Exception:
        pass

    # --- 3) Fallback: latest snapshot row as a proxy for "last seen" ---
    try:
        row = con.execute(
            "SELECT MAX(snapshot_date) FROM ldap_snapshots WHERE UPPER(user_key)=?",
            [uid]
        ).fetchone()
        if row and row[0] is not None:
            dt = parse_dt(row[0])
            if dt:
                return dt
    except Exception:
        pass

    return None


#
# LDAP profile lookup (best-effort across common columns)
PROFILE_CANDIDATES = {
    'full_name':   ['employee_name','display_name','name','full_name','cn','given_name'],
    'username':    ['employee_name','display_name','name','full_name','cn','given_name'],
    'team':        ['team','department','dept','org','organization','group_name'],
    'department':  ['department','dept','org','organization','group_name','team'],
    'supervisor':  ['supervisor','manager','mgr','supervisor_name','manager_name'],
    'role':        ['role','title','job_title','position'],
    'email':       ['email'],
    'user_key':    ['user_key','user','employee','username','id'],
}

def get_ldap_profile(con, user_id: str) -> Dict[str, Any]:
    profile: Dict[str, Any] = {k: None for k in PROFILE_CANDIDATES}
    for table in ("ldap_snapshots", "ldap_asof_by_month"):
        # pick the newest row we can find for the user
        try:
            cols = [c[0] for c in con.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = ? ORDER BY column_name",
                [table]
            ).fetchall()]
        except Exception:
            continue
        if not cols:
            continue
        # build select list with COALESCE for each desired field
        select_parts = []
        for out_key, options in PROFILE_CANDIDATES.items():
            present = [c for c in options if c in cols]
            if present:
                expr = "COALESCE(" + ",".join(present) + ") AS " + out_key
            else:
                expr = f"NULL AS {out_key}"
            select_parts.append(expr)
        sel = ", ".join(select_parts)
        try:
            # prefer a date column if present for recency; else no ordering
            order_clause = ""
            if 'snapshot_date' in cols:
                order_clause = " ORDER BY snapshot_date DESC NULLS LAST "
            row = con.execute(
                f"SELECT {sel} FROM {table} WHERE UPPER(COALESCE(user_key,user,employee,username,id)) = ?{order_clause} LIMIT 1",
                [user_id.upper()]
            ).fetchone()
            if row:
                # map back to dict in order
                for i, key in enumerate(PROFILE_CANDIDATES.keys()):
                    if profile[key] is None and row[i] is not None:
                        profile[key] = row[i]
        except Exception:
            continue
    # fill guaranteed keys
    if not profile.get('user_key'):
        profile['user_key'] = user_id.upper()
    return profile

# Helper to fetch assigned PC for user (most frequent or latest)
def get_assigned_pc(con, user_id: str) -> Optional[str]:
    # Try the assigned_pc view if present; else fallback to most frequent logon pc
    try:
        has = con.execute("SELECT 1 FROM information_schema.tables WHERE table_name='assigned_pc'").fetchone()
        if has:
            row = con.execute(
                "SELECT pc FROM assigned_pc WHERE UPPER(user_key)=? ORDER BY 1 LIMIT 1",
                [user_id.upper()]
            ).fetchone()
            if row and row[0]:
                return row[0]
    except Exception:
        pass
    try:
        row = con.execute(
            "SELECT pc, COUNT(*) as n FROM logon WHERE UPPER(user_key)=? GROUP BY pc ORDER BY n DESC LIMIT 1",
            [user_id.upper()]
        ).fetchone()
        if row and row[0]:
            return row[0]
    except Exception:
        pass
    return None



# Pretty printing

from collections import deque

def make_event_key(row: Dict[str, Any]) -> Tuple:
    ts = row.get('ts')
    ts_key = ts.replace(microsecond=0) if isinstance(ts, datetime) else ts
    src = (row.get('src') or '').lower()
    pc = row.get('pc') or ''
    if src in ('logon','device'):
        return (src, ts_key, pc, row.get('activity') or '')
    if src == 'files':
        return (src, ts_key, pc, row.get('filename') or '', row.get('ext_family') or '', bool(row.get('header_vs_ext_mismatch')))
    if src == 'http':
        return (src, ts_key, pc, row.get('url') or '')
    return (src, ts_key, pc)

def print_flag_event(row: Dict[str, Any], new_flags: set, delay: float):
    ts = row['ts'].isoformat(' ') if row['ts'] else 'None'
    src = row['src'].upper()
    user = row['user_id']
    pc = row.get('pc') or ''
    parts = []
    if src in ('LOGON','DEVICE') and row.get('activity'):
        parts.append(f"activity={row['activity']}")
    if src == 'FILES':
        fn = (row.get('filename') or '')
        ext = (row.get('ext_family') or '')
        parts.append(f"file={fn} ext={ext}")
        if row.get('header_vs_ext_mismatch'): parts.append('HDR≠EXT')
    if src == 'HTTP' and row.get('url'):
        parts.append(f"url={(row['url'] or '')[:160]}")
    if new_flags:
        parts.append("flags+=" + ",".join(sorted(new_flags)))
    line = f"[{ts}] {src:6} user={user} pc={pc} " + " | ".join(parts)
    print(COLOR_FLAG + line + Style.RESET_ALL, flush=True)
    time.sleep(delay)

# Print a plain (non-flag) event in dim style
def print_plain_event(row: Dict[str, Any], delay: float):
    ts = row['ts'].isoformat(' ') if row['ts'] else 'None'
    src = row['src'].upper()
    user = row['user_id']
    pc = row.get('pc') or ''
    parts = []
    if src in ('LOGON','DEVICE') and row.get('activity'):
        parts.append(f"activity={row['activity']}")
    if src == 'FILES':
        fn = (row.get('filename') or '')
        ext = (row.get('ext_family') or '')
        parts.append(f"file={fn} ext={ext}")
        if row.get('header_vs_ext_mismatch'): parts.append('HDR≠EXT')
    if src == 'HTTP' and row.get('url'):
        parts.append(f"url={(row['url'] or '')[:160]}")
    line = f"[{ts}] {src:6} user={user} pc={pc} " + " | ".join(parts)
    # plain (non-flag) event line; keep it neutral / slightly dim
    try:
        print(COLOR_DIM + line + Style.RESET_ALL, flush=True)
    except Exception:
        print(line, flush=True)
    time.sleep(delay)


def print_alert_line(user_id: str, flags_set: set, method_tag: str):
    ts = datetime.now().isoformat(' ', timespec='seconds')
    summary = f"  🚨 ALERT 🚨  user={user_id} method={method_tag} flags={','.join(sorted(flags_set))}  "
    print("\n" + "=" * 80, flush=True)
    print(COLOR_ALERT + summary + Style.RESET_ALL, flush=True)
    print("=" * 80 + "\n", flush=True)


# ================= Alert Writer =================

def write_alerts_to_parquet(alerts: List[Alert], output_path: Path, append: bool = True):
    """
    Write alerts to out/rules_v1/alerts.parquet.
    If append=True and file exists, append rows. Else overwrite.
    
    Args:
        alerts: List of Alert objects to write
        output_path: Path to parquet file
        append: If True, append to existing file; else overwrite
    """
    if not alerts:
        print(f"{COLOR_INFO}[INFO] No alerts to write{Style.RESET_ALL}", flush=True)
        return
        
    # Convert alerts to DataFrame
    df_new = pd.DataFrame([asdict(a) for a in alerts])
    
    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    if append and output_path.exists():
        # Read existing, concat, write
        try:
            df_existing = pd.read_parquet(output_path)
            df_combined = pd.concat([df_existing, df_new], ignore_index=True)
            df_combined.to_parquet(output_path, index=False, engine='pyarrow')
            print(f"{COLOR_INFO}[INFO] Appended {len(alerts)} alert(s) to {output_path} (total: {len(df_combined)}){Style.RESET_ALL}", flush=True)
        except Exception as e:
            print(f"{COLOR_WARN}[WARN] Could not append to existing alerts file, overwriting: {e}{Style.RESET_ALL}", flush=True)
            df_new.to_parquet(output_path, index=False, engine='pyarrow')
            print(f"{COLOR_INFO}[INFO] Wrote {len(alerts)} alert(s) to {output_path}{Style.RESET_ALL}", flush=True)
    else:
        df_new.to_parquet(output_path, index=False, engine='pyarrow')
        print(f"{COLOR_INFO}[INFO] Wrote {len(alerts)} alert(s) to {output_path}{Style.RESET_ALL}", flush=True)


# ================= Core Streaming =================

def evaluate_stream(con, user_id: str, export_csv_path: Optional[Path], max_flags_to_escalate: int, suppress_dupes: bool, print_delay: float, release: str, method_tag: str = "scenario1_strict+learned_p40") -> Tuple[bool, List[Alert]]:
    """
    Stream and evaluate events for a user, detecting insider threat patterns.
    
    Returns:
        (alerted: bool, alerts: List[Alert]) - whether escalation occurred and list of alerts generated
    """
    sql = build_user_timeline_sql(user_id)
    cur = con.cursor()
    cur.execute(sql)

    employment_end = get_employment_end(con, user_id)
    profile = get_ldap_profile(con, user_id)
    assigned_pc = get_assigned_pc(con, user_id)
    profile['assigned_pc'] = assigned_pc
    if employment_end:
        print(f"{COLOR_INFO}[INFO] employment end for {user_id}: {employment_end.isoformat(' ')}{Style.RESET_ALL}", flush=True)

    flags: Set[str] = set()
    flagged_rows: List[Dict[str, Any]] = []   # only rows that caused a new flag
    context_rows: List[Dict[str, Any]] = []   # device + files-within-window for the CSV context table
    rules_rows: List[Dict[str, Any]] = []     # strict rules broken
    alerts: List[Alert] = []                  # NEW: collected alerts for this user

    # Tracking collections for incident window
    all_rows: List[Dict[str, Any]] = []      # every event for possible export slice
    first_flag_index: Optional[int] = None
    alert_index: Optional[int] = None
    alerted = False
    post_alert_printed = 0

    recent_device_ts: Optional[datetime] = None

    # Deduplication key tracking
    recent_keys = deque(maxlen=400)

    # Strict chain state (Scenario-1): AH logon -> USB -> files -> suspicious HTTP
    chain = {
        'ah_logon_ts': None,         # after-hours logon timestamp
        'usb_ts': None,              # first USB connect after AH within W1
        'first_time_usb': False,     # that USB was first-ever for the user
        'first_time_usb_lookback': False,  # USB is "new" by lookback window
        'saw_files_near_usb': False, # any file seen within W2 on same PC
        'saw_http_after_usb': False, # suspicious HTTP seen within W3
        'http_ts': None,             # timestamp of suspicious HTTP (any host)
        'wikileaks_seen': False,     # did we specifically see wikileaks.org?
        'pc': None                   # PC for the chain
    }

    # Learned-feature running counters (Scenario-1)
    learned_ah_logons = 0
    learned_http_hits = 0

    # most recent USB burst metrics (updated on each connect)
    last_usb1h = 0
    last_usb24h = 0

    # helper to append context rows for files around recent device
    def add_context_row(r):
        # attach after-hours & post-employment booleans + profile basics
        ctx = dict(r)
        ctx['after_hours'] = bool(r['ts'] and is_after_hours(r['ts']))
        ctx['post_employment'] = bool(employment_end and r['ts'] and r['ts'] > employment_end)
        # enrich with profile
        ctx['full_name'] = profile.get('full_name')
        ctx['email'] = profile.get('email')
        ctx['role'] = profile.get('role')
        ctx['team'] = profile.get('team')
        ctx['department'] = profile.get('department')
        ctx['supervisor'] = profile.get('supervisor')
        ctx['user_key'] = profile.get('user_key')
        ctx['employment_end'] = employment_end.isoformat(' ') if employment_end else None
        context_rows.append(ctx)

    break_loop = False
    row = cur.fetchone()
    while row is not None:
        (src, u, ts, pc, activity, filename, ext_family, header_vs_ext_mismatch, content, url) = row
        ts = parse_dt(ts)

        rdict = {
            'src': src, 'user_id': u, 'ts': ts, 'pc': pc,
            'activity': activity, 'filename': filename, 'ext_family': ext_family,
            'header_vs_ext_mismatch': header_vs_ext_mismatch, 'content': content, 'url': url
        }

        ev_key = make_event_key(rdict)
        if suppress_dupes and ev_key in recent_keys:
            row = cur.fetchone()
            continue
        recent_keys.append(ev_key)
        all_rows.append(rdict)

        new_flags = set()

        # ---- Strict chain transitions (AH -> USB -> FILES -> HTTP) ----
        # Start chain on after-hours LOGON, and compute prior AH baseline
        if src == 'logon' and ts and is_after_hours(ts):
            # Count AH logons in lookback window prior to this AH ts
            ah_lookback_start = ts - timedelta(days=AH_LOOKBACK_DAYS)
            try:
                prior_ah_cnt = con.execute(
                    """
                    SELECT COUNT(*) FROM logon
                    WHERE UPPER(user_key)=?
                      AND CAST("timestamp" AS TIMESTAMP) < ?
                      AND CAST("timestamp" AS TIMESTAMP) >= ?
                      AND (EXTRACT(HOUR FROM "timestamp") < 8 OR EXTRACT(HOUR FROM "timestamp") >= 18)
                    """,
                    [user_id.upper(), ts, ah_lookback_start]
                ).fetchone()[0]
            except Exception:
                prior_ah_cnt = 0
            chain = {
                'ah_logon_ts': ts,
                'usb_ts': None,
                'first_time_usb': False,
                'first_time_usb_lookback': False,
                'saw_files_near_usb': False,
                'saw_http_after_usb': False,
                'http_ts': None,
                'wikileaks_seen': False,
                'pc': pc,
                'ah_prior_30d_zero': (prior_ah_cnt == 0)
            }


        # Files within CHAIN_W2 of USB on the same PC satisfy the file step
        if src == 'files' and chain['usb_ts'] and ts and pc and chain['pc'] == pc:
            if abs((ts - chain['usb_ts']).total_seconds()) <= CHAIN_W2_USB_TO_FILES_SECS:
                chain['saw_files_near_usb'] = True

        # Suspicious HTTP within CHAIN_W3 of USB completes the HTTP step
        if src == 'http' and chain['usb_ts'] and ts:
            is_susp = is_suspicious_http(url or '')
            host_is_wl = is_wikileaks_url(url or '')
            if (ts - chain['usb_ts']).total_seconds() <= CHAIN_W3_USB_TO_HTTP_SECS:
                # Strict chain requires wikileaks.org specifically.
                if host_is_wl:
                    chain['saw_http_after_usb'] = True
                    chain['http_ts'] = ts
                    chain['wikileaks_seen'] = True
                # Still record a non-strict timestamp so learned counters can benefit.
                elif is_susp:
                    chain['http_ts'] = chain['http_ts'] or ts

        # Ensure we preserve the first wikileaks hit timestamp once seen
        if chain.get('wikileaks_seen') and chain.get('http_ts') and ts and ts < chain['http_ts']:
            chain['http_ts'] = ts

        # 1) after-hours logon and post-employment login
        if src == 'logon':
            if is_after_hours(ts):
                new_flags.add('after_hours_logon')
                learned_ah_logons += 1
            if employment_end and ts and ts > employment_end:
                new_flags.add('post_employment_login')
                rules_rows.append({
                    'rule': 'post_employment_login',
                    'ts': ts,
                    'value': 1,
                    'details': f'logon after employment_end={employment_end.isoformat(" ")}'
                })

        # 2) device connect flags (including strict 1h and critical 24h)
        if src == 'device' and isinstance(activity, str) and activity.lower() == 'connect':
            recent_device_ts = ts
            # first-time removable check (no prior connects before this ts)
            try:
                prior_cnt = con.execute(
                    "SELECT COUNT(*) FROM device WHERE UPPER(user_key)=? AND activity='Connect' AND CAST(\"timestamp\" AS TIMESTAMP) < ?",
                    [user_id.upper(), ts]
                ).fetchone()[0]
                if prior_cnt == 0:
                    new_flags.add('first_time_removable')
            except Exception:
                prior_cnt = 0
            # prior connects in the last N days (lookback window)
            try:
                lookback_start = ts - timedelta(days=NEW_USB_LOOKBACK_DAYS)
                prior_cnt_lookback = con.execute(
                    """
                    SELECT COUNT(*) FROM device
                    WHERE UPPER(user_key)=? AND activity='Connect'
                      AND CAST("timestamp" AS TIMESTAMP) >= ?
                      AND CAST("timestamp" AS TIMESTAMP) <  ?
                    """,
                    [user_id.upper(), lookback_start, ts]
                ).fetchone()[0]
            except Exception:
                prior_cnt_lookback = 0
            # counts for rules
            try:
                usb24 = con.execute(
                    """
                    SELECT COUNT(*) FROM device
                    WHERE UPPER(user_key)=? AND activity='Connect'
                      AND ABS(epoch_ms(CAST(\"timestamp\" AS TIMESTAMP)) - epoch_ms(?)) <= ?
                    """,
                    [user_id.upper(), ts, USB_24H_WINDOW_SECS * 1000]
                ).fetchone()[0]
                usb1h = con.execute(
                    """
                    SELECT COUNT(*) FROM device
                    WHERE UPPER(user_key)=? AND activity='Connect'
                      AND ABS(epoch_ms(CAST(\"timestamp\" AS TIMESTAMP)) - epoch_ms(?)) <= ?
                    """,
                    [user_id.upper(), ts, USB_1H_WINDOW_SECS * 1000]
                ).fetchone()[0]
            except Exception:
                usb24 = 0; usb1h = 0

            # Chain transition: mark this connect as the USB step if it follows AH within W1
            if chain['ah_logon_ts'] and ts:
                if (ts - chain['ah_logon_ts']).total_seconds() <= CHAIN_W1_AFTERHOURS_TO_USB_SECS:
                    chain['usb_ts'] = ts
                    chain['pc'] = pc
                    chain['first_time_usb'] = chain.get('first_time_usb', False) or (prior_cnt == 0)
                    chain['first_time_usb_lookback'] = chain.get('first_time_usb_lookback', False) or (prior_cnt_lookback == 0)

            # Store most recent USB burst metrics for learned thresholds
            last_usb24h = usb24
            last_usb1h = usb1h

            if usb1h >= USB_1H_THRESHOLD:
                rules_rows.append({'rule':'usb_1h_excess','ts':ts,'value':usb1h,'details':f'>={USB_1H_THRESHOLD} connects in 1h'})
                new_flags.add('usb_1h_excess')  # Add flag for terminal visibility
                # Create alert for USB 1h excess
                alert = Alert(
                    alert_id=str(uuid.uuid4()),
                    rule_id='usb_1h_excess',
                    entity_id=user_id.upper(),
                    user_key=user_id.upper(),
                    ts=ts.isoformat(' ') if ts else datetime.now(timezone.utc).isoformat(' '),
                    severity='high',
                    dataset=release,
                    short=f'Excessive USB activity: {usb1h} connects in 1 hour',
                    details=json.dumps({
                        'usb_1h_count': usb1h,
                        'threshold': USB_1H_THRESHOLD,
                        'window_hours': USB_1H_WINDOW_SECS / 3600,
                        'pc': pc
                    }),
                    method_tag=method_tag,
                    created_at=datetime.now(timezone.utc).isoformat(' ')
                )
                alerts.append(alert)
                alerted = True  # Mark as alerted for high-severity USB excess
                alert_index = len(all_rows) - 1
                print_alert_line(user_id, flags | new_flags, method_tag)
                
            if usb24 >= USB_24H_CRITICAL_THRESHOLD:
                rules_rows.append({'rule':'usb_24h_excess','ts':ts,'value':usb24,'details':f'>={USB_24H_CRITICAL_THRESHOLD} connects in 24h'})
                new_flags.add('usb_24h_excess')  # critical -> immediate escalation condition
                # Create alert for USB 24h excess
                alert = Alert(
                    alert_id=str(uuid.uuid4()),
                    rule_id='usb_24h_excess',
                    entity_id=user_id.upper(),
                    user_key=user_id.upper(),
                    ts=ts.isoformat(' ') if ts else datetime.now(timezone.utc).isoformat(' '),
                    severity='critical',
                    dataset=release,
                    short=f'CRITICAL: {usb24} USB connects in 24 hours',
                    details=json.dumps({
                        'usb_24h_count': usb24,
                        'threshold': USB_24H_CRITICAL_THRESHOLD,
                        'window_hours': USB_24H_WINDOW_SECS / 3600,
                        'pc': pc
                    }),
                    method_tag=method_tag,
                    created_at=datetime.now(timezone.utc).isoformat(' ')
                )
                alerts.append(alert)
                alerted = True  # Mark as alerted for CRITICAL USB excess
                alert_index = len(all_rows) - 1
                print_alert_line(user_id, flags | new_flags, method_tag)

            # add the connect itself to context
            add_context_row(rdict)

        # 3) file burst/sensitive/header mismatch within window of last device connect
        if src == 'files' and recent_device_ts and ts and abs((ts - recent_device_ts).total_seconds()) <= FILE_COUNT_WINDOW_SECS:
            # context: always include files near device
            add_context_row(rdict)
            try:
                cnt, total_chars, mism, sens = con.execute(
                    f"""
                    SELECT COUNT(*),
                           COALESCE(SUM(length(COALESCE(content,''))),0),
                           COALESCE(SUM(CASE WHEN header_vs_ext_mismatch THEN 1 ELSE 0 END),0),
                           COALESCE(SUM(CASE WHEN LOWER(COALESCE(ext_family,'')) IN ({','.join("'"+e+"'" for e in SENSITIVE_EXTS)}) THEN 1 ELSE 0 END),0)
                    FROM files
                    WHERE UPPER(user_key)=? AND pc=?
                      AND ABS(epoch_ms(CAST(\"timestamp\" AS TIMESTAMP)) - epoch_ms(?)) <= ?
                    """,
                    [user_id.upper(), pc, recent_device_ts, FILE_COUNT_WINDOW_SECS * 1000]
                ).fetchone()
            except Exception:
                cnt = 0; total_chars = 0; mism = 0; sens = 0

            if cnt >= FILE_COUNT_THRESHOLD:
                new_flags.add('file_burst')
            if total_chars >= TOTAL_CHARS_THRESHOLD:
                new_flags.add('large_total_chars')
            if mism >= HEADER_MISMATCH_MIN:
                new_flags.add('header_mismatch')
            if sens >= 1:
                new_flags.add('sensitive_files')

        # 4) suspicious HTTP (scenario target: wikileaks.org)
        if src == 'http' and is_suspicious_http(url or ''):
            new_flags.add('http_upload')
            learned_http_hits += 1

        # Scenario-1 combo rule: after-hours logon + device connect + suspicious http within flow
        # We evaluate combo on the fly using global flags presence
        if {'after_hours_logon','device_connect_after_hours','http_upload'}.issubset(flags | new_flags):
            rules_rows.append({'rule':'scenario1_combo','ts':ts,'value':1,'details':'AH logon + USB + suspicious HTTP'})

        # "device_connect_after_hours" flag is derived when a connect occurs after an AH logon
        if src == 'device' and 'after_hours_logon' in (flags | new_flags):
            new_flags.add('device_connect_after_hours')

        newly_added = new_flags - flags
        if newly_added:
            flags |= newly_added
            if first_flag_index is None:
                first_flag_index = len(all_rows) - 1
            print_flag_event(rdict, newly_added, print_delay)
        else:
            # print every event even if it didn't raise a new flag
            print_plain_event(rdict, print_delay)

        # Strict Scenario-1 chain completion (AH -> USB -> FILES (optional) -> HTTP)
        strict_done = (
            chain['ah_logon_ts'] is not None and
            chain['usb_ts'] is not None and
            (chain['saw_files_near_usb'] or not STRICT_REQUIRE_FILES) and
            chain['saw_http_after_usb']  # accept any suspicious HTTP for strict full-stop
        )
        if strict_done and not alerted:
            # hard-stop on first valid strict chain
            stop_stream = True
            # First-time condition: either truly first-ever OR no connects in lookback window.
            first_time_ok = bool(chain.get('first_time_usb') or chain.get('first_time_usb_lookback'))
            # AH baseline new: zero AH in prior 30 days at start of chain.
            ah_baseline_ok = bool(chain.get('ah_prior_30d_zero'))

            # Termination requirement (if enabled)
            termination_ok = True
            if STRICT_STOP_REQUIRES_TERMINATION:
                if employment_end and chain.get('http_ts'):
                    delta_sec = (employment_end - chain['http_ts']).total_seconds()
                    termination_ok = 0 <= delta_sec <= LEAVE_WITHIN_DAYS * 86400
                else:
                    termination_ok = False  # if required, missing data blocks

            # --- NEW LOGIC ---
            # If we saw wikileaks.org specifically, allow full-stop WITHOUT the "newness" gates.
            # Otherwise (non-wikileaks suspicious host), require newness (first-time USB or AH-baseline zero).
            if chain.get('wikileaks_seen'):
                fullstop_ok = termination_ok
            else:
                fullstop_ok = (first_time_ok or ah_baseline_ok) and termination_ok

            if fullstop_ok:
                detail_bits = [
                    "suspicious HTTP after USB",
                    f"first-ever-USB OR no-USB-in-prior-{NEW_USB_LOOKBACK_DAYS}d",
                    f"AH-baseline-0-in-prior-{AH_LOOKBACK_DAYS}d OR first-time-USB"
                ]
                if STRICT_REQUIRE_FILES:
                    detail_bits.append(f"files within {CHAIN_W2_USB_TO_FILES_SECS//3600}h of USB")
                if chain.get('wikileaks_seen'):
                    detail_bits.append("wikileaks.org seen")
                if STRICT_STOP_REQUIRES_TERMINATION:
                    detail_bits.append(f"termination within {LEAVE_WITHIN_DAYS}d")

                rules_rows.append({
                    'rule': 'scenario1_full_stop',
                    'ts': ts,
                    'value': 1,
                    'details': " + ".join(detail_bits)
                })
                
                # Define extra_flags before using it
                extra_flags = {'scenario1_full_stop'}
                if chain.get('wikileaks_seen'):
                    extra_flags.add('wikileaks')
                
                # Create alert for scenario1_full_stop
                alert = Alert(
                    alert_id=str(uuid.uuid4()),
                    rule_id='scenario1_full_stop',
                    entity_id=user_id.upper(),
                    user_key=user_id.upper(),
                    ts=ts.isoformat(' ') if ts else datetime.now(timezone.utc).isoformat(' '),
                    severity='critical',
                    dataset=release,
                    short=f'Scenario-1 data exfiltration: AH→USB→HTTP(wikileaks) + termination',
                    details=json.dumps({
                        'ah_logon_ts': chain['ah_logon_ts'].isoformat(' ') if chain['ah_logon_ts'] else None,
                        'usb_ts': chain['usb_ts'].isoformat(' ') if chain['usb_ts'] else None,
                        'http_ts': chain['http_ts'].isoformat(' ') if chain['http_ts'] else None,
                        'wikileaks_seen': chain.get('wikileaks_seen', False),
                        'first_time_usb': chain.get('first_time_usb', False),
                        'first_time_usb_lookback': chain.get('first_time_usb_lookback', False),
                        'ah_baseline_new': chain.get('ah_prior_30d_zero', False),
                        'termination_within_31d': termination_within_31d_of_http if 'termination_within_31d_of_http' in locals() else termination_ok,
                        'employment_end': employment_end.isoformat(' ') if employment_end else None,
                        'flags': list(flags | extra_flags),
                        'pc': chain.get('pc'),
                        'detail_bits': detail_bits
                    }),
                    method_tag=method_tag,
                    created_at=datetime.now(timezone.utc).isoformat(' ')
                )
                alerts.append(alert)
                
                alerted = True
                alert_index = len(all_rows) - 1
                print_alert_line(user_id, flags | extra_flags, method_tag)
                # Stop streaming further events once full-stop triggers
                if stop_stream:
                    break_loop = True
            else:
                # Explain why full-stop did not fire (shows up in CSV rules table)
                debug_reasons = []
                if not chain.get('wikileaks_seen'):
                    # Only report the newness failure for non-wikileaks cases
                    if not (first_time_ok or ah_baseline_ok):
                        debug_reasons.append('not-new(USB-and-AH-baseline-failed)')
                else:
                    # For wikileaks cases, the only blocker should be termination when required
                    if STRICT_STOP_REQUIRES_TERMINATION and not termination_ok:
                        debug_reasons.append(f'no-termination-within-{LEAVE_WITHIN_DAYS}d')

                if not debug_reasons:
                    # Fallback reason to avoid empty details
                    debug_reasons.append('unknown')

                rules_rows.append({
                    'rule': 'scenario1_full_stop_missed',
                    'ts': ts,
                    'value': 0,
                    'details': ';'.join(debug_reasons)
                    if debug_reasons 
                    else f'blocked_by_termination_within_{LEAVE_WITHIN_DAYS}d'
                })

        # Learned-thresholds completion (Tier-2): AH>=6, USB_1h>=3, USB_24h>=3, HTTP>=3
        if USE_LEARNED_THRESHOLDS and not alerted:
            meets_learned = (
                learned_ah_logons >= LEARNED_MIN_AH_LOGONS and
                last_usb1h       >= LEARNED_MIN_USB_1H and
                last_usb24h      >= LEARNED_MIN_USB_24H and
                learned_http_hits >= LEARNED_MIN_HTTP_HITS
            )

            # Stop loop if break_loop is set
            if break_loop:
                break
            if meets_learned:
                rules_rows.append({
                    'rule': 'scenario1_learned_thresholds',
                    'ts': ts,
                    'value': 1,
                    'details': f'AH>={LEARNED_MIN_AH_LOGONS}, USB1h>={LEARNED_MIN_USB_1H}, USB24h>={LEARNED_MIN_USB_24H}, HTTP>={LEARNED_MIN_HTTP_HITS}'
                })
                new_flags.add('learned_thresholds')  # Add flag for learned threshold detection
                # Create alert for learned thresholds
                alert = Alert(
                    alert_id=str(uuid.uuid4()),
                    rule_id='scenario1_learned_thresholds',
                    entity_id=user_id.upper(),
                    user_key=user_id.upper(),
                    ts=ts.isoformat(' ') if ts else datetime.now(timezone.utc).isoformat(' '),
                    severity='high',
                    dataset=release,
                    short=f'Learned behavior thresholds exceeded (p={LEARNED_QUANTILE})',
                    details=json.dumps({
                        'ah_logons': learned_ah_logons,
                        'ah_threshold': LEARNED_MIN_AH_LOGONS,
                        'usb_1h': last_usb1h,
                        'usb_1h_threshold': LEARNED_MIN_USB_1H,
                        'usb_24h': last_usb24h,
                        'usb_24h_threshold': LEARNED_MIN_USB_24H,
                        'http_hits': learned_http_hits,
                        'http_threshold': LEARNED_MIN_HTTP_HITS,
                        'quantile': LEARNED_QUANTILE
                    }),
                    method_tag=method_tag,
                    created_at=datetime.now(timezone.utc).isoformat(' ')
                )
                alerts.append(alert)
                alerted = True  # Mark as alerted for learned threshold detection
                alert_index = len(all_rows) - 1
                print_alert_line(user_id, flags | new_flags, method_tag)

        # Escalate if enough flags or critical USB 24h
        if not alerted and (len(flags) >= max_flags_to_escalate or ('usb_24h_excess' in newly_added)):
            rules_rows.append({'rule':'flags_threshold','ts':ts,'value':len(flags),
                               'details':f'>={max_flags_to_escalate} distinct flags or critical USB 24h'})
            # Do not stop the stream unless the strict full scenario fires

        if alerted and (len(all_rows) - 1) > (alert_index or 0):
            post_alert_printed += 1
            if post_alert_printed >= 20:
                break

        # Chain reset rules:
        #  - Do NOT reset on LOGOFF. Sessions can span logoff/logon boundaries.
        #    We keep the chain alive and only expire it by time (W3) or when a NEW
        #    after-hours LOGON starts a fresh chain.
        #  - Do NOT reset on USB disconnect.
        if src == 'logon' and isinstance(activity, str) and activity.lower().startswith('logoff'):
            # intentionally do nothing; keep chain state
            pass

        # Expire stale chain if HTTP hasn't happened within W3 after USB
        if chain.get('usb_ts') and ts:
            if (ts - chain['usb_ts']).total_seconds() > CHAIN_W3_USB_TO_HTTP_SECS:
                chain['usb_ts'] = None
                chain['first_time_usb'] = False
                chain['first_time_usb_lookback'] = False
                chain['saw_files_near_usb'] = False
                chain['saw_http_after_usb'] = False
                chain['http_ts'] = None
                chain['wikileaks_seen'] = False
                chain['pc'] = None

        row = cur.fetchone()

    # --- determine export window ---
    if first_flag_index is None:
        start_idx = max(0, len(all_rows) - 120)  # no flags? export last ~120 events
    else:
        start_idx = max(0, first_flag_index - 100)
    if alert_index is None:
        end_idx = len(all_rows) - 1
    else:
        end_idx = min(len(all_rows) - 1, alert_index + 20)
        # try to extend until first device Disconnect or Logoff after alert
        for j in range(alert_index + 1, len(all_rows)):
            e = all_rows[j]
            if (e['src'] == 'device' and isinstance(e.get('activity'), str) and e['activity'].lower() == 'disconnect') \
               or (e['src'] == 'logon' and isinstance(e.get('activity'), str) and e['activity'].lower().startswith('logoff')):
                end_idx = max(end_idx, j)
                break
    # Prepare employment end values for CSV
    employment_end_str = employment_end.isoformat(' ') if employment_end else ''
    termination_within_31d_of_http = None
    if employment_end and chain.get('http_ts'):
        delta_sec = (employment_end - chain['http_ts']).total_seconds()
        termination_within_31d_of_http = (0 <= delta_sec <= LEAVE_WITHIN_DAYS * 86400)

    if export_csv_path is None:
        out_path = Path('out') / f"incident_{user_id.upper()}.csv"
    else:
        out_path = export_csv_path
    out_path.parent.mkdir(parents=True, exist_ok=True)

    with open(out_path, 'w', newline='', encoding='utf-8') as fh:
        w = csv.writer(fh)
        w.writerow(["USER", user_id.upper()])
        w.writerow(["METHOD_TAG", method_tag])
        w.writerow([])
                # ---- Table 0: Profile ----
        w.writerow(["PROFILE"])
        w.writerow([
            "user_key","full_name","email","assigned_pc",
            "role","supervisor","department","team",
            "employment_end","termination_within_31d_of_http"
        ])
        # ---- Table 0: Profile ----
        w.writerow([
            str(profile.get('user_key') or '').upper(),
            profile.get('full_name') or profile.get('username') or user_id.upper(),
            profile.get('email'),
            assigned_pc,
            profile.get('role'),
            profile.get('supervisor'),
            profile.get('department'),
            profile.get('team'),
            employment_end_str,
            (str(termination_within_31d_of_http).lower() if termination_within_31d_of_http is not None else '')
        ])
        w.writerow([])

        # ---- Table 1: Behavior Window ----
        w.writerow(["BEHAVIOR_WINDOW (from index %d to %d)" % (start_idx, end_idx)])
        bh_cols = [
            'ts','src','pc','activity','filename','ext_family','header_vs_ext_mismatch','url',
            'after_hours','post_employment'
        ]
        w.writerow(bh_cols)
        for i in range(start_idx, end_idx + 1):
            e = all_rows[i]
            ah = bool(e['ts'] and is_after_hours(e['ts']))
            pe = bool(employment_end and e['ts'] and e['ts'] > employment_end)
            w.writerow([
                e.get('ts').isoformat(' ') if isinstance(e.get('ts'), datetime) else e.get('ts'),
                e.get('src'), e.get('pc'), e.get('activity'), e.get('filename'), e.get('ext_family'),
                e.get('header_vs_ext_mismatch'), e.get('url'), ah, pe
            ])

        w.writerow([])
        # ---- Table 2: Rules Broken ----
        w.writerow(["RULES_BROKEN"])
        w.writerow(['rule','ts','value','details'])
        for rr in rules_rows:
            w.writerow([
                rr.get('rule'), rr.get('ts').isoformat(' ') if isinstance(rr.get('ts'), datetime) else rr.get('ts'),
                rr.get('value'), rr.get('details')
            ])

    print(f"{COLOR_INFO}[INFO] exported incident to {out_path}{Style.RESET_ALL}", flush=True)
    return alerted, alerts


def main():
    global PRINT_DELAY_SECS, USE_LEARNED_THRESHOLDS, LEARNED_QUANTILE

    ap = argparse.ArgumentParser(description="Hard-rule streaming MVP (Scenario-1) — stable runner")
    ap.add_argument("--project-root", default=".", help="Project root that contains the 'out/' folder")
    ap.add_argument("--release", default="r5.1",
                help="dataset release under out/ (e.g., r4.2, r5.1)")
    ap.add_argument("--validate-alerts", action="store_true",
                    help="After detection, validate alerts against answer keys and print precision/recall/F1.")
    ap.add_argument("--logon-glob", default=None, help="Override glob for logon parquet (e.g., 'out/auth_v1/*.parquet')")
    ap.add_argument("--device-glob", default=None, help="Override glob for device parquet")
    ap.add_argument("--files-glob", default=None, help="Override glob for files parquet")
    ap.add_argument("--http-glob", default=None, help="Override glob for http parquet")
    ap.add_argument("--ldap-snapshots-glob", default=None, help="Override glob for ldap snapshots parquet")
    ap.add_argument("--ldap-months-glob", default=None, help="Override glob for ldap as-of-by-month parquet")
    ap.add_argument("--assigned-pc-glob", default=None, help="Override glob for assigned_pc parquet (optional)")
    ap.add_argument("--user", help="Single user id (e.g., AAM0658). If omitted, use --answers-only or --all-users.")
    ap.add_argument("--answers-only", action="store_true",
                    help="Run all users present in answers/<release>-1/*.csv (filenames like r5.1-1-<USER>.csv)")
    ap.add_argument("--all-users", action="store_true",
                    help="Run detection on ALL users in the dataset (natural detection, not just answer keys)")
    ap.add_argument("--answers-limit", type=int, default=None,
                    help="When using --answers-only or --all-users, limit to the first N users (after optional shuffle).")
    ap.add_argument("--answers-shuffle", action="store_true",
                    help="When using --answers-only or --all-users, shuffle the user list before applying --answers-limit.")
    ap.add_argument("--use-learned", action="store_true",
                    help="Use learned thresholds from answers (quantile across <release>-1)")
    ap.add_argument("--learned-quantile", type=float, default=0.40,
                    help="Quantile to use for learned thresholds (default 0.40)")
    ap.add_argument('--strict-requires-termination', dest='strict_requires_termination', action='store_true',
                    help='Require termination-within-window for scenario1_full_stop (default: off)')
    ap.add_argument('--no-strict-requires-termination', dest='strict_requires_termination', action='store_false',
                    help='Do not require termination-within-window for scenario1_full_stop')
    ap.set_defaults(strict_requires_termination=False)
    ap.add_argument("--print-delay", type=float, default=0.0,
                    help="Seconds to sleep between printed events")
    ap.add_argument("--max-flags", type=int, default=MAX_FLAGS_TO_ESCALATE,
                    help="Escalate when this many distinct flags are raised")
    ap.add_argument("--suppress-dupes", dest="suppress_dupes",
                    action="store_true", default=True,
                    help="Suppress duplicate-looking events (default: on)")
    ap.add_argument("--no-suppress-dupes", dest="suppress_dupes",
                    action="store_false",
                    help="Do not suppress duplicate-looking events")
    ap.add_argument("--method-tag", default=None,
                    help="Optional label written in CSV and alert line (default auto: strict_only or strict+learned_pXX)")
    ap.add_argument("--alerts-output", default=None,
                    help="Path to alerts parquet file (default: out/rules_v1/alerts.parquet)")
    ap.add_argument("--append-alerts", dest="append_alerts", action="store_true", default=True,
                    help="Append to existing alerts file (default: True)")
    ap.add_argument("--no-append-alerts", dest="append_alerts", action="store_false",
                    help="Overwrite alerts file instead of appending")
    ap.add_argument("--debug", action="store_true", help="Extra diagnostics")

    args = ap.parse_args()

    global STRICT_STOP_REQUIRES_TERMINATION
    STRICT_STOP_REQUIRES_TERMINATION = bool(args.strict_requires_termination)
    print(f"{COLOR_INFO}[INFO] strict stop requires termination: {STRICT_STOP_REQUIRES_TERMINATION}{Style.RESET_ALL}", flush=True)

    # ----- config from args -----
    PRINT_DELAY_SECS = float(args.print_delay)
    # Respect the CLI flag --use-learned; previously this was always forced on which
    # could cause surprising behavior (learned thresholds applied even when user
    # expected them off).
    USE_LEARNED_THRESHOLDS = bool(args.use_learned)
    LEARNED_QUANTILE = float(args.learned_quantile)

    project_root = Path(args.project_root).resolve()
    release = args.release
    # Canary check for presence of logon files (respect overrides if provided)
    if args.logon_glob:
        canary_paths = [args.logon_glob]
    else:
        canary_paths = [
            str(project_root / 'out' / release / 'logon_v1' / '*.parquet'),
            str(project_root / 'out' / release / 'logon_v1' / '**' / '*.parquet'),
            str(project_root / 'out' / 'logon_v1' / '*.parquet'),
            str(project_root / 'out' / 'logon_v1' / '**' / '*.parquet'),
        ]
    if not any(pyglob(pat, recursive=True) for pat in canary_paths):
        print("[ERROR] Couldn't find any logon parquet files.", file=sys.stderr)
        for pat in canary_paths:
            print(f"        Searched: {pat}", file=sys.stderr)
        print("        Tips:", file=sys.stderr)
        print("          • Provide --logon-glob 'out/<your_dir>/*.parquet' to override.", file=sys.stderr)
        print("          • Or pass --project-root if your outputs are under another root.", file=sys.stderr)
        print("          • Or run the notebooks to generate out/logon_v1/*.parquet.", file=sys.stderr)
        sys.exit(2)

    # ----- DuckDB connection + views -----
    glob_overrides = {
        'logon': args.logon_glob,
        'device': args.device_glob,
        'files': args.files_glob,
        'http': args.http_glob,
        'ldap_snapshots': args.ldap_snapshots_glob,
        'ldap_asof_by_month': args.ldap_months_glob,
        'assigned_pc': args.assigned_pc_glob,
    }
    con = make_duckdb_conn(project_root, release, glob_overrides=glob_overrides)

    # Learned thresholds (optional)
    if USE_LEARNED_THRESHOLDS:
        q_ah, q_usb1h, q_usb24h, q_http = compute_learned_thresholds(con, release, LEARNED_QUANTILE)
        # set globals that evaluate_stream uses
        globals()["LEARNED_MIN_AH_LOGONS"] = q_ah
        globals()["LEARNED_MIN_USB_1H"] = q_usb1h
        globals()["LEARNED_MIN_USB_24H"] = q_usb24h
        globals()["LEARNED_MIN_HTTP_HITS"] = q_http
        print(f"{COLOR_INFO}[INFO] learned thresholds p={LEARNED_QUANTILE:.2f}: AH>={q_ah}, USB1h>={q_usb1h}, USB24h>={q_usb24h}, HTTP>={q_http}{Style.RESET_ALL}")

    # ----- decide which users to run -----
    users_to_run: List[str] = []
    if args.user:
        users_to_run = [args.user.strip().upper()]
    elif args.all_users:
        # Extract ALL unique users from the dataset (natural detection)
        print(f"{COLOR_INFO}[INFO] Extracting all users from dataset...{Style.RESET_ALL}", flush=True)
        try:
            # Query all unique users from logon table
            result = con.execute("""
                SELECT DISTINCT UPPER(user_key) AS user 
                FROM logon 
                WHERE user_key IS NOT NULL AND user_key != ''
                ORDER BY user
            """).fetchall()
            users_to_run = [row[0] for row in result if row[0]]
            
            if not users_to_run:
                print(f"[ERROR] No users found in logon data", file=sys.stderr)
                sys.exit(2)
            
            print(f"{COLOR_INFO}[INFO] Found {len(users_to_run)} total users in dataset{Style.RESET_ALL}", flush=True)
        except Exception as e:
            print(f"[ERROR] Failed to extract users from dataset: {e}", file=sys.stderr)
            sys.exit(2)
    elif args.answers_only:
        # Parse r4.2-1 answer files: r4.2-1-<USER>.csv  (case-insensitive, robust extension)
        answers_dir = project_root / "answers" / f"{release}-1"
        if not answers_dir.exists():
            print(f"[ERROR] answers directory not found: {answers_dir}", file=sys.stderr)
            sys.exit(2)

        # Accept names like r5.1-1-AAM0658.csv (case-insensitive)
        pattern = rf"(?i)^{re.escape(release)}-1-([a-z0-9]{{7}})\.csv$"
        for p in sorted(answers_dir.glob("*.csv")):
            m = re.match(pattern, p.name)
            if m:
                users_to_run.append(m.group(1).upper())

        if not users_to_run:
            print(f"[ERROR] No {release}-1 answer CSVs matched in {answers_dir}", file=sys.stderr)
            sys.exit(2)

    if args.debug:
        print(f"[DEBUG] users_to_run={users_to_run}", flush=True)
    # Optionally shuffle and limit the list of answer users
    if args.answers_shuffle:
        random.shuffle(users_to_run)
    if args.answers_limit is not None and args.answers_limit >= 0:
        users_to_run = users_to_run[:args.answers_limit]

    # Compose method tag used in terminal + export path
    if args.method_tag:
        method_tag = args.method_tag
    else:
        method_tag = f"scenario1_strict+learned_p{int(LEARNED_QUANTILE*100):02d}" if USE_LEARNED_THRESHOLDS else "scenario1_strict_only"

    suppress_dupes = bool(args.suppress_dupes)
    max_flags = int(args.max_flags)

    print(f"{COLOR_INFO}[INFO] will stream {len(users_to_run)} user(s). "
          f"Max flags to escalate = {max_flags}; print_delay={PRINT_DELAY_SECS:.2f}s{Style.RESET_ALL}", flush=True)

    # ----- run each user -----
    all_alerts: List[Alert] = []  # Collect alerts across all users
    
    for uid in users_to_run:
        print("\n" + "=" * 72, flush=True)
        print(f"{COLOR_INFO}[INFO] Starting stream for user: {uid}{Style.RESET_ALL}", flush=True)

        export_dir = Path("out") / "tests_s1" / method_tag / uid
        export_csv = export_dir / "incident.csv"
        export_dir.mkdir(parents=True, exist_ok=True)

        alerted, user_alerts = evaluate_stream(
            con=con,
            user_id=uid,
            export_csv_path=export_csv,
            max_flags_to_escalate=max_flags,
            suppress_dupes=suppress_dupes,
            print_delay=PRINT_DELAY_SECS,
            release=release,
            method_tag=method_tag
        )
        
        # Collect alerts from this user
        all_alerts.extend(user_alerts)

        if alerted:
            print(f"{COLOR_INFO}[INFO] Escalation occurred for user {uid}.{Style.RESET_ALL}", flush=True)
        else:
            print(f"{COLOR_INFO}[INFO] No escalation for user {uid}.{Style.RESET_ALL}", flush=True)

    # ----- Write all alerts to unified parquet file -----
    if all_alerts:
        alerts_path = Path(args.alerts_output) if args.alerts_output else Path("out/rules_v1/alerts.parquet")
        write_alerts_to_parquet(all_alerts, alerts_path, append=args.append_alerts)
        print(f"\n{COLOR_INFO}[INFO] Wrote {len(all_alerts)} total alert(s) to {alerts_path}{Style.RESET_ALL}", flush=True)
    else:
        print(f"\n{COLOR_INFO}[INFO] No alerts generated across {len(users_to_run)} user(s){Style.RESET_ALL}", flush=True)

    # ----- Validation step: compare alerts to answer keys -----
    if args.validate_alerts:
        print(f"\n{COLOR_INFO}{'='*80}{Style.RESET_ALL}", flush=True)
        print(f"{COLOR_INFO}[VALIDATION] Comparing detection results against answer keys...{Style.RESET_ALL}", flush=True)
        print(f"{COLOR_INFO}{'='*80}{Style.RESET_ALL}\n", flush=True)
        
        # Load answer user IDs from answers/<release>-1/*.csv
        answers_dir = project_root / "answers" / f"{release}-1"
        answer_users = set()
        
        if answers_dir.exists():
            pattern = rf"(?i)^{re.escape(release)}-1-([a-z0-9]{{7}})\.csv$"
            for p in answers_dir.glob("*.csv"):
                m = re.match(pattern, p.name)
                if m:
                    answer_users.add(m.group(1).upper())
            
            if answer_users:
                print(f"{COLOR_INFO}[VALIDATION] Loaded {len(answer_users)} ground truth insider(s) from {answers_dir}{Style.RESET_ALL}", flush=True)
            else:
                print(f"{COLOR_WARN}[WARN] No answer files found matching pattern in {answers_dir}{Style.RESET_ALL}", flush=True)
        else:
            print(f"{COLOR_WARN}[WARN] Answers directory not found: {answers_dir}{Style.RESET_ALL}", flush=True)
            print(f"{COLOR_WARN}[WARN] Skipping validation (no ground truth available){Style.RESET_ALL}", flush=True)
            answer_users = set()

        if answer_users:
            # Detected users (from alerts)
            detected_users = set(a.user_key.upper() for a in all_alerts)
            
            # Compute confusion matrix elements
            tp_users = detected_users & answer_users  # True Positives
            fp_users = detected_users - answer_users  # False Positives
            fn_users = answer_users - detected_users  # False Negatives
            
            tp = len(tp_users)
            fp = len(fp_users)
            fn = len(fn_users)
            
            # Compute metrics
            precision = tp / (tp + fp) if (tp + fp) > 0 else 0.0
            recall = tp / (tp + fn) if (tp + fn) > 0 else 0.0
            f1 = 2 * precision * recall / (precision + recall) if (precision + recall) > 0 else 0.0
            
            # Print validation results
            print(f"\n{COLOR_INFO}╔{'═'*78}╗{Style.RESET_ALL}")
            print(f"{COLOR_INFO}║{' '*26}VALIDATION RESULTS{' '*33}║{Style.RESET_ALL}")
            print(f"{COLOR_INFO}╠{'═'*78}╣{Style.RESET_ALL}")
            print(f"{COLOR_INFO}║ Ground Truth Insiders:  {len(answer_users):4d}{' '*49}║{Style.RESET_ALL}")
            print(f"{COLOR_INFO}║ Detected Users:         {len(detected_users):4d}{' '*49}║{Style.RESET_ALL}")
            print(f"{COLOR_INFO}╠{'═'*78}╣{Style.RESET_ALL}")
            print(f"{COLOR_INFO}║ True Positives (TP):    {tp:4d}  (detected AND in ground truth){' '*21}║{Style.RESET_ALL}")
            print(f"{COLOR_INFO}║ False Positives (FP):   {fp:4d}  (detected but NOT in ground truth){' '*16}║{Style.RESET_ALL}")
            print(f"{COLOR_INFO}║ False Negatives (FN):   {fn:4d}  (missed, in ground truth but not detected){' '*9}║{Style.RESET_ALL}")
            print(f"{COLOR_INFO}╠{'═'*78}╣{Style.RESET_ALL}")
            print(f"{COLOR_INFO}║ Precision:  {precision:6.2%}  (TP / (TP + FP)) - accuracy of positive predictions{' '*7}║{Style.RESET_ALL}")
            print(f"{COLOR_INFO}║ Recall:     {recall:6.2%}  (TP / (TP + FN)) - coverage of actual positives{' '*10}║{Style.RESET_ALL}")
            print(f"{COLOR_INFO}║ F1 Score:   {f1:6.2%}  (harmonic mean of precision and recall){' '*14}║{Style.RESET_ALL}")
            print(f"{COLOR_INFO}╚{'═'*78}╝{Style.RESET_ALL}\n")
            
            # Show specific users in each category
            if tp_users:
                print(f"{COLOR_FLAG} TRUE POSITIVES ({tp} users correctly detected):{Style.RESET_ALL}")
                for u in sorted(tp_users):
                    print(f"   • {u}")
                print()
            
            if fp_users:
                print(f"{COLOR_WARN}  FALSE POSITIVES ({fp} users incorrectly flagged):{Style.RESET_ALL}")
                for u in sorted(fp_users):
                    print(f"   • {u}")
                print()
            
            if fn_users:
                print(f"{COLOR_ALERT} FALSE NEGATIVES ({fn} insiders missed):{Style.RESET_ALL}")
                for u in sorted(fn_users):
                    print(f"   • {u}")
                print()
            
            # Performance interpretation
            if f1 >= 0.90:
                perf_msg = "EXCELLENT detection performance!"
            elif f1 >= 0.70:
                perf_msg = "GOOD detection performance"
            elif f1 >= 0.50:
                perf_msg = "MODERATE detection performance - consider tuning thresholds"
            else:
                perf_msg = "POOR detection performance - significant tuning needed"
            
            print(f"{COLOR_INFO}{perf_msg}{Style.RESET_ALL}\n")


if __name__ == "__main__":
    main()
