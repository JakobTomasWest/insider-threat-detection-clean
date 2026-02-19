#!/usr/bin/env python3
"""
Daily layer sanity checks for features_v1/daily_user.

Checks each present per-domain daily parquet (logon/device/file/http/email) and the union:
- Required keys exist: user_key (lowercase), day (DATE)
- Duplicate key rows (user_key, day)
- Null rates on keys
- For count columns (n_*): values >= 0
- For rate columns (*_rate): 0 <= value <= 1
- is_active_employee_day in {0,1}
- n_events_post_departure present and non-negative
- Basic row/user counts and date ranges
- Union table has domain-prefixed columns and no dup keys

Usage:
  python3 scripts/daily_check.py [verbose]
"""
from __future__ import annotations
import sys, re
from pathlib import Path
import duckdb

# Minimal required columns per per-domain daily parquet
REQUIRED_PER_DOMAIN = {
    "logon_daily": {"user_key", "day", "n_logon", "after_hours_rate", "is_active_employee_day"},
    "device_daily": {"user_key", "day", "n_device_events", "after_hours_rate", "is_active_employee_day"},
    "file_daily": {"user_key", "day", "n_file_events", "after_hours_rate", "is_active_employee_day"},
    "http_daily": {"user_key", "day", "n_http", "after_hours_rate", "is_active_employee_day"},
    # Email has historically gone missing critical flags; be strict here
    "email_daily": {
        "user_key", "day",
        "n_email_sent",
        "after_hours_rate",
        "is_active_employee_day",
        # these help diagnose directionality and coverage; keep them mandatory
        "n_internal_only", "n_internal_to_external", "n_external_to_internal", "n_external_only"
    },
}

# In the union, each domain must contribute at least these prefixed columns
REQUIRED_UNION_PREFIX = {
    "logon": {"after_hours_rate", "is_active_employee_day"},
    "device": {"after_hours_rate", "is_active_employee_day"},
    "file": {"after_hours_rate", "is_active_employee_day"},
    "http": {"after_hours_rate", "is_active_employee_day"},
    "email": {"after_hours_rate", "is_active_employee_day"},
}

def _missing(required: set[str], actual: list[str]) -> list[str]:
    present = set(actual)
    return sorted([c for c in required if c not in present])

VERBOSE = (len(sys.argv) > 1 and sys.argv[1].lower().startswith("v"))

def p(*a): print(*a, flush=True)

def exists(p: Path) -> bool:
    return p.exists() and p.is_file()

def rel() -> str:
    rp = Path("release.txt")
    if not rp.exists():
        raise SystemExit("No release.txt present.")
    return rp.read_text().strip()

def base_dir(R: str) -> Path:
    return Path("out")/R/"features_v1"/"daily_user"

def q_has_col(con, view, col) -> bool:
    try:
        return bool(con.execute(
            f"SELECT COUNT(*) FROM pragma_table_info('{view}') WHERE name='{col}'"
        ).fetchone()[0])
    except Exception:
        return False

def list_cols(con, view) -> list[str]:
    return [r[0] for r in con.execute(f"SELECT name FROM pragma_table_info('{view}') ORDER BY cid").fetchall()]

def check_table(con, name: str, path: Path) -> dict:
    con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{path.as_posix()}')")
    prob = []

    # Required columns
    if not q_has_col(con, name, "user_key"): prob.append("[FAIL] missing user_key")
    if not q_has_col(con, name, "day"): prob.append("[FAIL] missing day")
    # Strict schema presence check
    cols = list_cols(con, name)
    req = REQUIRED_PER_DOMAIN.get(name, set())
    if req:
        miss = _missing(req, cols)
        if miss:
            prob.append(f"[FAIL] missing required columns: {', '.join(miss)}")
    # Key dupes
    try:
        rows, nd = con.execute(
            f"SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT user_key||'|'||CAST(day AS VARCHAR)) FROM {name}"
        ).fetchone()
        dups = nd
    except Exception as e:
        rows, dups = "?", "?"
        prob.append(f"[WARN] key duplicate check failed: {e}")

    # Users + range
    try:
        users, dmin, dmax = con.execute(
            f"SELECT COUNT(DISTINCT user_key), MIN(day), MAX(day) FROM {name}"
        ).fetchone()
    except Exception:
        users, dmin, dmax = "?", "?", "?"

    # Null key rate
    try:
        null_key = con.execute(
            f"SELECT SUM(CASE WHEN user_key IS NULL OR user_key='' THEN 1 ELSE 0 END) FROM {name}"
        ).fetchone()[0]
        null_key_rate = (null_key / rows) if isinstance(rows, int) and rows else 0
    except Exception:
        null_key_rate = None

    # Column-level sanity
    numeric_counts = [c for c in cols if c.startswith("n_")]
    rate_cols = [c for c in cols if c.endswith("_rate")]
    flags_cols = [c for c in cols if c in ("is_active_employee_day",)]

    # Non-negative counts
    for c in numeric_counts:
        try:
            neg = con.execute(f"SELECT COUNT(*) FROM {name} WHERE {c} < 0").fetchone()[0]
            if neg > 0:
                prob.append(f"[FAIL] {c} has {neg} negative values")
        except Exception as e:
            prob.append(f"[WARN] count check for {c} failed: {e}")

    # Rate bounds
    for c in rate_cols:
        try:
            # NULL after_hours_rate is effectively "missing computation"
            bad = con.execute(
                f"SELECT COUNT(*) FROM {name} WHERE {c} < 0 OR {c} > 1"
            ).fetchone()[0]
            nulls = con.execute(f"SELECT COUNT(*) FROM {name} WHERE {c} IS NULL").fetchone()[0]
            if bad > 0:
                prob.append(f"[FAIL] {c} has {bad} out-of-range values")
            if nulls > 0:
                prob.append(f"[FAIL] {c} has {nulls} NULLs (expected 0)")
        except Exception as e:
            prob.append(f"[WARN] rate check for {c} failed: {e}")

    # Flags in {0,1}
    for c in flags_cols:
        try:
            bad = con.execute(
                f"SELECT COUNT(*) FROM {name} WHERE {c} NOT IN (0,1)"
            ).fetchone()[0]
            if bad > 0:
                prob.append(f"[WARN] {c} has {bad} non-binary values")
        except Exception as e:
            prob.append(f"[WARN] flag check for {c} failed: {e}")

    # Post-departure present and non-negative if present
    if q_has_col(con, name, "n_events_post_departure"):
        try:
            neg = con.execute(
                f"SELECT COUNT(*) FROM {name} WHERE n_events_post_departure < 0"
            ).fetchone()[0]
            if neg > 0:
                prob.append(f"[FAIL] n_events_post_departure has {neg} negative values")
        except Exception as e:
            prob.append(f"[WARN] post-departure check failed: {e}")

    return {
        "name": name,
        "path": str(path),
        "rows": rows,
        "users": users,
        "range": f"[{dmin} .. {dmax}]",
        "dups": dups,
        "null_key_rate": null_key_rate,
        "cols": cols,
        "problems": prob,
    }

def check_union(con, path: Path, present_domains: list[str]) -> dict:
    name = "daily_user"
    con.execute(f"CREATE OR REPLACE VIEW {name} AS SELECT * FROM read_parquet('{path.as_posix()}')")
    prob = []

    # Keys and dupes
    cols = list_cols(con, name)
    if "user_key" not in cols: prob.append("[FAIL] daily_user missing user_key")
    if "day" not in cols: prob.append("[FAIL] daily_user missing day")

    rows, dups = "?", "?"
    try:
        rows, dups = con.execute(
            f"SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT user_key||'|'||CAST(day AS VARCHAR)) FROM {name}"
        ).fetchone()
    except Exception as e:
        prob.append(f"[WARN] union dup-key check failed: {e}")

    # Domain prefix presence
    for dom in present_domains:
        if not any(c.startswith(dom + "_") for c in cols):
            prob.append(f"[FAIL] union missing any '{dom}_*' columns")
    # Domain-prefixed required columns must exist in union
    for dom, req_suffixes in REQUIRED_UNION_PREFIX.items():
        for suf in req_suffixes:
            need = f"{dom}_{suf}"
            if need not in cols:
                prob.append(f"[FAIL] union missing required column: {need}")
    # Basic stats
    try:
        users, dmin, dmax = con.execute(
            f"SELECT COUNT(DISTINCT user_key), MIN(day), MAX(day) FROM {name}"
        ).fetchone()
    except Exception:
        users, dmin, dmax = "?", "?", "?"

    return {
        "name": "daily_user",
        "path": str(path),
        "rows": rows,
        "users": users,
        "range": f"[{dmin} .. {dmax}]",
        "dups": dups,
        "cols": cols,
        "problems": prob,
    }

def main():
    R = rel()
    base = base_dir(R)
    if not base.exists():
        raise SystemExit(f"No daily outputs found under {base}")

    con = duckdb.connect(database=":memory:")

    domains = ["logon","device","file","http","email"]
    present = []
    results = []

    p(f"== Daily checks (release {R}) ==")
    for d in domains:
        dp = base / f"{d}_daily.parquet"
        if exists(dp):
            present.append(d)
            res = check_table(con, f"{d}_daily", dp)
            results.append(res)
        else:
            p(f"[skip] {d}_daily.parquet not found")

    # Union
    up = base / "daily_user.parquet"
    if exists(up):
        res_u = check_union(con, up, present_domains=present)
    else:
        res_u = {"name":"daily_user","path":str(up),"rows":"-","users":"-","range":"[-]","dups":"-","problems":[f"[FAIL] missing {up}"]}

    # Summary table
    p("\n-- Summary --")
    p(f"{'table':<18} {'rows':>10} {'users':>10} {'dups':>8} {'range':>30}   path")
    for r in results + [res_u]:
        p(f"{r['name']:<18} {str(r['rows']):>10} {str(r['users']):>10} {str(r['dups']):>8} {r['range']:>30}   {r['path']}")

    # Problems
    p("\n-- Findings --")
    any_prob = False
    for r in results + [res_u]:
        probs = r.get("problems") or []
        if probs:
            any_prob = True
            p(f"[{r['name']}]")
            for m in probs:
                p("  ", m)
    if not any_prob:
        p("No problems detected.")

    if VERBOSE:
        p("\n-- Columns --")
        for r in results + [res_u]:
            cols = r.get("cols") or []
            if cols:
                p(f"[{r['name']}] " + ", ".join(cols))

    # Exit non-zero if any failures/warnings were recorded
    sys.exit(1 if any_prob else 0)

if __name__ == "__main__":
    main()