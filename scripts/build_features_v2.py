#!/usr/bin/env python3
"""
features_v2 builder — final-behavior overview (plain English)

Detectors ingest a 14‑day window of rows for each user on day D:
  • Detector window: rows for days [D−13, D] inclusive.

What each row already carries (so detectors don’t recompute history):
  • ah_rate_1d:        global after‑hours rate for that day
                        = (sum of per‑domain AH events) / (sum of per‑domain total events)
                        where per‑domain AH events ≈ total_events_domain * after_hours_rate_domain.
  • usb_count_1d:      USB connects on that day (integer).
  • ah_rate_trend:     mean of ah_rate_1d over the last 7 days including today [d−6..d].
  • ah_rate_baseline:  mean of ah_rate_1d over the prior 30 days excluding the last 7 [d−37..d−8].
  • usb_count_trend:   sum of usb_count_1d over the last 7 days including today [d−6..d].
  • usb_count_baseline:sum of usb_count_1d over the prior 30 days excluding the last 7 [d−37..d−8].
  • Evidence fields come from v1 as‑is (no duplication), e.g.:
      http_n_wikileaks, http_n_dropbox, … (use directly in rules).

Novelty signals (consumed by rules and anomaly logic to pick up on spikes):
  • ah_novel:  ah_rate_trend > ah_rate_baseline + 1e−6   (tiny epsilon avoids float noise)
  • usb_novel: usb_count_trend > usb_count_baseline

Contract:
  • Primary key remains (user_key, day).
  • Output keeps all v1 columns and appends the v2 columns listed above.
  • Baseline excludes the most recent 7 days so a fresh surge doesn’t inflate its own “normal.”

Safe to run repeatedly:
  • Overwrites out/<REL>/features_v2/daily_user/daily_user.parquet.
  • Never mutates v1 artifacts.
"""
import json
from pathlib import Path
import pandas as pd
import numpy as np

# Standard libs + pandas/numpy only. No project-internal imports to keep this builder isolated.

# read_release()
# Reads the single-line text in release.txt (e.g., "r5.1") so we know which
# out/<REL>/ path to read from and write to. Exits loudly if the file is missing.
def read_release():
    p = Path("release.txt")
    if not p.exists(): raise SystemExit("release.txt not found")  # Fail fast: builder can’t run without knowing which release to use.
    return p.read_text().strip()

# main()
# 1) Resolve input/output paths from the current REL.
# 2) Load the v1 daily_user parquet (one row per user per calendar day).
# 3) Add “v2” columns:
#    - Per-day basics: ah_rate_1d, usb_1d, evidence passthroughs.
#    - Rolling windows per user to compute 7d trend and 30d excl-7 baseline.
# 4) Write out the v2 parquet. Print a tiny JSON summary.
def main():
    rel = read_release()
    # Input: v1 features already built by your ETL
    # Output: a sibling v2 folder with appended, richer columns
    in_p = Path(f"out/{rel}/features_v1/daily_user/daily_user.parquet")
    out_dir = Path(f"out/{rel}/features_v2/daily_user")
    out_dir.mkdir(parents=True, exist_ok=True)
    out_p = out_dir / "daily_user.parquet"
    if not in_p.exists():
        raise SystemExit(f"Missing input: {in_p}")
    df = pd.read_parquet(in_p)
    # -----------------------------
    # v2 BASE COLUMNS (per calendar day)
    # -----------------------------
    # We keep these super simple: no history, just today’s aggregates and evidence.
    # Later, we add rolling windows to compute short-term trends and older baselines.
    # Ensure day is a datetime for later time-aware ops
    df["day"] = pd.to_datetime(df["day"], errors="coerce")

    # Keep original key set so output matches v1 exactly (no extra asfreq days)
    orig_keys = df[["user_key","day"]].copy()

    # Compute global after-hours rate for the day from per-domain totals and rates.
    total_cols = {
        "logon": "logon_n_logon",
        "device": "device_n_device_events",
        "file": "file_n_file_events",
        "http": "http_n_http",
        "email": "email_n_email_sent",
    }
    rate_cols = {
        "logon": "logon_after_hours_rate",
        "device": "device_after_hours_rate",
        "file": "file_after_hours_rate",
        "http": "http_after_hours_rate",
        "email": "email_after_hours_rate",
    }
    # ensure all referenced columns exist
    for c in total_cols.values():
        if c not in df.columns: df[c] = 0
        df[c] = df[c].fillna(0)
    for c in rate_cols.values():
        if c not in df.columns: df[c] = 0.0
        df[c] = df[c].fillna(0.0)
    # estimated AH events per domain = total * rate
    ah_estimates = []
    totals = []
    for dom in total_cols:
        tcol = total_cols[dom]
        rcol = rate_cols[dom]
        est = df[tcol] * df[rcol]
        ah_estimates.append(est)
        totals.append(df[tcol])
    total_ah = np.sum(ah_estimates, axis=0)
    total_all = np.sum(totals, axis=0)
    df["ah_rate_1d"] = np.where(total_all > 0, total_ah / total_all, 0.0)

    # USB activity (connects) for the day. This remains numeric; detectors can threshold it.
    if "device_n_usb_connects" not in df.columns:
        df["device_n_usb_connects"] = 0
    df["usb_count_1d"] = df["device_n_usb_connects"].fillna(0).astype("int64")

    # -----------------------------
    # ROLLING WINDOWS (per user)
    # -----------------------------
    # We now compute:
    #   - ah_rate_7d_mean: mean of AH over the last 7 days INCLUDING today [d-6..d]
    #   - ah_rate_prev30_ex7_mean: baseline from [d-37..d-8], which keeps recent spikes
    #     out of the baseline so “novelty” isn’t diluted by its own surge.
    #   - usb_days_7d / usb_days_prev30_ex7: counts of days with any USB activity in
    #     those same windows, used later for novelty flags.
    #
    # Implementation notes:
    #   • We dedupe any accidental duplicate rows per calendar day before rolling, so a
    #     7-day count can never exceed 7, etc.
    #   • We use asfreq("D") so time-based rolling windows behave correctly even if some
    #     calendar days are missing for a given user.
    def _add_windows_per_user(u: pd.DataFrame) -> pd.DataFrame:
        
        # Put rows in calendar order, one per day, then give ourselves a proper daily index.
        u = u.sort_values("day")
        # collapse any accidental duplicates per calendar day before time-based rolling
        u = u.groupby("day", as_index=False).last()
        u = u.set_index("day").asfreq("D")
        # Fill for safe math; these are rates/counts so zero is a sane default.
        u["ah_rate_1d"] = u["ah_rate_1d"].fillna(0)
        u["usb_count_1d"] = u["usb_count_1d"].fillna(0)
        
        # Short horizon: last 7 days INCLUDING today -> [d-6 .. d]
        u["ah_rate_trend"] = u["ah_rate_1d"].rolling("7D", closed="both").mean()
        u["ah_rate_trend"] = u["ah_rate_trend"].clip(lower=0, upper=1)
        
        # “Left” windows exclude today (d). We build a 37-day and a 7-day mean,
        # then subtract to isolate the 30 days BEFORE the last week.
        #   ah_prev37_left  -> mean over [d-37 .. d-1]
        #   ah_prev7_left   -> mean over [d-7  .. d-1]
        # Baseline window we want is [d-37 .. d-8]  (30 days)
        ah_prev37_left = u["ah_rate_1d"].rolling("37D", closed="left").mean()   # [d-37 .. d-1]
        ah_prev7_left  = u["ah_rate_1d"].rolling("7D",  closed="left").mean()   # [d-7  .. d-1]
    
        # Baseline mean over [d-37 .. d-8]:
        #   sum([d-37 .. d-1]) - sum([d-7 .. d-1])  == sum([d-37 .. d-8])
        #   divide by 30 to convert back to a mean
        u["ah_rate_baseline"] = ((ah_prev37_left * 37.0) - (ah_prev7_left * 7.0)) / 30.0
        u["ah_rate_baseline"] = u["ah_rate_baseline"].clip(lower=0, upper=1)
    
        # USB counts
        # Trend is 7-day SUM including today: [d-6 .. d]
        u["usb_count_trend"] = u["usb_count_1d"].rolling("7D",  closed="both").sum()  # [d-6 .. d]
    
        # Baseline is prior 30 days EXCLUDING last 7:
        #   usb_sum_prev37_left -> sum over [d-37 .. d-1]
        #   usb_sum_prev7_left  -> sum over [d-7  .. d-1]
        #   baseline sum        -> sum([d-37 .. d-8]) = prev37_left - prev7_left
        usb_sum_prev37_left = u["usb_count_1d"].rolling("37D", closed="left").sum()   # [d-37 .. d-1]
        usb_sum_prev7_left  = u["usb_count_1d"].rolling("7D",  closed="left").sum()   # [d-7  .. d-1]
        u["usb_count_baseline"] = (usb_sum_prev37_left - usb_sum_prev7_left)          # [d-37 .. d-8]
    
        # Bounds: keep sums non-negative
        u["usb_count_trend"]    = u["usb_count_trend"].clip(lower=0)
        u["usb_count_baseline"] = u["usb_count_baseline"].clip(lower=0)

        # === Novelty flags ===
        # Recent activity exceeds the user's long-term baseline (i.e., a behavioral spike).
        # Rates use a tiny epsilon to ignore float jitter; counts are integers.
        eps = 1e-6
        u["ah_novel"]  = (u["ah_rate_trend"]  > (u["ah_rate_baseline"].fillna(0) + eps)).astype("int8")
        u["usb_novel"] = (u["usb_count_trend"] >  u["usb_count_baseline"].fillna(0)).astype("int8")

        return u.reset_index()
    
    # Apply per-user windows without including grouping columns in the function input,
    # then reattach the group key as a column so downstream code can rely on (user_key, day).
    df = (
        df.groupby("user_key", group_keys=False)
          .apply(lambda u: _add_windows_per_user(u).assign(user_key=u.name), include_groups=False)
    )

    # Restrict output rows to the exact (user_key, day) keys present in v1.
    # Rolling windows used asfreq to compute trends/baselines across gaps,
    # but we only emit rows that existed in v1 daily_user.
    df = orig_keys.merge(df, on=["user_key","day"], how="left")

    # Reorder: keys, then original v1 cols, then new v2 cols for readability
    v2_cols = ["ah_rate_1d","ah_rate_trend","ah_rate_baseline",
            "usb_count_1d","usb_count_trend","usb_count_baseline",
            "ah_novel","usb_novel"]
    base_keys = ["user_key","day"]
    v1_cols = [c for c in df.columns if c not in set(base_keys + v2_cols)]
    df = df[base_keys + v1_cols + v2_cols]

    # Write the enriched v2 daily_user. This file is safe to overwrite on reruns.
    df.to_parquet(out_p, index=False)
    # Tiny JSON summary so callers (or CI) can sanity check output at a glance.
    print(json.dumps({"rows": len(df), "out_path": str(out_p)}, indent=2))

if __name__ == "__main__":
    main()