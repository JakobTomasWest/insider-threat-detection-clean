#!/usr/bin/env python3
"""
Quick Scenario-1 rule eval harness.

- Reads features_v2/daily_user.parquet for the current release.
- Loops over users and days, building 14-day windows like run_loop.
- Calls src.detector.rules.check(...) for each (user, day).
- Prints:
    * total alert count
    * counts per reason
    * distinct users per reason
"""

from __future__ import annotations

import argparse
import collections
from pathlib import Path
import sys

import pandas as pd

# Ensure project root is on sys.path so `import src...` works
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.detector import rules


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Evaluate Scenario-1 rules over a date range.")
    p.add_argument(
        "--start",
        default="2010-01-01",
        help="Start date (inclusive), YYYY-MM-DD [default: 2010-01-01]",
    )
    p.add_argument(
        "--end",
        default="2010-03-31",
        help="End date (inclusive), YYYY-MM-DD [default: 2010-03-31]",
    )
    p.add_argument(
        "--limit-users",
        type=int,
        default=None,
        help="Optional cap on number of users to evaluate (for speed).",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()

    rel = Path("release.txt").read_text().strip()
    daily_path = Path("out") / rel / "features_v2" / "daily_user" / "daily_user.parquet"

    print(f"[info] Release: {rel}")
    print(f"[info] Loading {daily_path}")
    df = pd.read_parquet(daily_path)

    # Normalize day column
    df["day"] = pd.to_datetime(df["day"])

    # Apply date filter
    start = pd.to_datetime(args.start)
    end = pd.to_datetime(args.end)
    mask = df["day"].between(start, end)
    df = df.loc[mask].sort_values(["user_key", "day"])

    print(f"[info] Rows in range [{args.start}, {args.end}]: {len(df)}")
    if df.empty:
        print("[warn] No rows in that date range — nothing to evaluate.")
        return

    # Optionally limit users for speed
    all_users = df["user_key"].unique()
    if args.limit_users is not None:
        users = all_users[: args.limit_users]
        df = df[df["user_key"].isin(users)]
        print(f"[info] Limited to first {len(users)} users")
    else:
        users = all_users

    alerts = []

    # Walk per-user, per-day and build 14-day windows
    for user in users:
        g = df[df["user_key"] == user]
        if g.empty:
            continue

        days = g["day"].unique()
        for cur_day in days:
            # 14-day window: [cur_day - 13, cur_day]
            window_start = cur_day - pd.Timedelta(days=13)
            window = g[g["day"].between(window_start, cur_day)].copy()
            if window.empty:
                continue

            out = rules.check(
                window,
                day=cur_day.date().isoformat(),
                user_key=user,
            )
            if out:
                alerts.extend(out)

    print(f"[info] Total alerts: {len(alerts)}")

    if not alerts:
        print("[info] No alerts fired in this range.")
        return

    # Summaries
    reason_counts = collections.Counter(a["reason"] for a in alerts)
    reason_user_pairs = collections.Counter((a["reason"], a["user_key"]) for a in alerts)

    print("\nReason counts:")
    for r, c in reason_counts.most_common():
        print(f"  {r}: {c} alerts")

    print("\nDistinct users per reason:")
    for r in reason_counts:
        users_for_reason = {u for (r2, u) in reason_user_pairs if r2 == r}
        print(f"  {r}: {len(users_for_reason)} users")

    # Show a couple of example alerts for debugging
    print("\nSample alerts (up to 5):")
    for a in alerts[:5]:
        ev = a.get("evidence", {})
        print(
            f"  day={a['day']} user={a['user_key']} reason={a['reason']} "
            f"ah_novel_7d={ev.get('ah_novel_7d')} "
            f"usb_novel_7d={ev.get('usb_novel_7d')} "
            f"usb_total_7d={ev.get('usb_total_7d')}"
        )


if __name__ == "__main__":
    main()