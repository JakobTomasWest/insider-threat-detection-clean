"""
MVP 0 — Core Loop Scaffolding (cleaned)

What this is:
  A tiny, reviewable skeleton that simulates the *control flow* of the final pipeline:
    - iterate days,
    - maintain a rolling 14‑day window per user,
    - call three detector hooks,
    - write alerts (NDJSON) in a minimal schema.

Why this exists:
  It proves the loop + window mechanics and the write path before we hook up real data
  (PR #2 swaps placeholders for `daily_user.parquet`).

Usage:
  python -m src.run_loop --start 2010-01-01 --end 2010-01-14 --dry-run
  python -m src.run_loop --start 2010-01-01 --end 2010-01-14
  r5.2: run_loop command for first_day to last_day of activity 
	  python -m src.run_loop --start 2010-01-02 --end 2011-06-02
"""

from __future__ import annotations
import argparse
import json
import os
from pathlib import Path
from datetime import date, timedelta
from collections import defaultdict, deque
import pandas as pd
from typing import Any
from typing import TYPE_CHECKING
if TYPE_CHECKING:
    import duckdb  # for type hints only
# Satisfy static analyzers; real import happens inside simulate_days()
try:
    import duckdb as _duckdb_for_lint  # type: ignore  # noqa: F401
except Exception:
    _duckdb_for_lint = None  # type: ignore

# Default output path for MVP0 alerts (NDJSON)
DEFAULT_OUT_DIR = "out/mvp0/alerts_ndjson"

# --- PR2 helpers: wire to daily_user.parquet ---
def _read_release_tag() -> str:
	"""
	Read the active release tag from release.txt.
	Falls back to 'r5.1' if missing so local tests don't implode.
	"""
	try:
		return Path("release.txt").read_text().strip()
	except Exception:
		return "r5.1"

def _daily_user_parquet(rel: str) -> Path:
    p2 = Path("out") / rel / "features_v2" / "daily_user" / "daily_user.parquet"
    p1 = Path("out") / rel / "features_v1" / "daily_user" / "daily_user.parquet"
    return p2 if p2.exists() else p1

def _load_day_rows(con: Any, p: Path, d_iso: str) -> list[dict]:
	"""
	Return list of dict rows for a single day from daily_user.parquet.
	Uses Arrow to avoid pulling in pandas as a hard dependency.
	"""
	try:
		q = (
			"SELECT * FROM _daily "
			f"WHERE day >= TIMESTAMP '{d_iso} 00:00:00' "
			f"AND   day <  TIMESTAMP '{d_iso} 00:00:00' + INTERVAL 1 DAY"
		)
		tbl = con.execute(q).fetch_arrow_table()
	except Exception:
		# If schema surprises us (e.g., no `day`), just return empty and keep the loop alive.
		return []
	return tbl.to_pylist()  # list[dict]

#
# CLI contract:
#   --start / --end  : inclusive date range for the simulation
#   --out-dir        : where alerts.ndjson is written
#   --dry-run        : exercise control flow without writing outputs
# -----------------------------
# CLI
# -----------------------------
def _parse_args() -> argparse.Namespace:
	p = argparse.ArgumentParser(description="Day-by-day simulation loop with 14-day windows (MVP0).")
	p.add_argument("--start", required=True, help="Start date YYYY-MM-DD")
	p.add_argument("--end", required=True, help="End date YYYY-MM-DD (inclusive)")
	p.add_argument("--out-dir", default=DEFAULT_OUT_DIR, help=f"Output directory (default: {DEFAULT_OUT_DIR})")
	p.add_argument("--dry-run", action="store_true", help="Run without writing alerts")
	return p.parse_args()

#
# Yield each calendar day in [d0, d1]. The real loop will use these to query
# per‑day users (from daily_user.parquet) once PR #2 lands.
#
def _daterange(d0: date, d1: date):
	cur = d0
	one = timedelta(days=1)
	while cur <= d1:
		yield cur
		cur += one

def _load_last_seen_map(rel: str) -> dict[str, date]:
    """
    Load per-user *true* last_seen from ldap_v3_lean and return as {user_key: date}.

    Only users whose last_seen is earlier than the global max last_seen
    are treated as terminated. Users whose last_seen equals the global max
    are assumed to still be employed when the dataset ends.
    """
    ldap_path = Path("out") / rel / "ldap_v3_lean" / "ldap_asof_by_month.parquet"
    if not ldap_path.exists():
        return {}

    import duckdb
    con = duckdb.connect(database=":memory:")
    con.execute(f"""
        CREATE OR REPLACE VIEW ldap_lean AS
        SELECT
            lower(user_key) AS user_key,
            CAST(last_seen AS DATE) AS last_seen
        FROM read_parquet('{ldap_path.as_posix()}', union_by_name=true)
    """)

    # 1) Global max last_seen across all users (dataset boundary)
    max_row = con.execute("SELECT max(last_seen) FROM ldap_lean").fetchone()
    if not max_row or max_row[0] is None:
        con.close()
        return {}

    global_max = max_row[0]

    # 2) Per-user last_seen
    rows = con.execute("""
        SELECT user_key, max(last_seen) AS last_seen
        FROM ldap_lean
        GROUP BY user_key
    """).fetchall()
    con.close()

    # 3) Only keep users whose last_seen is strictly earlier than the dataset boundary
    result: dict[str, date] = {}
    for user_key, ls in rows:
        if not user_key or ls is None:
            continue
        if ls < global_max:
            result[user_key] = ls

    print(f"[run_loop] termination map: {len(result)} users with last_seen < {global_max}")
    return result

from src.detector import rules
from src.detector import anomaly as anomaly
from src.detector import ml
from src.detector import forecast_s1

# Core simulation flow:
#   1) Establish an output file and a dict of per-user rolling windows (14‑day deque).
#   2) For each simulated day:
#        a) Build the set of "todays_users" (placeholder users here; real users in PR #2).
#        b) Append today into each user's deque (automatically capped at 14).
#        c) Call each detector with {"user_key", "window"} and write any returned alerts.
#        d) Emit one loop "heartbeat" line per day so tests have a stable floor.
#   3) Return number of lines written (useful for sanity checks).
def simulate_days(start: date, end: date, out_dir: str) -> int:
	"""
	Simulate day-by-day, maintain per-user 14-day windows, call stub detectors,
	and write one NDJSON record per alert (plus one heartbeat per day).
	Returns the number of lines written.
	"""
	
	# --- Progress bar and info ---
	total_days = (end - start).days + 1
	print(f"[run_loop] Simulating {total_days} days: {start.isoformat()} -> {end.isoformat()}")
	print(f"[run_loop] Writing alerts to: {Path(out_dir) / 'alerts.ndjson'}")
	# --- Pre-flight: verify anomaly z-score lookup is valid ---
	from src.detector import anomaly as _anom
	try:
		_anom._load_zscores()
	except Exception as e:
		raise SystemExit(
            f"[run_loop] FATAL: anomaly z-score lookup failed to load.\n"
            f"{e}\n\n"
            "Do NOT run full simulation.\n"
            "Rebuild anomaly artifacts:\n"
            "  1) python -m src.anomaly.build_user_roles\n"
            "  2) python -m src.anomaly.build_user_org_structure\n"
            "  3) python -m src.anomaly.train_isolation_forest\n"
            "  4) python -m src.anomaly.build_window_scores\n"
            "  5) python -m src.anomaly.compute_baselines\n"
        )

	if _anom._z_lookup is None or len(_anom._z_lookup) < 1000:
		raise SystemExit(
            f"[run_loop] FATAL: anomaly z-score lookup is empty or too small "
            f"({0 if _anom._z_lookup is None else len(_anom._z_lookup)} rows).\n"
            "Do NOT run full simulation.\n"
        )

	print(f"[run_loop] Verified z-score lookup: {len(_anom._z_lookup)} entries.")
	# Prepare data source and a DuckDB connection
	rel_tag = _read_release_tag()
	daily_path = _daily_user_parquet(rel_tag)
	
	# Load per-user last_seen from LDAP to gate detectors after termination
	last_seen_by_user = _load_last_seen_map(rel_tag)
	print(f"[run_loop] Loaded last_seen for {len(last_seen_by_user)} users.")

	import duckdb
	con = duckdb.connect(database=":memory:")
	# Version-safe threading pragmas
	try:
		con.execute("PRAGMA threads=system_threads()")
	except Exception:
		n = os.cpu_count() or 4
		try:
			con.execute(f"PRAGMA threads={int(n)}")
		except Exception:
			pass
	# Cache pragma (ignore if unsupported)
	try:
		con.execute("PRAGMA enable_object_cache=true")
	except Exception:
		pass

    # Memory cap to avoid tiny default; helps larger scans
	try:
		con.execute("PRAGMA memory_limit='4GB'")
	except Exception:
		pass

    # Preload a skinny projected view once, then slice per day.
    # This avoids re-reading the Parquet on every iteration.
	start_iso = f"{start.isoformat()} 00:00:00"
	end_iso   = f"{end.isoformat()} 00:00:00"
	proj_cols = ",".join([
        "lower(user_key) AS user_key",
        "day",
        # v1 after-hours + counts
        "logon_after_hours_rate","device_after_hours_rate","file_after_hours_rate",
        "http_after_hours_rate","email_after_hours_rate",
        "logon_n_logon","device_n_device_events","file_n_file_events","http_n_http","email_n_email_sent",
        "email_n_after_hours","device_n_usb_connects","http_n_wikileaks",
        # v2 baseline + novelty fields used by rules.py
        "ah_rate_1d","ah_rate_trend","ah_rate_baseline",
        "usb_count_1d","usb_count_trend","usb_count_baseline",
        "ah_novel","usb_novel",
        # post-departure event features
        "logon_n_events_post_departure",
        "device_n_events_post_departure",
        "file_n_events_post_departure",
        "http_n_events_post_departure",
        "email_n_events_post_departure",
    ])
	con.execute(f"""
		CREATE OR REPLACE VIEW _daily AS
		SELECT {proj_cols}
		FROM read_parquet('{daily_path.as_posix()}')
		WHERE day >= TIMESTAMP '{start_iso}'
		  AND day <  TIMESTAMP '{end_iso}' + INTERVAL 13 DAY
	""")

	outp = Path(out_dir)
	outp.mkdir(parents=True, exist_ok=True)
	out_path = outp / "alerts.ndjson"

	windows: dict[str, deque] = defaultdict(lambda: deque(maxlen=14))
	lines = 0

	with out_path.open("w") as f:
		for i, day in enumerate(_daterange(start, end), start=1):
			# PR2 feed:
			#   Scan daily_user.parquet for exactly this day. If missing, keep going gracefully.
			features_by_user: dict[str, dict] = {}
			todays_users: list[str] = []
			if daily_path.exists():
				rows = _load_day_rows(con, daily_path, day.isoformat())
				for r in rows:
					u = str(r.get("user_key") or "").lower()
					if not u:
						continue
					todays_users.append(u)
					features_by_user[u] = r
			else:
				todays_users = []

			# Append today's marker into the per‑user deque.
			# deque(maxlen=14) automatically discards the oldest entry on overflow.
			day_str = day.isoformat()
			# Progress bar on a single line (approx. once per day)
			progress = i / total_days
			bar_width = 40
			filled = int(bar_width * progress)
			bar = "#" * filled + "-" * (bar_width - filled)
			print(f"\r[run_loop] {day_str}  {i}/{total_days} [{bar}] {progress*100:5.1f}%", end="", flush=True)
			for u in todays_users:
				windows[u].append({"day": day_str, "features": features_by_user.get(u, {})})

			# Call detectors per user with current window
			for u in todays_users:
				user_window = list(windows[u])  # materialize

				# Build a 14‑day DataFrame for RULES from the stored daily features
				# Build a 14-day DataFrame for RULES from the stored daily features
				try:
					frame_rows = []
					for e in user_window:
						if isinstance(e, dict):
							row = dict(e.get("features", {}))
							row["day"] = e.get("day")
							frame_rows.append(row)
					window_df = pd.DataFrame(frame_rows)
				except Exception:
					window_df = pd.DataFrame()

				# LDAP cutoff: past last_seen => post-departure
				ls = last_seen_by_user.get(u)
				is_post_departure_window = False
				if ls is not None and day > ls:
					is_post_departure_window = True

                # Also treat any row with post-departure event counts as out-of-scope for anomaly/forecast
				post_departure_cols = [
					"logon_n_events_post_departure",
					"device_n_events_post_departure",
					"file_n_events_post_departure",
					"http_n_events_post_departure",
					"email_n_events_post_departure",
				]
				feat = features_by_user.get(u, {}) or {}
				for col in post_departure_cols:
					val = feat.get(col)
                    # Coerce None to 0 and treat >0 as a signal this is a post-departure row
					if (val or 0) > 0:
						is_post_departure_window = True
						break

				ctx = {
					"user_key": u,
					"window": user_window,
					"features": feat,
					"is_post_departure_window": is_post_departure_window,
				}

				prong_scores = {
					"rules_score": 0.0,
					"anomaly_score": 0.0,
					"ml_score": 0.0,
					"forecast_score": 0.0,
				}

				# RULES: real detector
				for alert in (rules.check(window_df, day=day_str, user_key=u) or []):
					sc = float(alert.get("score", 0.0))
					prong_scores["rules_score"] = max(prong_scores["rules_score"], sc)
					record = {
						"day": day_str,
						"user_key": u,
						"detector": "rules",
						"reason": alert.get("reason", "rules"),
						"score": sc,
						"evidence": alert.get("evidence", {}),
						"human_summary": alert.get("human_summary"),
					}
					f.write(json.dumps(record) + "\n")
					lines += 1

				# Anomaly detector (full pipeline exported from anomaly_base.py)
				# Note: anomaly detector runs for all users; it can use is_post_departure_window from ctx if needed
				for alert in (anomaly.check(ctx) or []):
					sc = float(alert.get("score", 0.0))
					prong_scores["anomaly_score"] = max(prong_scores["anomaly_score"], sc)
					record = {
						"day": day_str,
						"user_key": u,
						"detector": "anomaly",
						"reason": alert.get("reason", "anomaly"),
						"score": sc,
						"evidence": alert.get("evidence", {}),
					}
					f.write(json.dumps(record) + "\n")
					lines += 1
			
				# Forecast detector (Scenario 1)
				# Note: forecast detector runs for all users; it can use is_post_departure_window from ctx if needed
				for alert in (forecast_s1.check(ctx) or []):
					sc = float(alert.get("score", 0.0))
					# track max forecast score for this user/day
					prong_scores["forecast_score"] = max(prong_scores["forecast_score"], sc)
					record = {
						"day": day_str,
						"user_key": u,
						"detector": "forecast",
						"reason": alert.get("reason", "forecast"),
						"score": sc,
						"evidence": alert.get("evidence", {}),
					}
					f.write(json.dumps(record) + "\n")
					lines += 1
				
				ml_ctx = {**ctx, **prong_scores}
				for alert in (ml.check(ml_ctx) or []):
					sc = float(alert.get("score", 0.0))
					# track max ML score for this user/day
					prong_scores["ml_score"] = max(prong_scores["ml_score"], sc)
					record = {
						"day": day_str,
						"user_key": u,
						"detector": "ml",
						"reason": alert.get("reason", "ml"),
						"score": sc,
						"rules_score": prong_scores["rules_score"],
						"anomaly_score": prong_scores["anomaly_score"],
						"ml_score": prong_scores["ml_score"],
						"forecast_score": prong_scores["forecast_score"],
					}
					f.write(json.dumps(record) + "\n")
					lines += 1

			# Heartbeat:
			#   Even if detectors produce nothing, we write one line per day.
			#   This guarantees progress and makes quick `wc -l` checks meaningful.
			hb_user = todays_users[0] if todays_users else f"user{i%3:03d}"
			hb = {"day": day_str, "user_key": hb_user, "detector": "loop", "reason": "heartbeat"}
			f.write(json.dumps(hb) + "\n")
			lines += 1

	print()  # finish progress bar line
	print(f"Wrote per-day NDJSON alerts to {out_path}")
	return lines

# Main entrypoint: parse args, guard dates, optionally dry‑run, then simulate.
def main():
	args = _parse_args()
	start = date.fromisoformat(args.start)
	end = date.fromisoformat(args.end)
	if end < start:
		raise SystemExit("end date must be >= start date")

	if args.dry_run:
		print(f"OK: looped {(end - start).days + 1} days (dry run)")
		return

	simulate_days(start, end, args.out_dir)

if __name__ == "__main__":
	main()
