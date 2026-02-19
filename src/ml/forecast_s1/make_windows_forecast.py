"""
Build 14-day forecast windows for Scenario 1.

This script consumes the daily forecast labels produced by
`build_daily_labels_forecast.py` and creates per-split window tables
under:

    out/<REL>/windows_forecast/windows_{train,val,test}.parquet

Each row represents a 14-day window ending on `end_day` for a given
`user_key`, with:

    - label: copied from `label_forecast_day` on that end_day

We only create windows where:

    - `label_forecast_day` is NOT NULL (i.e., not during exfil), and
    - the user has at least `min_history_days` days of history
      (default 14) up to and including `end_day`.

User splits are taken from:

    out/<REL>/ml_splits/daily_labels_splits.parquet

This script does NOT touch the existing detection windows.
"""

import argparse
from pathlib import Path

import duckdb


def read_release_arg(cli_release: str | None) -> str:
    """Return the release string, preferring CLI arg over release.txt."""
    if cli_release is not None:
        return cli_release
    release_file = Path("release.txt")
    if not release_file.exists():
        raise FileNotFoundError("release.txt not found and --release not provided")
    text = release_file.read_text().strip()
    if not text:
        raise ValueError("release.txt is empty")
    return text


def make_windows_forecast(release: str, min_history_days: int) -> None:
    root = Path("out") / release

    daily_forecast_path = root / "labels_forecast" / "daily_labels_forecast.parquet"
    splits_path = root / "ml_splits" / "daily_labels_splits.parquet"
    out_dir = root / "windows_forecast"
    out_dir.mkdir(parents=True, exist_ok=True)

    if not daily_forecast_path.exists():
        raise FileNotFoundError(
            f"Missing daily forecast labels at {daily_forecast_path}. "
            "Run build_daily_labels_forecast.py first."
        )

    if not splits_path.exists():
        raise FileNotFoundError(f"Missing user splits at {splits_path}")

    con = duckdb.connect()

    daily_str = daily_forecast_path.as_posix()
    splits_str = splits_path.as_posix()

    # Build a unified temp table with user_key, end_day, label, split and
    # a row-number per (user_key) ordered by day so we can enforce the
    # minimum history requirement.
    base_sql = f"""
    WITH daily AS (
        SELECT
            user_key,
            day,
            label_forecast_day AS label
        FROM read_parquet('{daily_str}')
        WHERE label_forecast_day IS NOT NULL
    ),
    splits AS (
        SELECT DISTINCT user_key, split
        FROM read_parquet('{splits_str}')
    ),
    joined AS (
        SELECT
            d.user_key,
            d.day,
            d.label,
            s.split,
            ROW_NUMBER() OVER (
                PARTITION BY d.user_key
                ORDER BY d.day
            ) AS rn
        FROM daily d
        JOIN splits s USING (user_key)
    ),
    filtered AS (
        SELECT
            user_key,
            day AS end_day,
            label,
            split
        FROM joined
        WHERE rn >= {min_history_days}
    )
    SELECT * FROM filtered
    """

    con.execute(f"CREATE OR REPLACE TEMP VIEW forecast_windows AS {base_sql}")

    summaries = {}

    for split in ("train", "val", "test"):
        out_path = out_dir / f"windows_{split}.parquet"
        # baseline detection windows for this split (contains window_json)
        baseline_path = (root / "windows" / f"windows_{split}.parquet").as_posix()

        query = f"""
            SELECT
                f.user_key,
                f.end_day,
                f.label,
                b.window_json
            FROM forecast_windows f
            JOIN read_parquet('{baseline_path}') b
              ON f.user_key = b.user_key
             AND f.end_day = b.end_day
            WHERE f.split = '{split}'
        """

        con.execute(
            f"COPY ({query}) TO '{out_path.as_posix()}' (FORMAT 'parquet');"
        )

        summary = con.execute(
            f"""
            SELECT
                COUNT(*) AS n_windows,
                SUM(label) AS n_pos,
                AVG(CAST(label AS DOUBLE)) AS pos_rate
            FROM read_parquet('{out_path.as_posix()}')
            """
        ).fetchone()

        summaries[split] = {
            "out_path": str(out_path),
            "n_windows": int(summary[0]),
            "n_pos": int(summary[1] or 0),
            "pos_rate": float(summary[2] or 0.0),
        }

    print(
        {
            "release": release,
            "min_history_days": min_history_days,
            "splits": summaries,
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build 14-day forecast windows for Scenario 1."
    )
    parser.add_argument(
        "--release",
        type=str,
        default=None,
        help="CERT release (default: read from release.txt)",
    )
    parser.add_argument(
        "--min-history-days",
        type=int,
        default=14,
        help="Minimum number of days of history required for a window (default: 14)",
    )

    args = parser.parse_args()
    release = read_release_arg(args.release)

    print({"release": release, "min_history_days": args.min_history_days})
    make_windows_forecast(release, args.min_history_days)


if __name__ == "__main__":
    main()
