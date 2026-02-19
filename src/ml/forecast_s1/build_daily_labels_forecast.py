"""
Build daily forecast labels for Scenario 1.

For each (user_key, day) in features_v2/daily_user, we compute:

- is_exfil_day: whether this day falls inside any Scenario-1 exfil range
- days_until_exfil: days until the next Scenario-1 exfil_start for that user
- label_forecast_day:

      * NULL        if is_exfil_day = TRUE          (during exfil; not a forecast target)
      * 1           if 1 <= days_until_exfil <= H   (inside forecast horizon)
      * 0           otherwise                       (no exfil within horizon)

H (forecast horizon, in days) is passed via --horizon (default 7).

Outputs:
    out/<REL>/labels_forecast/daily_labels_forecast.parquet

This does NOT touch the existing detection labels.
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


def build_daily_labels_forecast(release: str, horizon: int) -> None:
    root = Path("out") / release

    daily_user_path = root / "features_v2" / "daily_user" / "daily_user.parquet"
    exfil_ranges_path = root / "labels" / "exfil_ranges.parquet"
    out_dir = root / "labels_forecast"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "daily_labels_forecast.parquet"

    if not daily_user_path.exists():
        raise FileNotFoundError(f"Missing daily_user features at {daily_user_path}")

    if not exfil_ranges_path.exists():
        raise FileNotFoundError(f"Missing exfil_ranges at {exfil_ranges_path}")

    con = duckdb.connect()

    daily_user_str = daily_user_path.as_posix()
    exfil_ranges_str = exfil_ranges_path.as_posix()
    out_path_str = out_path.as_posix()

    sql = f"""
    WITH daily AS (
        SELECT *
        FROM read_parquet('{daily_user_str}')
    ),
    ranges AS (
        SELECT
            user_key,
            exfil_start,
            exfil_end
        FROM read_parquet('{exfil_ranges_str}')
        WHERE scenario_id = 'SCENARIO_1'
    ),
    -- For each (user, day), compute days until next Scenario-1 exfil_start
    next_exfil AS (
        SELECT
            d.user_key,
            d.day,
            MIN(DATEDIFF('day', d.day, r.exfil_start)) AS days_until_exfil
        FROM (
            SELECT DISTINCT user_key, day FROM daily
        ) d
        LEFT JOIN ranges r
          ON r.user_key = d.user_key
         AND r.exfil_start > d.day
        GROUP BY d.user_key, d.day
    ),
    flags AS (
        SELECT
            d.user_key,
            d.day,
            CASE
                WHEN EXISTS (
                    SELECT 1
                    FROM ranges r
                    WHERE r.user_key = d.user_key
                      AND d.day BETWEEN r.exfil_start AND r.exfil_end
                ) THEN TRUE
                ELSE FALSE
            END AS is_exfil_day,
            ne.days_until_exfil
        FROM (
            SELECT DISTINCT user_key, day FROM daily
        ) d
        LEFT JOIN next_exfil ne
          ON ne.user_key = d.user_key
         AND ne.day      = d.day
    ),
    final AS (
        SELECT
            d.*,
            f.is_exfil_day,
            f.days_until_exfil,
            {horizon}::INTEGER AS forecast_horizon_days,
            CASE
                WHEN f.is_exfil_day THEN NULL                -- during exfil, not a forecast target
                WHEN f.days_until_exfil IS NULL THEN 0       -- no future exfil for this user
                WHEN f.days_until_exfil BETWEEN 1 AND {horizon} THEN 1
                ELSE 0
            END AS label_forecast_day
        FROM daily d
        LEFT JOIN flags f
          USING (user_key, day)
    )
    SELECT * FROM final
    """

    con.execute(
        f"COPY ({sql}) TO '{out_path_str}' (FORMAT 'parquet');"
    )

    # Quick summary for sanity
    summary = con.execute(
        f"""
        SELECT
            COUNT(*) AS rows,
            SUM(CASE WHEN label_forecast_day = 1 THEN 1 ELSE 0 END) AS pos_rows,
            SUM(CASE WHEN label_forecast_day = 0 THEN 1 ELSE 0 END) AS neg_rows,
            SUM(CASE WHEN label_forecast_day IS NULL THEN 1 ELSE 0 END) AS null_rows
        FROM read_parquet('{out_path_str}')
        """
    ).fetchone()

    print(
        {
            "release": release,
            "horizon_days": horizon,
            "out_path": str(out_path),
            "rows": summary[0],
            "pos_rows": summary[1],
            "neg_rows": summary[2],
            "null_rows": summary[3],
        }
    )


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Build daily forecast labels for Scenario 1."
    )
    parser.add_argument(
        "--release",
        type=str,
        default=None,
        help="CERT release (default: read from release.txt)",
    )
    parser.add_argument(
        "--horizon",
        type=int,
        default=7,
        help="Forecast horizon in days (default: 7)",
    )

    args = parser.parse_args()
    release = read_release_arg(args.release)

    print({"release": release, "horizon_days": args.horizon})
    build_daily_labels_forecast(release, args.horizon)


if __name__ == "__main__":
    main()
