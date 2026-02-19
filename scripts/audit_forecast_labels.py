#!/usr/bin/env python3
"""
Audit Forecast Labels for Scenario-1

Verifies that benign users (like JDJ1949) are NOT in the positive forecast training set,
and that known insiders (like KEW0198, DAS1320) have correct positive labels.

Usage:
    python scripts/audit_forecast_labels.py
"""

import duckdb
from pathlib import Path


def audit_forecast_labels():
    """Audit forecast labels for probe users and summary statistics."""
    rel = "r5.2"
    labels_path = Path(f"out/{rel}/labels_forecast/daily_labels_forecast.parquet")
    
    if not labels_path.exists():
        print(f"ERROR: Forecast labels not found at {labels_path}")
        print("Run: python -m src.ml.forecast_s1.build_daily_labels_forecast")
        return
    
    con = duckdb.connect()
    
    print("=" * 80)
    print("FORECAST LABEL AUDIT")
    print("=" * 80)
    
    # Global summary
    summary = con.execute(f"""
        SELECT
            COUNT(*) AS total_rows,
            SUM(CASE WHEN label_forecast_day = 1 THEN 1 ELSE 0 END) AS positive_labels,
            SUM(CASE WHEN label_forecast_day = 0 THEN 1 ELSE 0 END) AS negative_labels,
            SUM(CASE WHEN label_forecast_day IS NULL THEN 1 ELSE 0 END) AS null_labels,
            COUNT(DISTINCT user_key) AS total_users,
            COUNT(DISTINCT CASE WHEN label_forecast_day = 1 THEN user_key END) AS users_with_positives
        FROM read_parquet('{labels_path.as_posix()}')
    """).fetchone()
    
    print(f"\nGlobal Summary:")
    print(f"  Total rows: {summary[0]:,}")
    print(f"  Positive labels: {summary[1]:,}")
    print(f"  Negative labels: {summary[2]:,}")
    print(f"  NULL labels (during exfil): {summary[3]:,}")
    print(f"  Total users: {summary[4]:,}")
    print(f"  Users with positive labels: {summary[5]:,}")
    
    print("\n" + "=" * 80)
    print("PROBE USER AUDIT")
    print("=" * 80)
    
    # Check JDJ1949 (benign user - should have ZERO positive labels)
    print("\n[1] JDJ1949 (Benign User - Expected: 0 positive labels)")
    print("-" * 80)
    
    result = con.execute(f"""
        SELECT user_key, day, label_forecast_day, is_exfil_day, days_until_exfil
        FROM read_parquet('{labels_path.as_posix()}')
        WHERE user_key = 'jdj1949'
          AND label_forecast_day = 1
    """).fetchall()
    
    if result:
        print(f"❌ ERROR: JDJ1949 has {len(result)} positive forecast labels (should be 0)")
        print("\nPositive labels found:")
        for row in result[:10]:  # show first 10
            print(f"  {row[1]}: label={row[2]}, is_exfil={row[3]}, days_until={row[4]}")
        if len(result) > 10:
            print(f"  ... and {len(result) - 10} more")
    else:
        print("✅ PASS: JDJ1949 has no positive forecast labels (correct)")
    
    # Check KEW0198 (known insider - should have positive labels)
    print("\n[2] KEW0198 (Known Insider - Expected: positive labels before exfil)")
    print("-" * 80)
    
    result = con.execute(f"""
        SELECT day, label_forecast_day, is_exfil_day, days_until_exfil
        FROM read_parquet('{labels_path.as_posix()}')
        WHERE user_key = 'kew0198'
          AND label_forecast_day = 1
        ORDER BY day
    """).fetchall()
    
    if result:
        print(f"✅ FOUND: {len(result)} positive forecast labels for KEW0198")
        print("\nFirst 5 positive labels:")
        for row in result[:5]:
            print(f"  {row[0]}: label={row[1]}, is_exfil={row[2]}, days_until={row[3]}")
    else:
        print("❌ WARNING: KEW0198 has no positive forecast labels (unexpected)")
    
    # Check DAS1320 (known insider - should have positive labels)
    print("\n[3] DAS1320 (Known Insider - Expected: positive labels before exfil)")
    print("-" * 80)
    
    result = con.execute(f"""
        SELECT day, label_forecast_day, is_exfil_day, days_until_exfil
        FROM read_parquet('{labels_path.as_posix()}')
        WHERE user_key = 'das1320'
          AND label_forecast_day = 1
        ORDER BY day
    """).fetchall()
    
    if result:
        print(f"✅ FOUND: {len(result)} positive forecast labels for DAS1320")
        print("\nFirst 5 positive labels:")
        for row in result[:5]:
            print(f"  {row[0]}: label={row[1]}, is_exfil={row[2]}, days_until={row[3]}")
    else:
        print("❌ WARNING: DAS1320 has no positive forecast labels (unexpected)")
    
    print("\n" + "=" * 80)
    print("EXFIL RANGE CROSS-CHECK")
    print("=" * 80)
    
    # Check if probe users have exfil ranges defined
    exfil_ranges_path = Path(f"out/{rel}/labels/exfil_ranges.parquet")
    
    if exfil_ranges_path.exists():
        ranges = con.execute(f"""
            SELECT user_key, exfil_start, exfil_end, scenario_id
            FROM read_parquet('{exfil_ranges_path.as_posix()}')
            WHERE user_key IN ('jdj1949', 'kew0198', 'das1320')
              AND scenario_id = 'SCENARIO_1'
            ORDER BY user_key
        """).fetchall()
        
        if ranges:
            print("\nExfil ranges for probe users:")
            for row in ranges:
                print(f"  {row[0]}: {row[1]} to {row[2]} (scenario: {row[3]})")
        else:
            print("\n⚠️  No Scenario-1 exfil ranges found for probe users")
    else:
        print(f"\n⚠️  Exfil ranges file not found at {exfil_ranges_path}")
    
    print("\n" + "=" * 80)
    print("AUDIT COMPLETE")
    print("=" * 80)
    
    con.close()


if __name__ == "__main__":
    audit_forecast_labels()
