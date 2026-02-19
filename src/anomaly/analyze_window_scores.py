"""
Analyze window_scores.parquet to understand anomaly score distribution
and identify high-scoring windows with potential exfiltration activity.
"""

import duckdb
import pandas as pd
from pathlib import Path

# Paths
REL = Path("release.txt").read_text().strip()
SCORES = Path(f"out/{REL}/anomaly/window_scores.parquet")
DAILY = Path(f"out/{REL}/features_v2/daily_user/daily_user.parquet")
INSIDERS = Path("answers/insiders.csv")


def main():
    con = duckdb.connect(database=":memory:")
    
    print("="*70)
    print("WINDOW SCORES ANALYSIS")
    print("="*70)
    
    # Basic stats
    result = con.execute(f"""
        SELECT 
            COUNT(*) as total_windows,
            COUNT(DISTINCT user_key) as unique_users,
            MIN(end_day) as first_day,
            MAX(end_day) as last_day,
            MIN(base_score) as min_score,
            MAX(base_score) as max_score,
            AVG(base_score) as mean_score,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY base_score) as median_score,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY base_score) as p25,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY base_score) as p75
        FROM read_parquet('{SCORES}')
    """).df()
    
    print("\n=== Basic Stats ===")
    print(f"Total windows: {result['total_windows'].iloc[0]:,}")
    print(f"Unique users: {result['unique_users'].iloc[0]}")
    print(f"Date range: {result['first_day'].iloc[0]} to {result['last_day'].iloc[0]}")
    
    print("\n=== Score Distribution ===")
    print(f"Min:    {result['min_score'].iloc[0]:.6f}")
    print(f"25%:    {result['p25'].iloc[0]:.6f}")
    print(f"Median: {result['median_score'].iloc[0]:.6f}")
    print(f"Mean:   {result['mean_score'].iloc[0]:.6f}")
    print(f"75%:    {result['p75'].iloc[0]:.6f}")
    print(f"Max:    {result['max_score'].iloc[0]:.6f}")
    
    # Top 20 highest scores
    print("\n=== Highest Anomaly Scores (Top 20) ===")
    top20 = con.execute(f"""
        SELECT user_key, end_day, base_score, role_id
        FROM read_parquet('{SCORES}')
        ORDER BY base_score DESC
        LIMIT 20
    """).df()
    print(top20.to_string(index=False))
    
    # Check insider overlap
    insiders_df = pd.read_csv(INSIDERS)
    r52_insiders = set(insiders_df[insiders_df['dataset'].astype(str) == '5.2']['user'].str.lower())
    
    top_users = con.execute(f"""
        SELECT DISTINCT user_key
        FROM (
            SELECT user_key, base_score
            FROM read_parquet('{SCORES}')
            ORDER BY base_score DESC
            LIMIT 100
        )
    """).df()['user_key'].tolist()
    
    insider_overlap = set(top_users) & r52_insiders
    
    print(f"\n=== High Scores vs Known Insiders ===")
    print(f"Top 100 highest scoring windows come from {len(set(top_users))} unique users")
    print(f"Of these, {len(insider_overlap)} are known r5.2 malicious insiders")
    if insider_overlap:
        print(f"\nMalicious users in top 100:")
        for user in sorted(insider_overlap):
            # Get their scenario
            scenario = insiders_df[insiders_df['user'].str.lower() == user]['scenario'].iloc[0]
            print(f"  - {user} (scenario {scenario})")
    
    # For highest scorer, show what happened in that window
    print("\n" + "="*70)
    print("DETAILED ANALYSIS OF HIGHEST SCORING WINDOW")
    print("="*70)
    
    highest = top20.iloc[0]
    user = highest['user_key']
    end_day = highest['end_day']
    score = highest['base_score']
    
    print(f"\nUser: {user}")
    print(f"Window end date: {end_day}")
    print(f"Anomaly score: {score:.6f}")
    
    # Check if malicious
    is_insider = user in r52_insiders
    print(f"Known malicious: {'YES' if is_insider else 'NO'}")
    if is_insider:
        scenario = insiders_df[insiders_df['user'].str.lower() == user]['scenario'].iloc[0]
        print(f"Scenario: {scenario}")
    
    # Get window details - 14 days ending on end_day
    print(f"\n=== Activity During This Window ===")
    window_data = con.execute(f"""
        SELECT 
            day,
            logon_n_logon,
            device_n_device_events,
            device_n_usb_connects,
            file_n_file_events,
            http_n_http,
            http_n_wikileaks,
            email_n_email_sent,
            logon_after_hours_rate,
            device_after_hours_rate
        FROM read_parquet('{DAILY}')
        WHERE LOWER(user_key) = '{user}'
          AND day <= TIMESTAMP '{end_day}'
          AND day > TIMESTAMP '{end_day}' - INTERVAL 14 DAY
        ORDER BY day
    """).df()
    
    if len(window_data) > 0:
        print(window_data.to_string(index=False))
        
        # Check for exfiltration indicators
        print(f"\n=== Exfiltration Indicators ===")
        total_usb = window_data['device_n_usb_connects'].sum()
        total_wikileaks = window_data['http_n_wikileaks'].sum()
        total_files = window_data['file_n_file_events'].sum()
        total_emails = window_data['email_n_email_sent'].sum()
        avg_after_hours = window_data['logon_after_hours_rate'].mean()
        
        print(f"USB connects: {total_usb}")
        print(f"WikiLeaks visits: {total_wikileaks}")
        print(f"File events: {total_files}")
        print(f"Emails sent: {total_emails}")
        print(f"Avg after-hours rate: {avg_after_hours:.2%}")
        
        if total_wikileaks > 0 or total_usb > 10:
            print(f"\n⚠️  POSSIBLE EXFILTRATION DETECTED!")
            if total_wikileaks > 0:
                print(f"   - WikiLeaks activity detected")
            if total_usb > 10:
                print(f"   - High USB usage detected")
    else:
        print("No daily data found for this window")


def analyze_role_scores(con):
    """Analyze scores by role - which roles are naturally more anomalous."""
    print("\n" + "="*70)
    print("ROLE-BASED ANALYSIS")
    print("="*70)
    
    # Average score by role
    print("\n=== Average Anomaly Score by Role ===")
    role_stats = con.execute(f"""
        SELECT 
            role_id,
            COUNT(*) as num_windows,
            AVG(base_score) as avg_score,
            STDDEV(base_score) as std_score,
            MIN(base_score) as min_score,
            MAX(base_score) as max_score
        FROM read_parquet('{SCORES}')
        WHERE role_id IS NOT NULL
        GROUP BY role_id
        ORDER BY avg_score DESC
        LIMIT 20
    """).df()
    print(role_stats.to_string(index=False))
    
    # Find users who score much higher than their role peers
    print("\n=== Users Scoring Much Higher Than Role Peers ===")
    outliers = con.execute(f"""
        WITH user_scores AS (
            SELECT 
                user_key,
                role_id,
                AVG(base_score) as user_avg_score,
                COUNT(*) as num_windows
            FROM read_parquet('{SCORES}')
            WHERE role_id IS NOT NULL
            GROUP BY user_key, role_id
        ),
        role_stats AS (
            SELECT 
                role_id,
                AVG(base_score) as role_avg_score,
                STDDEV(base_score) as role_std_score
            FROM read_parquet('{SCORES}')
            WHERE role_id IS NOT NULL
            GROUP BY role_id
        )
        SELECT 
            u.user_key,
            u.role_id,
            u.user_avg_score,
            r.role_avg_score,
            (u.user_avg_score - r.role_avg_score) / NULLIF(r.role_std_score, 0) as z_score_vs_role,
            u.num_windows
        FROM user_scores u
        JOIN role_stats r ON u.role_id = r.role_id
        WHERE r.role_std_score > 0
        ORDER BY z_score_vs_role DESC
        LIMIT 15
    """).df()
    print(outliers.to_string(index=False))


def analyze_feature_attribution(con):
    """Identify which features drive high anomaly scores."""
    print("\n" + "="*70)
    print("FEATURE ATTRIBUTION ANALYSIS")
    print("="*70)
    
    print("\n=== Comparing High-Score vs Low-Score Windows ===")
    
    # Get features for high-scoring windows (top 10%)
    high_threshold = con.execute(f"""
        SELECT PERCENTILE_CONT(0.90) WITHIN GROUP (ORDER BY base_score) as p90
        FROM read_parquet('{SCORES}')
    """).fetchone()[0]
    
    low_threshold = con.execute(f"""
        SELECT PERCENTILE_CONT(0.10) WITHIN GROUP (ORDER BY base_score) as p10
        FROM read_parquet('{SCORES}')
    """).fetchone()[0]
    
    print(f"High-score threshold (90th percentile): {high_threshold:.6f}")
    print(f"Low-score threshold (10th percentile): {low_threshold:.6f}")
    
    # Get high and low scoring windows with their daily features
    print("\n=== Average Feature Values: High vs Low Scoring Windows ===")
    
    feature_comparison = con.execute(f"""
        WITH high_scores AS (
            SELECT user_key, end_day
            FROM read_parquet('{SCORES}')
            WHERE base_score >= {high_threshold}
        ),
        low_scores AS (
            SELECT user_key, end_day
            FROM read_parquet('{SCORES}')
            WHERE base_score <= {low_threshold}
        )
        SELECT 
            'HIGH_SCORE' as group_type,
            AVG(d.logon_n_logon) as avg_logons,
            AVG(d.device_n_usb_connects) as avg_usb,
            AVG(d.http_n_wikileaks) as avg_wikileaks,
            AVG(d.file_n_file_events) as avg_files,
            AVG(d.email_n_email_sent) as avg_emails,
            AVG(d.logon_after_hours_rate) as avg_after_hours,
            AVG(d.device_after_hours_rate) as avg_device_after_hours
        FROM read_parquet('{DAILY}') d
        JOIN high_scores h ON LOWER(d.user_key) = h.user_key 
            AND d.day <= CAST(h.end_day AS TIMESTAMP)
            AND d.day > CAST(h.end_day AS TIMESTAMP) - INTERVAL 14 DAY
        
        UNION ALL
        
        SELECT 
            'LOW_SCORE' as group_type,
            AVG(d.logon_n_logon) as avg_logons,
            AVG(d.device_n_usb_connects) as avg_usb,
            AVG(d.http_n_wikileaks) as avg_wikileaks,
            AVG(d.file_n_file_events) as avg_files,
            AVG(d.email_n_email_sent) as avg_emails,
            AVG(d.logon_after_hours_rate) as avg_after_hours,
            AVG(d.device_after_hours_rate) as avg_device_after_hours
        FROM read_parquet('{DAILY}') d
        JOIN low_scores l ON LOWER(d.user_key) = l.user_key 
            AND d.day <= CAST(l.end_day AS TIMESTAMP)
            AND d.day > CAST(l.end_day AS TIMESTAMP) - INTERVAL 14 DAY
    """).df()
    
    print(feature_comparison.to_string(index=False))
    
    # Calculate differences
    if len(feature_comparison) == 2:
        high_row = feature_comparison[feature_comparison['group_type'] == 'HIGH_SCORE'].iloc[0]
        low_row = feature_comparison[feature_comparison['group_type'] == 'LOW_SCORE'].iloc[0]
        
        print("\n=== Feature Differences (High - Low) ===")
        for col in feature_comparison.columns[1:]:
            diff = high_row[col] - low_row[col]
            pct_change = (diff / low_row[col] * 100) if low_row[col] > 0 else 0
            print(f"{col:30s}: {diff:+10.2f} ({pct_change:+.1f}%)")


def analyze_known_insiders(con):
    """Analyze scores for known malicious insiders."""
    print("\n" + "="*70)
    print("KNOWN INSIDER SCORE ANALYSIS")
    print("="*70)
    
    # Load insiders
    insiders_df = pd.read_csv(INSIDERS)
    r52_insiders = insiders_df[insiders_df['dataset'].astype(str) == '5.2'].copy()
    r52_insiders['user_lower'] = r52_insiders['user'].str.lower()
    
    print(f"\n=== Score Statistics for {len(r52_insiders)} Known r5.2 Insiders ===")
    
    # Get all windows for each insider
    for scenario in sorted(r52_insiders['scenario'].unique()):
        scenario_insiders = r52_insiders[r52_insiders['scenario'] == scenario]
        print(f"\n--- Scenario {scenario} ({len(scenario_insiders)} insiders) ---")
        
        for _, insider in scenario_insiders.head(5).iterrows():  # Show first 5 per scenario
            user = insider['user_lower']
            start = insider['start']
            end = insider['end']
            
            # Get scores for this insider
            user_scores = con.execute(f"""
                SELECT 
                    COUNT(*) as num_windows,
                    AVG(base_score) as avg_score,
                    MAX(base_score) as max_score,
                    MIN(base_score) as min_score
                FROM read_parquet('{SCORES}')
                WHERE user_key = '{user}'
            """).fetchone()
            
            if user_scores[0] > 0:
                print(f"  {user:12s}: avg={user_scores[1]:.4f}, max={user_scores[2]:.4f}, windows={user_scores[0]}")
                
                # Get scores during attack period
                attack_scores = con.execute(f"""
                    SELECT 
                        COUNT(*) as attack_windows,
                        AVG(base_score) as attack_avg_score,
                        MAX(base_score) as attack_max_score
                    FROM read_parquet('{SCORES}')
                    WHERE user_key = '{user}'
                      AND end_day >= '{start[:10]}'
                      AND end_day <= '{end[:10]}'
                """).fetchone()
                
                if attack_scores[0] > 0:
                    print(f"               During attack: avg={attack_scores[1]:.4f}, max={attack_scores[2]:.4f}, windows={attack_scores[0]}")


if __name__ == "__main__":
    main()
    
    # Additional analyses
    con = duckdb.connect(database=":memory:")
    
    analyze_role_scores(con)
    analyze_feature_attribution(con)
    analyze_known_insiders(con)
