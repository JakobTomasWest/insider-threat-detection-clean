#!/usr/bin/env python3
"""
Compute rolling z-scores using expanding window baselines.

For each window, compares the current anomaly score against historical baselines
at multiple levels: personal (user), role, business unit, department, and team.

Uses vectorized pandas operations for efficient computation across 680k+ windows.

Output: window_zscores.parquet with z-scores at all baseline levels.
"""
import duckdb
import pandas as pd
import numpy as np
from pathlib import Path


def compute_personal_baselines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-user baselines using expanding window.
    
    For each window, calculates mean/std from ALL prior windows for that user.
    """
    print("\n[1/7] Computing personal baselines...")
    
    # MUST be sorted by user, then date
    df = df.sort_values(['user_key', 'end_day']).reset_index(drop=True)
    
    # Expanding statistics (includes current window)
    df['_personal_mean_raw'] = df.groupby('user_key')['base_score'].expanding().mean().reset_index(level=0, drop=True)
    df['_personal_std_raw'] = df.groupby('user_key')['base_score'].expanding().std().reset_index(level=0, drop=True)
    df['prior_window_count'] = df.groupby('user_key').cumcount()
    
    # Shift to use ONLY prior windows (exclude current)
    df['personal_mean'] = df.groupby('user_key')['_personal_mean_raw'].shift(1)
    df['personal_std'] = df.groupby('user_key')['_personal_std_raw'].shift(1)
    
    # Compute z-score
    df['z_personal'] = (df['base_score'] - df['personal_mean']) / df['personal_std']
    
    # Mark insufficient history as NaN (need at least 14 prior windows)
    df.loc[df['prior_window_count'] < 14, ['z_personal', 'personal_mean', 'personal_std']] = np.nan
    
    # Drop temporary columns
    df = df.drop(columns=['_personal_mean_raw', '_personal_std_raw'])
    
    print(f"   ✓ Personal baselines computed for {df['user_key'].nunique()} users")
    print(f"   ✓ {df['z_personal'].notna().sum():,} windows have valid z_personal scores")
    
    return df


def compute_role_baselines(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-role baselines using strict temporal windowing.
    
    For each window, calculates mean/std from ALL prior windows across all users
    in the same role from PRIOR DATES ONLY (not including current date).
    """
    print("\n[2/7] Computing role baselines...")
    
    # Create a unique key for merging back
    df['_merge_key'] = df['user_key'] + '_' + df['end_day'].astype(str)
    
    # For each (role, date), compute stats from all prior dates
    role_baselines = []
    
    for role in df['role_id'].unique():
        role_df = df[df['role_id'] == role].sort_values('end_day')
        unique_dates = sorted(role_df['end_day'].unique())
        
        for current_date in unique_dates:
            # Get all windows from THIS role on PRIOR dates
            prior_windows = role_df[role_df['end_day'] < current_date]
            
            if len(prior_windows) >= 50:
                role_mean = prior_windows['base_score'].mean()
                role_std = prior_windows['base_score'].std()
                role_n = len(prior_windows)
            else:
                role_mean = np.nan
                role_std = np.nan
                role_n = len(prior_windows)
            
            # Apply to all windows on this date for this role
            current_windows = role_df[role_df['end_day'] == current_date]
            for _, row in current_windows.iterrows():
                merge_key = row['user_key'] + '_' + str(row['end_day'])
                role_baselines.append({
                    '_merge_key': merge_key,
                    'role_mean': role_mean,
                    'role_std': role_std,
                    'role_n': role_n
                })
    
    # Convert to DataFrame and merge back
    role_df = pd.DataFrame(role_baselines)
    df = df.merge(role_df, on='_merge_key', how='left')
    
    # Compute z-scores
    df['z_role'] = (df['base_score'] - df['role_mean']) / df['role_std']
    
    print(f"   ✓ Role baselines computed for {df['role_id'].nunique()} roles")
    print(f"   ✓ {df['z_role'].notna().sum():,} windows have valid z_role scores")
    
    return df


def compute_group_baselines(df: pd.DataFrame, group_col: str, min_windows: int = 30) -> pd.DataFrame:
    """
    Compute baselines for organizational groups (BU, department, team).
    
    Uses strict temporal windowing - only prior dates, no same-day leakage.
    Handles missing group values by leaving z-scores as NaN.
    """
    print(f"\n[3-6/7] Computing {group_col} baselines...")
    
    # Filter to rows with non-null group
    has_group = df[group_col].notna()
    
    if not has_group.any():
        print(f"   ⚠ No users have {group_col} defined, skipping")
        df[f'{group_col}_mean'] = np.nan
        df[f'{group_col}_std'] = np.nan
        df[f'{group_col}_n'] = 0
        df[f'z_{group_col}'] = np.nan
        return df
    
    # For each (group, date), compute stats from all prior dates
    group_baselines = []
    
    for group_val in df[group_col].dropna().unique():
        group_df = df[df[group_col] == group_val].sort_values('end_day')
        unique_dates = sorted(group_df['end_day'].unique())
        
        for current_date in unique_dates:
            # Get all windows from THIS group on PRIOR dates
            prior_windows = group_df[group_df['end_day'] < current_date]
            
            if len(prior_windows) >= min_windows:
                group_mean = prior_windows['base_score'].mean()
                group_std = prior_windows['base_score'].std()
                group_n = len(prior_windows)
            else:
                group_mean = np.nan
                group_std = np.nan
                group_n = len(prior_windows)
            
            # Apply to all windows on this date for this group
            current_windows = group_df[group_df['end_day'] == current_date]
            for _, row in current_windows.iterrows():
                merge_key = row['user_key'] + '_' + str(row['end_day'])
                group_baselines.append({
                    '_merge_key': merge_key,
                    f'{group_col}_mean': group_mean,
                    f'{group_col}_std': group_std,
                    f'{group_col}_n': group_n
                })
    
    # Convert to DataFrame and merge back
    if len(group_baselines) > 0:
        group_baseline_df = pd.DataFrame(group_baselines)
        df = df.merge(group_baseline_df, on='_merge_key', how='left')
    else:
        # No groups to process
        df[f'{group_col}_mean'] = np.nan
        df[f'{group_col}_std'] = np.nan
        df[f'{group_col}_n'] = 0
    
    # Compute z-scores
    df[f'z_{group_col}'] = (df['base_score'] - df[f'{group_col}_mean']) / df[f'{group_col}_std']
    
    unique_groups = df.loc[has_group, group_col].nunique()
    valid_zscores = df[f'z_{group_col}'].notna().sum()
    print(f"   ✓ {group_col} baselines computed for {unique_groups} groups")
    print(f"   ✓ {valid_zscores:,} windows have valid z_{group_col} scores")
    
    return df


def compute_max_zscore(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute maximum z-score across all baseline types.
    
    This represents the "worst case" deviation - the strongest signal
    that something is anomalous relative to ANY baseline.
    """
    print("\n[7/7] Computing maximum z-scores...")
    
    z_cols = ['z_personal', 'z_role', 'z_business_unit', 'z_functional_unit', 'z_department', 'z_team_id']
    
    # max() ignores NaN values automatically
    df['z_max'] = df[z_cols].max(axis=1)
    
    print(f"   ✓ z_max computed for {df['z_max'].notna().sum():,} windows")
    
    return df


def main():
    print("="*70)
    print("COMPUTE BASELINES - Rolling Z-Score Calculation")
    print("="*70)
    
    # Load window scores with org structure
    print("\nLoading data...")
    con = duckdb.connect()
    
    df = con.execute("""
        SELECT 
            w.user_key,
            w.end_day,
            w.base_score,
            w.role_id,
            o.business_unit,
            o.functional_unit,
            o.department,
            o.team_id
        FROM 'out/r5.2/anomaly/window_scores.parquet' w
        LEFT JOIN 'out/r5.2/anomaly/user_org_structure.parquet' o
            ON w.user_key = o.user_key
        ORDER BY w.user_key, w.end_day
    """).df()
    
    con.close()
    
    print(f"   ✓ Loaded {len(df):,} windows")
    print(f"   ✓ {df['user_key'].nunique()} unique users")
    print(f"   ✓ {df['role_id'].nunique()} unique roles")
    print(f"   ✓ {df['business_unit'].nunique()} unique business units")
    print(f"   ✓ {df['functional_unit'].nunique()} unique functional units")
    print(f"   ✓ {df['department'].nunique()} unique departments")
    print(f"   ✓ {df['team_id'].nunique()} unique teams")
    
    # Check for missing org data
    print(f"\n   Missing org data:")
    print(f"     - business_unit: {df['business_unit'].isna().sum():,} windows ({df['business_unit'].isna().mean()*100:.1f}%)")
    print(f"     - functional_unit: {df['functional_unit'].isna().sum():,} windows ({df['functional_unit'].isna().mean()*100:.1f}%)")
    print(f"     - department: {df['department'].isna().sum():,} windows ({df['department'].isna().mean()*100:.1f}%)")
    print(f"     - team_id: {df['team_id'].isna().sum():,} windows ({df['team_id'].isna().mean()*100:.1f}%)")
    
    # Compute baselines at each level
    # Personal baseline requires user+date sort, so do it first
    df = compute_personal_baselines(df)
    
    # Add index column for restoring order after subsequent sorts
    df['_orig_index'] = range(len(df))
    
    # All other baselines preserve original order internally
    df = compute_role_baselines(df)
    df = compute_group_baselines(df, 'business_unit', min_windows=30)
    df = compute_group_baselines(df, 'functional_unit', min_windows=30)
    df = compute_group_baselines(df, 'department', min_windows=30)
    df = compute_group_baselines(df, 'team_id', min_windows=20)
    df = compute_max_zscore(df)
    
    # Drop index column
    df = df.drop(columns=['_orig_index'])
    
    # Save result
    output_path = Path('out/r5.2/anomaly/window_zscores.parquet')
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    df.to_parquet(output_path, index=False)
    
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    
    print(f"\nOutput saved: {output_path}")
    print(f"File size: {output_path.stat().st_size / 1024 / 1024:.1f} MB")
    print(f"Total windows: {len(df):,}")
    
    print(f"\nZ-score coverage:")
    print(f"  z_personal:        {df['z_personal'].notna().sum():,} ({df['z_personal'].notna().mean()*100:.1f}%)")
    print(f"  z_role:            {df['z_role'].notna().sum():,} ({df['z_role'].notna().mean()*100:.1f}%)")
    print(f"  z_business_unit:   {df['z_business_unit'].notna().sum():,} ({df['z_business_unit'].notna().mean()*100:.1f}%)")
    print(f"  z_functional_unit: {df['z_functional_unit'].notna().sum():,} ({df['z_functional_unit'].notna().mean()*100:.1f}%)")
    print(f"  z_department:      {df['z_department'].notna().sum():,} ({df['z_department'].notna().mean()*100:.1f}%)")
    print(f"  z_team_id:         {df['z_team_id'].notna().sum():,} ({df['z_team_id'].notna().mean()*100:.1f}%)")
    print(f"  z_max:             {df['z_max'].notna().sum():,} ({df['z_max'].notna().mean()*100:.1f}%)")
    
    print(f"\nZ-score distributions (z_max):")
    print(f"  Mean:   {df['z_max'].mean():.2f}")
    print(f"  Median: {df['z_max'].median():.2f}")
    print(f"  Std:    {df['z_max'].std():.2f}")
    print(f"  90th percentile: {df['z_max'].quantile(0.90):.2f}")
    print(f"  95th percentile: {df['z_max'].quantile(0.95):.2f}")
    print(f"  99th percentile: {df['z_max'].quantile(0.99):.2f}")
    print(f"  Max:    {df['z_max'].max():.2f}")
    
    # High z-score counts
    print(f"\nHigh z-score thresholds:")
    for threshold in [2.0, 2.5, 3.0, 3.5, 4.0]:
        count = (df['z_max'] > threshold).sum()
        pct = count / len(df) * 100
        print(f"  z_max > {threshold}: {count:,} windows ({pct:.2f}%)")
    
    print("\n✅ Baseline computation complete!")


if __name__ == '__main__':
    main()
