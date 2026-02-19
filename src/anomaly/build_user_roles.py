"""
Step 0: Build User -> Job Role Lookup

Purpose:
  Extract user_key -> job_role mapping from LDAP data.
  This is used later for peer group baselines (comparing users with similar roles).

Output:
  out/r5.2/anomaly/user_roles.parquet
  
Columns:
  - user_key: lowercase user ID
  - role_id: job role from LDAP

Usage:
  python -m src.anomaly.build_user_roles
"""

from pathlib import Path
import duckdb

# Read release tag
REL = Path("release.txt").read_text().strip()

# Input: LDAP snapshots
LDAP = Path(f"out/{REL}/ldap_v3_full/ldap_snapshots.parquet")

# Output: User roles lookup
OUT = Path(f"out/{REL}/anomaly/user_roles.parquet")
OUT.parent.mkdir(parents=True, exist_ok=True)


def main():
    """Extract user -> role mapping from LDAP data."""
    
    if not LDAP.exists():
        raise FileNotFoundError(
            f"LDAP data not found at {LDAP}. "
            f"Make sure you've run the ETL pipeline first."
        )
    
    print(f"Reading LDAP data from: {LDAP}")
    
    # Query: Get distinct user_key -> role mappings
    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT
                lower(user_key) AS user_key,
                role AS role_id
            FROM read_parquet('{LDAP}')
            WHERE role IS NOT NULL
        )
        TO '{OUT}' (FORMAT PARQUET);
    """)
    
    print(f"Wrote user roles to: {OUT}")
    
    # Show summary
    df = duckdb.sql(f"SELECT * FROM read_parquet('{OUT}')").df()
    print(f"\nTotal users: {len(df)}")
    print(f"Unique roles: {df['role_id'].nunique()}")
    print("\nRole distribution:")
    print(df['role_id'].value_counts().head(10))


if __name__ == "__main__":
    main()
