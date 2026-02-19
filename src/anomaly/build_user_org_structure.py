"""
Step 0b: Build User -> Organizational Structure Lookup

Purpose:
  Extract complete organizational hierarchy from LDAP data.
  Includes role, business unit, functional unit, department, and team.
  This enables more granular peer group baselines.

Output:
  out/r5.2/anomaly/user_org_structure.parquet
  
Columns:
  - user_key: lowercase user ID
  - role_id: job role
  - business_unit: business unit
  - functional_unit: functional unit
  - department: department
  - team_id: team

Usage:
  python -m src.anomaly.build_user_org_structure
"""

from pathlib import Path
import duckdb

# Read release tag
REL = Path("release.txt").read_text().strip()

# Input: LDAP snapshots (full version with all org fields)
LDAP = Path(f"out/{REL}/ldap_v3_full/ldap_snapshots.parquet")

# Output: User organizational structure
OUT = Path(f"out/{REL}/anomaly/user_org_structure.parquet")
OUT.parent.mkdir(parents=True, exist_ok=True)


def main():
    """Extract user -> organizational structure from LDAP data."""
    
    if not LDAP.exists():
        raise FileNotFoundError(
            f"LDAP data not found at {LDAP}. "
            f"Make sure you've run the ETL pipeline first."
        )
    
    print(f"Reading LDAP data from: {LDAP}")
    
    # Query: Get distinct user_key -> org structure mappings
    duckdb.sql(f"""
        COPY (
            SELECT DISTINCT
                lower(user_key) AS user_key,
                role AS role_id,
                business_unit,
                functional_unit,
                department,
                team AS team_id
            FROM read_parquet('{LDAP}')
            WHERE role IS NOT NULL
        )
        TO '{OUT}' (FORMAT PARQUET);
    """)
    
    print(f"Wrote organizational structure to: {OUT}")
    
    # Show summary
    df = duckdb.sql(f"SELECT * FROM read_parquet('{OUT}')").df()
    print(f"\nTotal users: {len(df)}")
    print(f"Unique roles: {df['role_id'].nunique()}")
    print(f"Unique business units: {df['business_unit'].nunique()}")
    print(f"Unique functional units: {df['functional_unit'].nunique()}")
    print(f"Unique departments: {df['department'].nunique()}")
    print(f"Unique teams: {df['team_id'].nunique()}")
    
    print("\nRole distribution:")
    print(df['role_id'].value_counts().head(10))
    
    print("\nBusiness unit distribution:")
    print(df['business_unit'].value_counts().head(10))


if __name__ == "__main__":
    main()
