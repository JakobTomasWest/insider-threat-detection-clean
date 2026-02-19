# src/helpers/domain_flags.py
from __future__ import annotations

import pandas as pd
from src.helpers.time import month_start

def load_ldap_asof(env, ldap_family_for_join: str = "ldap_v3_lean") -> pd.DataFrame:
    """
    Tiny helper to load the LDAP as-of parquet for a given family.

    This keeps the path logic in one place so ETL callers don't have to
    repeat out_path / existence checks.
    """
    from pathlib import Path
    from src.helpers.io import out_path

    asof_path = out_path(env, ldap_family_for_join, "ldap_asof_by_month")
    if not Path(asof_path).exists():
        raise FileNotFoundError(f"Missing LDAP as-of at {asof_path}. Build LDAP first.")
    return pd.read_parquet(asof_path)


def add_after_hours(df: pd.DataFrame, ts_col: str = "timestamp") -> pd.DataFrame:
    """
    Ensure a boolean 'after_hours' column exists on df, using the common rule:
      hour < 07 or hour >= 19.

    Returns the same DataFrame with the column added/overwritten.
    """
    if ts_col not in df.columns:
        # Nothing to do; keep contract simple.
        df["after_hours"] = False
        return df

    ts = pd.to_datetime(df[ts_col], errors="coerce")
    df["after_hours"] = ts.dt.hour.lt(7) | ts.dt.hour.ge(19)
    return df


def add_active_employee_flag(df: pd.DataFrame,
                             month_col: str = "event_month",
                             first_seen_col: str = "first_seen",
                             last_seen_col: str = "last_seen",
                             out_col: str = "user_is_active_employee") -> pd.DataFrame:
    """
    Standardize employment status across domains.

    Sets `out_col` to True when:
      - first_seen and last_seen are present, and
      - month_col is within [first_seen, last_seen] (inclusive).

    If bounds are missing, falls back to a best-effort proxy:
      out_col = df['joined_ldap'] (if present) or False.
    """
    # Normalize month to month-start if present.
    if month_col in df.columns:
        df[month_col] = month_start(df[month_col])

    has_bounds = (first_seen_col in df.columns) and (last_seen_col in df.columns)
    if has_bounds:
        fs = df[first_seen_col]
        ls = df[last_seen_col]
        if month_col in df.columns:
            m = df[month_col]
        else:
            m = month_start(pd.Series(pd.NaT, index=df.index))

        df[out_col] = (
            fs.notna()
            & ls.notna()
            & (m >= fs)
            & (m <= ls)
        )
    else:
        joined = df.get("joined_ldap")
        if joined is not None:
            df[out_col] = joined.fillna(False).astype(bool)
        else:
            df[out_col] = False

    return df

def ensure_seen_bounds(
    events: pd.DataFrame,
    ldap_asof: pd.DataFrame,
    user_col: str = "user_key",
    month_col: str = "event_month",
) -> pd.DataFrame:
    """
    Ensure that an events dataframe (already joined to LDAP as-of) has clean,
    month-start-normalized `first_seen` and `last_seen` columns.

    Behavior:
    - Trust LDAP as the source of truth for employment bounds.
    - If `first_seen` / `last_seen` are already present on `events`, they are
      normalized to month-start timestamps.
    - If either is missing, it is pulled directly from `ldap_asof`, which is
      expected to already contain `first_seen`/`last_seen` at the user level.
    - Any merge-suffix variants (first_seen_x/first_seen_y, etc.) are collapsed
      back into a single column.
    """
    df = events.copy()

    # Normalize event_month to month-start if present
    if month_col in df.columns:
        df[month_col] = month_start(df[month_col])

    # Normalize any existing first_seen / last_seen
    for col in ("first_seen", "last_seen"):
        if col in df.columns:
            df[col] = month_start(df[col])

    # If LDAP has bounds, prepare a per-user mapping
    has_ldap_bounds = all(c in ldap_asof.columns for c in ("first_seen", "last_seen"))
    if has_ldap_bounds:
        bounds = (
            ldap_asof[[user_col, "first_seen", "last_seen"]]
            .drop_duplicates(user_col)
            .copy()
        )
        bounds["first_seen"] = month_start(bounds["first_seen"])
        bounds["last_seen"] = month_start(bounds["last_seen"])
    else:
        bounds = None

    # If either bound is missing entirely on events, pull from LDAP
    missing = [c for c in ("first_seen", "last_seen") if c not in df.columns]
    if bounds is not None and missing:
        df = df.merge(bounds, on=user_col, how="left")

    # Collapse any merge-created suffix columns for seen-bounds
    for col in ("first_seen", "last_seen"):
        x, y = f"{col}_x", f"{col}_y"
        if x in df.columns or y in df.columns:
            base = df[col] if col in df.columns else pd.NaT
            if x in df.columns:
                base = base.fillna(df[x])
            if y in df.columns:
                base = base.fillna(df[y])
            df[col] = base
            drop_cols = [c for c in (x, y) if c in df.columns]
            if drop_cols:
                df.drop(columns=drop_cols, inplace=True)

    return df