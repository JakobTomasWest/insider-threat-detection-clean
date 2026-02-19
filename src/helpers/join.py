import pandas as pd

def normalize_month(df: pd.DataFrame, col: str) -> pd.DataFrame:
    df[col] = pd.to_datetime(df[col], errors="coerce").dt.to_period("M").dt.to_timestamp(how="start")
    return df

def left_join_ldap_by_month(events: pd.DataFrame, ldap_asof: pd.DataFrame):
    """Expect events with user_key,event_month; ldap_asof with same keys + attrs."""
    normalize_month(events, "event_month")
    normalize_month(ldap_asof, "event_month")
    out = events.merge(ldap_asof, on=["user_key","event_month"], how="left", suffixes=("", "_ldap"))
    out["joined_ldap"] = out.get("employee_name").notna() if "employee_name" in out.columns else False
    return out