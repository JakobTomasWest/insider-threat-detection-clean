import pandas as pd

def _ensure_ns_datetime(s: pd.Series) -> pd.Series:
    """Return a numpy-backed datetime64[ns] Series (works even if pyarrow dtype)."""
    s = pd.to_datetime(s, errors="coerce")
    try:
        # If this is an Arrow-backed datetime array, cast to numpy datetime64[ns]
        if getattr(s.array, "__class__", None).__name__.startswith("Arrow"):
            return s.astype("datetime64[ns]")
    except Exception:
        pass
    # Ensure consistent ns resolution even for numpy-backed series
    return s.astype("datetime64[ns]")

def parse_timestamp(df: pd.DataFrame, date_col: str, strict_fmt: str = "%m/%d/%Y %H:%M:%S") -> pd.Series:
    """Parse with format first; if <50% parse, fall back to flexible coerce."""
    ts = pd.to_datetime(df[date_col], format=strict_fmt, errors="coerce")
    if ts.notna().mean() < 0.5:
        ts = pd.to_datetime(df[date_col], errors="coerce")
    return _ensure_ns_datetime(ts)

def month_start(s: pd.Series) -> pd.Series:
    """Convert datetimes to month-start timestamps (period M)."""
    s = _ensure_ns_datetime(s)
    return s.dt.to_period("M").dt.to_timestamp(how="start")

def add_timestamp_and_month(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    ts = parse_timestamp(df, date_col)
    df["timestamp"]   = _ensure_ns_datetime(ts)
    df["event_month"] = month_start(df["timestamp"])
    return df