import re
import pandas as pd

# Strip domain like 'DTAA/AMA0606' or 'dtaa\AMA0606' -> 'AMA0606' -> 'ama0606'
_domain_prefix = re.compile(r'^[A-Za-z0-9._-]+[\\/]')

def normalize_user_series(s: pd.Series) -> pd.Series:
    s = s.astype(str).str.strip()
    s = s.str.replace(_domain_prefix, "", regex=True)
    s = s.str.lower()
    return s.where(s.ne(""), None)