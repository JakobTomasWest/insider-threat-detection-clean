# Usage:
#   python scripts/compare_parquet.py \
#       --base  out/r3.1/ldap_v1/ldap_snapshots.parquet \
#       --trial out/r3.1/trial1/ldap_v1/ldap_snapshots.parquet \
#       --keys user_key event_month
import argparse
from pathlib import Path
import pandas as pd
from pandas.util import hash_pandas_object

def load_parquet(p: str) -> pd.DataFrame:
    fp = Path(p)
    if not fp.exists():
        raise FileNotFoundError(fp)
    return pd.read_parquet(fp)

def signature(df: pd.DataFrame, keys: list[str]) -> int:
    if not keys:
        return len(df)
    cols = [c for c in keys if c in df.columns]
    if not cols:
        return len(df)
    tmp = (
        df[cols]
        .astype(str)
        .fillna("<NA>")
        .drop_duplicates()
        .sort_values(cols)
        .reset_index(drop=True)
    )
    # Stable, version-agnostic hash
    return int(hash_pandas_object(tmp, index=False).sum())

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base",  required=True)
    ap.add_argument("--trial", required=True)
    ap.add_argument("--keys",  nargs="*", default=[])
    args = ap.parse_args()

    base  = load_parquet(args.base)
    trial = load_parquet(args.trial)

    print("rows:", len(base), "vs", len(trial))
    print("colsets equal:", set(base.columns) == set(trial.columns))

    shared = [c for c in base.columns if c in trial.columns]
    dtypes_equal = all(str(base[c].dtype) == str(trial[c].dtype) for c in shared)
    print("dtypes equal:", dtypes_equal)

    sig_base  = signature(base,  args.keys)
    sig_trial = signature(trial, args.keys)
    print(f"key-signature equal ({args.keys}):", sig_base == sig_trial)

    ok = (
        len(base) == len(trial)
        and set(base.columns) == set(trial.columns)
        and dtypes_equal
        and sig_base == sig_trial
    )
    raise SystemExit(0 if ok else 1)

if __name__ == "__main__":
    main()