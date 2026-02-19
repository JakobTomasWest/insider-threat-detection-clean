from pathlib import Path
import pandas as pd

REL = "r5.2"

DAILY_PATH = Path(f"out/{REL}/features_v2/daily_user/daily_user.parquet")
RANGES_PATH = Path(f"out/{REL}/labels/exfil_ranges.parquet")
OUT_PATH = Path(f"out/{REL}/labels")
OUT_PATH.mkdir(parents=True, exist_ok=True)

# This will be updated once the real daily_user schema is inspected
DATE_COL = "day"

def main():
    print("Loading daily_user...")
    daily = pd.read_parquet(DAILY_PATH)

    print("Loading exfil ranges...")
    ranges = pd.read_parquet(RANGES_PATH)

    print("Initial label setup...")
    daily["label"] = 0

    # Assign labels via any-overlap rule per user
    for user_key, grp in ranges.groupby("user_key"):
        mask = daily["user_key"] == user_key
        if mask.sum() == 0:
            continue

        user_days = daily.loc[mask, DATE_COL]

        for _, row in grp.iterrows():
            # exfil_start / exfil_end are already pandas Timestamps (datetime64[ns])
            # daily[DATE_COL] is also datetime64[ns], so compare like with like.
            start = row["exfil_start"]
            end = row["exfil_end"]

            in_range = (user_days >= start) & (user_days <= end)
            daily.loc[mask & in_range, "label"] = 1

    out_file = OUT_PATH / "daily_labels.parquet"
    daily.to_parquet(out_file, index=False)
    print(f"Wrote: {out_file}")

if __name__ == "__main__":
    main()
