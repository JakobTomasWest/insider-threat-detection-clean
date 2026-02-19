from pathlib import Path
import pandas as pd
from datetime import datetime

ANSWERS_ROOT = Path("answers") / "r5.2-1"
REL = "r5.2"
OUT_PATH = Path("out") / REL / "labels"
OUT_PATH.mkdir(parents=True, exist_ok=True)


def extract_timestamp_from_line(line: str):
    """
    Split on commas but only up to the 3rd comma.
    Column 3 (index 2) is ALWAYS the timestamp by project definition.
    """
    parts = line.strip().split(",", 3)  # at most 4 chunks
    if len(parts) < 3:
        return None  # not enough columns

    ts_str = parts[2].strip()
    try:
        return datetime.strptime(ts_str, "%m/%d/%Y %H:%M:%S")
    except ValueError:
        return None


def main():
    rows = []

    for path in sorted(ANSWERS_ROOT.glob("*.csv")):
        user_id = path.stem.split("-")[-1]
        user_key = user_id.lower()

        timestamps = []

        with path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                ts = extract_timestamp_from_line(line)
                if ts:
                    timestamps.append(ts)

        if not timestamps:
            print(f"WARNING: No timestamps found in {path}")
            continue

        exfil_start = min(timestamps).date()
        exfil_end = max(timestamps).date()

        rows.append(
            {
                "user_key": user_key,
                "scenario_id": "SCENARIO_1",
                "exfil_start": pd.to_datetime(exfil_start),
                "exfil_end": pd.to_datetime(exfil_end),
            }
        )

    if not rows:
        raise SystemExit("No exfil ranges built. Check answers folder.")

    df = pd.DataFrame(rows).sort_values(
        ["user_key", "scenario_id", "exfil_start"]
    )

    out_file = OUT_PATH / "exfil_ranges.parquet"
    df.to_parquet(out_file, index=False)
    print(f"Wrote {len(df)} ranges to {out_file}")


if __name__ == "__main__":
    main()