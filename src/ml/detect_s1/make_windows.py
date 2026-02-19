

from pathlib import Path
import pandas as pd

REL = "r5.2"

LABELS_SPLIT_PATH = Path(f"out/{REL}/ml_splits/daily_labels_splits.parquet")
OUT_DIR = Path(f"out/{REL}/windows")
OUT_DIR.mkdir(parents=True, exist_ok=True)

WINDOW = 14  # fixed by ml_assistant_context.md


def main():
    print("Loading daily_labels_splits...")
    df = pd.read_parquet(LABELS_SPLIT_PATH)

    for split_name in ["train", "val", "test"]:
        print(f"Processing split: {split_name}")
        df_split = df[df["split"] == split_name].copy()
        df_split = df_split.sort_values(["user_key", "day"]).reset_index(drop=True)
        rows = []
        for user_key, grp in df_split.groupby("user_key"):
            grp = grp.reset_index(drop=True)
            for i in range(len(grp)):
                if i < WINDOW - 1:
                    continue
                window_df = grp.iloc[i - WINDOW + 1 : i + 1]
                rows.append({
                    "user_key": user_key,
                    "end_day": window_df.iloc[-1]["day"],
                    "label": window_df.iloc[-1]["label"],
                    "window_json": window_df.to_json(orient="records"),
                })
        out_path = OUT_DIR / f"windows_{split_name}.parquet"
        if rows:
            pd.DataFrame(rows).to_parquet(out_path, index=False)
            print(f"Wrote {len(rows)} windows to {out_path}")
        else:
            print(f"WARNING: No windows generated for split '{split_name}', skipping write.")


if __name__ == "__main__":
    main()