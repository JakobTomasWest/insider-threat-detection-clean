

from pathlib import Path
import pandas as pd
import numpy as np

REL = "r5.2"

LABELS_PATH = Path(f"out/{REL}/labels/daily_labels.parquet")
OUT_DIR = Path(f"out/{REL}/ml_splits")
OUT_DIR.mkdir(parents=True, exist_ok=True)

def main():
    print("Loading daily_labels...")
    df = pd.read_parquet(LABELS_PATH)

    print("Extracting unique users...")
    users = df["user_key"].unique().tolist()

    print("Shuffling users with fixed seed...")
    rng = np.random.default_rng(42)
    rng.shuffle(users)

    n = len(users)
    t0 = int(0.70 * n)
    t1 = int(0.85 * n)

    train_users = set(users[:t0])
    val_users = set(users[t0:t1])
    test_users = set(users[t1:])

    print(f"Users: {n}, train={len(train_users)}, val={len(val_users)}, test={len(test_users)}")

    def assign_split(u):
        if u in train_users:
            return "train"
        if u in val_users:
            return "val"
        return "test"

    df["split"] = df["user_key"].apply(assign_split)

    out_path = OUT_DIR / "daily_labels_splits.parquet"
    df.to_parquet(out_path, index=False)
    print(f"Wrote: {out_path}")

if __name__ == "__main__":
    main()