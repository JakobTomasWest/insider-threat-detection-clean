from pathlib import Path
import json

from src.detector import anomaly as anomaly_mod

def main():
    # 1. Force the z-lookup / z-score table to load once
    if hasattr(anomaly_mod, "_load_zscores"):
        anomaly_mod._load_zscores()  # ensure z-score lookup is loaded
    else:
        raise RuntimeError("anomaly module has no _load_zscores; check file.")

    print("[SMOKE] z-lookup loaded OK.")

    # 2. Point to a *small* alerts file you trust
    # alerts_path = Path("out/r5.2/alerts_preflight/alerts.ndjson")
    alerts_path = Path("out/mvp0/alerts_ndjson_smoke/alerts.ndjson")
    if not alerts_path.exists():
        raise FileNotFoundError(f"{alerts_path} not found; run a short test run_loop first.")

    n_anom = 0
    n_non_null_z = 0
    n_boosted = 0

    with alerts_path.open() as f:
        for line in f:
            rec = json.loads(line)
            if rec.get("detector") != "anomaly":
                continue
            n_anom += 1
            ev = rec.get("evidence", {}) or {}
            z_p = ev.get("z_personal")
            z_r = ev.get("z_role")
            z_m = ev.get("z_max")
            boost_pct = ev.get("boost_pct", 0)

            if any(v is not None for v in (z_p, z_r, z_m)):
                n_non_null_z += 1
            if boost_pct and boost_pct != 0:
                n_boosted += 1

    print(f"[SMOKE] anomaly alerts:         {n_anom}")
    print(f"[SMOKE] with non-null z_*:      {n_non_null_z}")
    print(f"[SMOKE] with boost_pct != 0:    {n_boosted}")

    if n_anom == 0:
        print("[SMOKE] No anomaly alerts in test file; not ideal, but z-lookup path works.")
    elif n_non_null_z == 0:
        raise RuntimeError("[SMOKE] All anomaly z-scores are null. Do NOT run full run_loop.")
    else:
        print("[SMOKE] Anomaly z-scores are being populated. Safe to run full loop.")
        if n_boosted == 0:
            print("[SMOKE] Note: no boosting seen in this tiny sample, but z_* fields are valid.")


if __name__ == "__main__":
    main()