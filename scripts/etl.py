#
# scripts/etl.py
from __future__ import annotations

import argparse
from pathlib import Path
import pandas as pd
import json

#
# nb_paths lives under notebooks/. Be resilient whether or not it's a package.
# Order: package import -> file import -> plain module import -> fail with guidance.
from pathlib import Path as _Path__nbpaths
import sys as _sys__nbpaths
import importlib.util as _ilu__nbpaths
from notebooks.nb_paths import iter_csv_chunks

def _load_nb_paths():
    # 1) Try package-style: notebooks.nb_paths
    try:
        from notebooks.nb_paths import bootstrap, read_csv, iter_csv_chunks  # type: ignore
        return bootstrap, read_csv
    except Exception:
        pass

    # 2) Try direct file import: <repo>/notebooks/nb_paths.py (even if 'notebooks' lacks __init__.py)
    repo_root = _Path__nbpaths(__file__).resolve().parents[1]
    nb_file = repo_root / "notebooks" / "nb_paths.py"
    if nb_file.exists():
        spec = _ilu__nbpaths.spec_from_file_location("notebooks.nb_paths", nb_file)
        if spec and spec.loader:
            nb_mod = _ilu__nbpaths.module_from_spec(spec)
            _sys__nbpaths.modules["notebooks.nb_paths"] = nb_mod
            spec.loader.exec_module(nb_mod)  # type: ignore
            return nb_mod.bootstrap, nb_mod.read_csv

    # 3) Last resort: a bare 'nb_paths' module on PYTHONPATH
    try:
        from nb_paths import bootstrap, read_csv  # type: ignore
        return bootstrap, read_csv
    except Exception as e:
        raise ModuleNotFoundError(
            "Cannot import nb_paths. Expected notebooks/nb_paths.py to exist. "
            "Run from repo root with `python -m scripts.etl ...` or set "
            "PYTHONPATH to include the project root and notebooks/."
        ) from e

bootstrap, read_csv = _load_nb_paths()

from src.helpers.io import out_dir, out_path
from src.helpers.users import normalize_user_series
from src.helpers.time import month_start, add_timestamp_and_month
from src.helpers.join import left_join_ldap_by_month
from src.helpers.domain_flags import ensure_seen_bounds, add_after_hours, add_active_employee_flag, load_ldap_asof
from src.helpers.emit import (
    emit_ldap,
    emit_logon_final,
    emit_device_final,
    emit_file_final,
    emit_http_final,
    emit_email_final,
)

def build_ldap(*, profile: str = "lean", family: str = "ldap_v3", overwrite: bool = False):
    env = bootstrap()
    ldap_dir = env.RAW / "LDAP"
    paths = sorted(ldap_dir.glob("*.csv"))
    if not paths:
        raise FileNotFoundError(f"No LDAP CSVs found under {ldap_dir}")

    WANT = [
        "employee_name", "email", "role",
        "business_unit", "functional_unit", "department", "team", "supervisor",
    ]

    def _snap(p: Path) -> pd.Timestamp:
        y, m = p.stem.split("-")
        return pd.Timestamp(year=int(y), month=int(m), day=1)

    required = {"user_id"}
    got = set(map(str.lower, pd.read_csv(paths[0], nrows=0).columns))
    missing = required - got
    if missing:
        raise AssertionError(f"LDAP missing required columns: {sorted(missing)}")
    
    parts = []
    for p in paths:
        df = pd.read_csv(p, dtype=str)
        df["user_key"] = normalize_user_series(df["user_id"])
        df["snapshot_date"] = _snap(p)
        keep = ["user_key", "snapshot_date"] + [c for c in WANT if c in df.columns]
        parts.append(df[keep])

    snapshots = (
        pd.concat(parts, ignore_index=True)
          .sort_values(["user_key","snapshot_date"], kind="stable")
          .drop_duplicates(["user_key","snapshot_date"], keep="last")
          .reset_index(drop=True)
    )
    snapshots["snapshot_date"] = pd.to_datetime(snapshots["snapshot_date"])  # ensure ts

    # Normalize email early so every downstream join sees a clean value
    if "email" in snapshots.columns:
        snapshots["email"] = (
            snapshots["email"].astype(str).str.strip().str.lower().replace({"": pd.NA})
        )
        # Optional: keep a domain column for quick audits/filters
        snapshots["email_domain"] = snapshots["email"].str.extract(r"@([\w\.-]+)$")

    asof = (
        snapshots.assign(event_month=lambda d: month_start(d["snapshot_date"]))
                 [["user_key","event_month"] + [c for c in WANT if c in snapshots.columns] + (["email_domain"] if "email_domain" in snapshots.columns else [])]
                 .drop_duplicates(["user_key","event_month"])
                 .sort_values(["user_key","event_month"]).reset_index(drop=True)
    )

    # --- Enrich as-of with last_seen and supervisor_key (strict NAME mapping, no email fallback) ---
    # first_seen: first month this user appears in snapshots
    first_seen = (
        snapshots.assign(event_month=lambda d: month_start(d["snapshot_date"]))
                 .groupby("user_key", as_index=False)["event_month"].min()
                 .rename(columns={"event_month": "first_seen"})
    )
    first_seen["first_seen"] = month_start(first_seen["first_seen"])
    # last_seen: last month this user appears in snapshots
    last_seen = (
        snapshots.assign(event_month=lambda d: month_start(d["snapshot_date"]))
                 .groupby("user_key", as_index=False)["event_month"].max()
                 .rename(columns={"event_month": "last_seen"})
    )
    last_seen["last_seen"] = month_start(last_seen["last_seen"])

    def _norm(s: pd.Series) -> pd.Series:
        s = s.astype(str).str.strip().str.lower()
        return s.str.replace(r"\s+", " ", regex=True)

    snaps_for_map = snapshots.copy()
    snaps_for_map["event_month"] = month_start(snaps_for_map["snapshot_date"])
    snaps_for_map["employee_name_l"] = _norm(snaps_for_map.get("employee_name", pd.Series(index=snaps_for_map.index, dtype=object)))

    name_map = (
        snaps_for_map.dropna(subset=["employee_name_l"])
                     .drop_duplicates(["event_month","employee_name_l"])
                     .set_index(["event_month","employee_name_l"])["user_key"]
    )

    asof = asof.copy()
    asof["event_month"] = month_start(asof["event_month"])
    asof["supervisor_l"] = _norm(asof.get("supervisor", pd.Series(index=asof.index, dtype=object)))

    # Guard: require supervisor to be NAME-like (no emails). Fail early if violated.
    email_like = asof["supervisor_l"].str.contains("@", na=False)
    if bool(email_like.any()):
        bad = asof.loc[email_like, ["user_key","event_month","supervisor"]].head(20)
        raise AssertionError(
            "LDAP supervisor field contains email-like values; pipeline requires NAME-only supervisor. "
            "Examples (first 20):\n" + bad.to_csv(index=False)
        )

    # Resolve strictly by NAME within the same month
    idx_name = list(zip(asof["event_month"], asof["supervisor_l"]))
    asof["supervisor_key"] = pd.Series([name_map.get(k) for k in idx_name], index=asof.index)

    # Attach first_seen and last_seen
    asof = asof.merge(first_seen, on="user_key", how="left", validate="m:1")
    asof = asof.merge(last_seen,  on="user_key", how="left", validate="m:1")

    out_dir(env, family).mkdir(parents=True, exist_ok=True)
    if "role" in snapshots.columns:
        snapshots["role"] = snapshots["role"].astype(str).str.strip()
    written = emit_ldap(env, df_snapshots=snapshots, df_asof=asof,
                        profile=profile, family=family, overwrite=overwrite)

    print("[LDAP] wrote:")
    for k, p in written.items():
        print("  ", k, "->", p)

    assert snapshots.duplicated(["user_key","snapshot_date"]).sum() == 0

# -----------------
# LOGON pipeline
# -----------------

def build_logon(*, profile: str = "lean", family: str = "logon_v3",
                ldap_family_for_join: str = "ldap_v3_lean", overwrite: bool = False):
    env = bootstrap()

    # Clean
    df = read_csv(env, "logon")
    df = add_timestamp_and_month(df, "date")
    df["user_key"] = normalize_user_series(df["user"])
    df["user_raw"] = df["user"]
    if "activity" in df.columns:
        df["activity"] = df["activity"].astype(str).str.strip().str.lower()

    if "id" in df.columns and df["id"].notna().any():
        df = df.drop_duplicates("id")
    else:
        df = df.drop_duplicates(["timestamp","user_key","pc","activity"], keep="first")

    clean_cols = ["timestamp","event_month","user_key","user_raw","pc","activity","id","date"]
    logon_clean = df[[c for c in clean_cols if c in df.columns]].copy()

    # LDAP join (as-of)
    ldap_asof = load_ldap_asof(env, ldap_family_for_join)

    logon_enriched = left_join_ldap_by_month(logon_clean, ldap_asof)

    # Bounds + reason + after-hours (avoid column-collision on 'last_seen')
    # Centralized helper: normalize or derive first_seen/last_seen from LDAP as-of.
    e2 = ensure_seen_bounds(logon_enriched, ldap_asof)

    def _reason(em, fs, ls):
        if pd.isna(fs):
            return "not_in_ldap"
        # Normalize a scalar event_month to month-start without using month_start (expects Series)
        em = pd.Timestamp(em).to_period('M').to_timestamp(how='start')
        if em < fs:
            return "pre_hire"
        if em > ls:
            return "post_departure"
        return "matched"
    e2["join_reason"] = [
        _reason(em, fs, ls)
        for em, fs, ls in zip(e2["event_month"], e2["first_seen"], e2["last_seen"])
    ]

    # Normalize timestamps once here; downstream helpers reuse this for flags.
    e2["timestamp"] = pd.to_datetime(e2["timestamp"], errors="coerce")

    # Standardized flags shared across domains
    e2 = add_after_hours(e2, ts_col="timestamp")
    e2 = add_active_employee_flag(
        e2,
        month_col="event_month",
        first_seen_col="first_seen",
        last_seen_col="last_seen",
        out_col="user_is_active_employee",
    )

    logon_enriched_v3 = e2

    # --- Robust Shared-PC + Assigned-PC derivation ---
    # Goal: identify ~100 lab-like shared PCs using a month-wise distinct non-admin user signal.
    # Steps:
    #   1) Exclude admins using LDAP role (treat "itadmin" and "it admin" as admin).
    #   2) Compute per-(pc, event_month) distinct non-admin users from the enriched table.
    #   3) Take the monthly MAX per PC as the sharedness metric.
    #   4) Choose threshold K automatically to yield ~100 shared PCs (95..120); fallback to top-100 cutoff.
    #   5) Keep optional dominance metric (top_user_share) for audit, but do NOT use it to filter count down.
    en = logon_enriched.copy()
    # Prefer explicit is_admin if present, else fall back to role parsing
    if "is_admin" in en.columns:
        is_admin_user = en["is_admin"].fillna(False).astype(bool)
    else:
        role_norm = en.get("role", pd.Series(index=en.index, dtype=object)).astype(str).str.strip().str.lower()
        is_admin_user = role_norm.replace({"it admin": "itadmin"}).eq("itadmin")
    admin_keys = set(en.loc[is_admin_user, "user_key"].dropna().astype(str).unique())

    # Ensure month field exists/normalized
    en["event_month"] = month_start(en["event_month"])
    en = en[en["pc"].notna()].copy()
    en["pc"] = en["pc"].astype(str)
    en["user_key"] = en["user_key"].astype(str)

    # Only consider non-admins for "sharedness"
    en_na = en[~en["user_key"].isin(admin_keys)].copy()

    # Per-(pc, month) distinct non-admin users
    per_pc_month = (
        en_na.groupby(["pc", "event_month"])["user_key"]
             .nunique()
             .rename("distinct_non_admin_users")
             .reset_index()
    )

    # Monthly MAX per PC (lab-like machines have a high monthly max)
    pc_monthly_max = (
        per_pc_month.groupby("pc")["distinct_non_admin_users"]
                    .max()
                    .rename("max_non_admin_users_in_a_month")
                    .reset_index()
                    .sort_values("max_non_admin_users_in_a_month", ascending=False)
                    .reset_index(drop=True)
    )

    # Optional dominance (for QC only)
    pc_user_counts = en_na.groupby(["pc", "user_key"]).size().rename("events").reset_index()
    per_pc_events = pc_user_counts.groupby("pc")["events"].sum().rename("pc_events")
    per_pc_top = (
        pc_user_counts.sort_values(["pc", "events"], ascending=[True, False])
                      .groupby("pc").head(1)
                      .merge(per_pc_events, on="pc", how="left")
    )
    per_pc_top["top_user_share"] = per_pc_top["events"] / per_pc_top["pc_events"]
    dominance = per_pc_top[["pc", "top_user_share"]]

    # Auto-choose K to land ~100 shared PCs
    desired_N = 100
    counts = pc_monthly_max["max_non_admin_users_in_a_month"].astype(int).to_numpy()
    chosen_K, approx_ct = None, None
    feasible = []
    if len(counts):
        unique_vals = sorted(set(int(x) for x in counts), reverse=True)
        cumsum = {k: int((counts >= k).sum()) for k in unique_vals}
        feasible = [(k, c) for k, c in sorted(cumsum.items(), key=lambda x: (-x[0], x[1])) if 95 <= c <= 120]
        if feasible:
            # pick the largest K that still yields in-range count (more conservative)
            chosen_K, approx_ct = sorted(feasible, key=lambda x: x[0])[-1]

    # Final shared set:
    if chosen_K is not None:
        # Threshold path (within target band)
        shared_base = pc_monthly_max.loc[pc_monthly_max["max_non_admin_users_in_a_month"] >= chosen_K].copy()
    else:
        # Exact Top-N fallback to avoid giant tie sets (e.g., K=2 -> thousands)
        shared_base = pc_monthly_max.sort_values("max_non_admin_users_in_a_month", ascending=False).head(desired_N).copy()

    # Attach dominance metric for QC preview (not used for filtering)
    shared_pcs = shared_base.merge(dominance, on="pc", how="left")
    # For backward-compat with earlier previews, expose the metric under "distinct_non_admin_users"
    shared_pcs = shared_pcs.rename(columns={
        "max_non_admin_users_in_a_month": "distinct_non_admin_users"
    })
    shared_pcs["shared_pc"] = True
    approx_ct = int(len(shared_pcs)) if chosen_K is None else approx_ct

    # Assigned PC per user:
    # Use only non-shared PCs and business-hours logons to avoid noise.
    # Require at least 5 distinct days observed and the modal PC to cover >=50% of those days.
    e2_tmp = e2.copy()
    e2_tmp["pc"] = e2_tmp["pc"].astype(str)
    shared_set = set(shared_pcs["pc"].unique().tolist())
    # business hours ~= 07:00..18:59 (mirror after_hours_login definition)
    e2_tmp["is_business_hours"] = ~(e2_tmp["timestamp"].dt.hour.lt(7) | e2_tmp["timestamp"].dt.hour.ge(19))
    e2_bh = e2_tmp[
        e2_tmp["pc"].notna()
        & ~e2_tmp["pc"].isin(shared_set)
        & e2_tmp["is_business_hours"]
    ].copy()

    # day-level signal to reduce rapid re-logon noise
    e2_bh["day"] = e2_bh["timestamp"].dt.floor("D")

    # Count days per (user, pc)
    per_user_pc_days = (
        e2_bh.groupby(["user_key", "pc"])["day"].nunique()
             .rename("days_on_pc").reset_index()
    )
    # Total observed days per user
    per_user_days = per_user_pc_days.groupby("user_key")["days_on_pc"].sum().rename("user_days")
    # Pick modal PC by days; require dominance >= 50% and at least 5 total observed days
    per_user_pc_days = per_user_pc_days.merge(per_user_days, on="user_key", how="left")
    per_user_pc_days["share"] = per_user_pc_days["days_on_pc"] / per_user_pc_days["user_days"].replace(0, pd.NA)

    # modal pick
    modal_idx = per_user_pc_days.sort_values(
        ["user_key", "days_on_pc", "pc"], ascending=[True, False, True]
    ).groupby("user_key").head(1).index
    assigned_pc = per_user_pc_days.loc[modal_idx, ["user_key", "pc", "days_on_pc", "user_days", "share"]].copy()
    # enforce stability rules
    assigned_pc.loc[~((assigned_pc["user_days"] >= 5) & (assigned_pc["share"] >= 0.50)), "pc"] = pd.NA
    assigned_pc = assigned_pc.rename(columns={"pc": "assigned_pc"})

    # --- Attach PC-context flags onto enriched_v3 (e2) ---
    sp_set = set(shared_pcs.loc[shared_pcs["shared_pc"] == True, "pc"].astype(str).unique())
    e2["pc"] = e2["pc"].astype(str)
    assigned_pc["assigned_pc"] = assigned_pc["assigned_pc"].astype("string")
    e2 = e2.merge(assigned_pc[["user_key", "assigned_pc"]], on="user_key", how="left")

    e2["on_shared_pc"] = e2["pc"].isin(sp_set)
    e2["on_unassigned_pc"] = (
        e2["pc"].notna()
        & e2["assigned_pc"].notna()
        & e2["pc"].ne(e2["assigned_pc"])
    )

    # Build FULL (audit-rich) — keep raw traceability + broad LDAP context

    # Build FULL (audit-rich) — keep raw traceability + broad LDAP context
    full_cols = [c for c in [
        # event
        "timestamp","event_month","user_key","user_raw","pc","activity",
        # LDAP context (rich)
        "email","role","is_admin","team","supervisor_key","first_seen","last_seen",
        "employee_name","business_unit","functional_unit","department","supervisor",
        # risk flags
        "after_hours","on_shared_pc","on_unassigned_pc","user_is_active_employee",
        # optional ids for traceability if present
        "id","date",
    ] if c in e2.columns]
    logon_full = e2[full_cols].copy()

    # Build LEAN (model-ready) — compact subset
    lean_cols = [c for c in [
        "timestamp","event_month","user_key","pc","activity",
        "role","is_admin","first_seen","supervisor_key","team",
        "after_hours","on_shared_pc","on_unassigned_pc","user_is_active_employee",
    ] if c in e2.columns]
    logon_lean = e2[lean_cols].copy()

    # Emit exactly two artifacts
    written = emit_logon_final(
        env,
        df_full=logon_full,
        df_lean=logon_lean,
        family=family,
        overwrite=overwrite,
    )

    # (Compatibility) Also write the tiny helper parquets used by other domains today.
    # These are not "domain outputs" — they are helper artifacts to avoid breaking DEVICE/HTTP/FILE.
    shared_p_path   = out_path(env, family, "shared_pcs")
    assigned_p_path = out_path(env, family, "assigned_pc")
    shared_pcs[["pc", "distinct_non_admin_users", "top_user_share", "shared_pc"]].to_parquet(shared_p_path, engine="pyarrow", index=False)
    assigned_pc[["user_key", "assigned_pc", "days_on_pc", "user_days", "share"]].to_parquet(assigned_p_path, engine="pyarrow", index=False)
    print(f"  helper -> {shared_p_path}")
    print(f"  helper -> {assigned_p_path}")

    # --- QC rollups to out/<release>/qc ---
    qc_dir = Path(env.OUT) / env.RELEASE / "qc"
    qc_dir.mkdir(parents=True, exist_ok=True)

    # Shared PC candidates (for quick human audit)
    sp_cols = [c for c in ["pc","distinct_non_admin_users","top_user_share"] if c in shared_pcs.columns]
    sp_qc_pq = qc_dir / "shared_pc_candidates.parquet"
    try:
        shared_pcs[sp_cols].to_parquet(sp_qc_pq, engine="pyarrow", index=False)
    except Exception:
        # fall back to whatever columns exist
        shared_pcs.to_parquet(sp_qc_pq, engine="pyarrow", index=False)

    count_shared = None
    if "shared_pc" in shared_pcs.columns:
        count_shared = int(shared_pcs.loc[shared_pcs["shared_pc"] == True, "pc"].nunique())
    else:
        count_shared = int(shared_pcs["pc"].nunique()) if "pc" in shared_pcs.columns else int(len(shared_pcs))

    preview_cols = [c for c in ["pc","distinct_non_admin_users","top_user_share"] if c in shared_pcs.columns]
    preview = shared_pcs[preview_cols].head(10).to_dict(orient="records")

    meta = {
        "selection": "monthly max distinct non-admin users per PC (K if feasible, else exact Top-100)",
        "method": "threshold-K" if chosen_K is not None else "top-100",
        "chosen_K": int(chosen_K) if chosen_K is not None else None,
        "approx_count_PCs_ge_K_or_topN": int(approx_ct) if approx_ct is not None else None,
        "count_shared_pcs": count_shared,
        "preview_first_10": preview,
    }
    import json
    (qc_dir / "shared_pc_candidates_meta.json").write_text(json.dumps(meta, indent=2))

    # Simple markdown preview
    def _mk_md_table(df):
        if df.empty:
            return "_no rows_"
        cols = [str(c) for c in df.columns]
        rows = ["| " + " | ".join(map(str, r)) + " |" for r in df.itertuples(index=False, name=None)]
        return "\n".join([
            "| " + " | ".join(cols) + " |",
            "| " + " | ".join(["---"]*len(cols)) + " |",
            *rows
        ])

    md_lines = [
        "# Shared PC candidates",
        f"- Shared PCs: **{count_shared}**",
        "",
        "## Preview (first 10)",
        _mk_md_table(shared_pcs[preview_cols].head(10)) if preview_cols else "_no preview columns_"
    ]
    (qc_dir / "shared_pc_candidates.md").write_text("\n".join(md_lines))

    # Assigned PC sample
    ap_cols = [c for c in ["user_key","assigned_pc","days_on_pc","user_days","share"] if c in assigned_pc.columns]
    ap_sample = assigned_pc[ap_cols].head(10) if ap_cols else assigned_pc.head(10)
    ap_lines = [
        "# Assigned PC sample (first 10)",
        _mk_md_table(ap_sample)
    ]
    (qc_dir / "assigned_pc_sample.md").write_text("\n".join(ap_lines))

    print("[LOGON] wrote:")
    print("   full ->", written["full"])
    print("   lean ->", written["lean"])

# -----------------
# DEVICE pipeline
# -----------------

def build_device(*, profile: str = "lean", family: str = "device_v3",
                 ldap_family_for_join: str = "ldap_v3_lean",
                 logon_family_for_pc: str = "logon_v3",
                 overwrite: bool = False):
    env = bootstrap()

    # Clean
    df = read_csv(env, "device")  # expects id/date/user/pc/activity
    df = add_timestamp_and_month(df, "date")
    df["user_key"] = normalize_user_series(df["user"])
    df["user_raw"] = df["user"]

    # Drop duplicates by id when available; otherwise use a stable tuple
    if "id" in df.columns and df["id"].notna().any():
        df = df.drop_duplicates("id")
    else:
        df = df.drop_duplicates(["timestamp", "user_key", "pc", "activity"], keep="first")

    clean_cols = [
        "timestamp", "event_month", "user_key", "user_raw",
        "pc", "file_tree", "activity", "id", "date",
    ]
    dev_clean = df[[c for c in clean_cols if c in df.columns]].copy()

    # normalize strings that vary across releases
    if "activity" in dev_clean.columns:
        dev_clean["activity"] = dev_clean["activity"].astype(str).str.strip().str.lower()
    # file_tree is optional (missing on older releases); keep if present, else no-op

    # LDAP join
    ldap_asof = load_ldap_asof(env, ldap_family_for_join)

    dev_enriched = left_join_ldap_by_month(dev_clean, ldap_asof)

    # Normalize / derive first_seen / last_seen and clean bounds via shared helper.
    # This keeps employment bounds consistent with LOGON and FILE.
    e2 = ensure_seen_bounds(dev_enriched, ldap_asof)

    # Risk flags (v3): timestamp + month + after-hours
    e2["timestamp"] = pd.to_datetime(e2["timestamp"], errors="coerce")
    e2 = add_after_hours(e2, ts_col="timestamp")

    # Shared PCs + Assigned PC from LOGON artifacts (REQUIRED)
    #
    # We intentionally ignore the logon_family_for_pc knob here to avoid misaligned paths:
    # always expect helpers under logon_v3. If missing, fail loudly.
    helper_family = "logon_v3"  # helpers live under out/<REL>/logon_v3/
    shared_path = out_path(env, helper_family, "shared_pcs")
    assigned_path = out_path(env, helper_family, "assigned_pc")

    missing_helpers = []
    if not Path(shared_path).exists():
        missing_helpers.append(str(shared_path))
    if not Path(assigned_path).exists():
        missing_helpers.append(str(assigned_path))

    if missing_helpers:
        msg_lines = [
            "[DEVICE] required LOGON helper artifacts are missing.",
            "Expected shared/assigned PC helpers at:",
            *[f"  - {p}" for p in missing_helpers],
            "",
            "Run the LOGON stage first so these helpers exist, for example:",
            "  make build ldap logon device",
        ]
        raise FileNotFoundError("\n".join(msg_lines))

    # Shared-PC flag: simple set membership on PC string.
    sp = pd.read_parquet(shared_path)
    pc_col = "pc" if "pc" in sp.columns else sp.columns[0]
    pcs = (
        sp[pc_col]
        .dropna()
        .astype(str)
        .unique()
    )
    shared_set: set[str] = set(pcs)
    print(f"[DEVICE] shared_pcs loaded from {shared_path}: {len(shared_set)} PCs")
    if len(shared_set):
        sample = sorted(list(shared_set))[:5]
        print(f"[DEVICE] shared_pcs sample: {sample}")

    if "pc" in e2.columns:
        e2["pc"] = e2["pc"].astype(str)
        e2["on_shared_pc"] = e2["pc"].isin(shared_set)
    else:
        e2["on_shared_pc"] = False

    # Assigned PC per user (required to be present if we got this far)
    ap = pd.read_parquet(assigned_path)
    if {"user_key", "assigned_pc"}.issubset(ap.columns):
        ap = ap.astype({"user_key": str, "assigned_pc": str})
        e2 = e2.merge(ap[["user_key", "assigned_pc"]], on="user_key", how="left")
    else:
        # If schema is wrong, fail loudly as well.
        raise ValueError(
            "[DEVICE] assigned_pc helper is missing required columns "
            "'user_key' and 'assigned_pc' – check LOGON emit logic."
        )

    pc_cmp = e2["pc"].astype(str).fillna("")
    ap_cmp = e2["assigned_pc"].astype(str).fillna("")
    e2["on_unassigned_pc"] = e2["assigned_pc"].notna() & (pc_cmp != ap_cmp)

    # Standard employment flag derived from LDAP bounds (or joined_ldap fallback)
    e2 = add_active_employee_flag(
        e2,
        month_col="event_month",
        first_seen_col="first_seen",
        last_seen_col="last_seen",
        out_col="user_is_active_employee",
    )

    #Debug: how many rows are flagged as shared / unassigned?
    # if "on_shared_pc" in e2.columns:
        # print("[DEVICE] on_shared_pc value_counts:",
            #   e2["on_shared_pc"].value_counts(dropna=False).to_dict())
    # if "on_unassigned_pc" in e2.columns:
        # print("[DEVICE] on_unassigned_pc value_counts:",
            #   e2["on_unassigned_pc"].value_counts(dropna=False).to_dict())

    full_cols = [c for c in [
        # event
        "timestamp", "event_month", "user_key", "user_raw", "pc", "file_tree", "activity",
        # org context (enriched)
        "email", "role", "is_admin", "team", "supervisor_key", "first_seen", "last_seen",
        "employee_name", "business_unit", "functional_unit", "department", "supervisor",
        # risk flags
        "after_hours", "on_shared_pc", "on_unassigned_pc", "user_is_active_employee",
        # ids for traceability if present
        "id", "date",
    ] if c in e2.columns]
    dev_full = e2[full_cols].copy()

    lean_cols = [c for c in [
        "timestamp", "event_month", "user_key", "pc", "file_tree", "activity",
        "role", "is_admin", "supervisor_key", "team",
        "after_hours", "on_shared_pc", "on_unassigned_pc", "user_is_active_employee",
    ] if c in e2.columns]
    dev_lean = e2[lean_cols].copy()

    written = emit_device_final(
        env,
        df_full=dev_full,
        df_lean=dev_lean,
        family=family,
        overwrite=overwrite,
    )
    print("[DEVICE] wrote:")
    print("   full ->", written["full"])
    print("   lean ->", written["lean"])


# -----------------
# FILE pipeline
# -----------------
def build_file(*, profile: str = "lean", family: str = "file_v3",
               ldap_family_for_join: str = "ldap_v3_lean",
               logon_family_for_pc: str = "logon_v3",
               overwrite: bool = False,
               reemit: bool = False):
    env = bootstrap()

    # Clean
    df = read_csv(env, "file")  # r3.1: id,date,user,pc,filename,content; r5.1 adds: activity,to_removable_media,from_removable_media
    df = add_timestamp_and_month(df, "date")
    df["user_key"] = normalize_user_series(df["user"])
    df["user_raw"] = df["user"]

    # normalize string-ish cols if present
    for col in ("activity", "filename", "pc"):
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()

    # drop dupes by id when present, else by a stable tuple
    if "id" in df.columns and df["id"].notna().any():
        df = df.drop_duplicates("id")
    else:
        uniq_keys = [c for c in ["timestamp","user_key","pc","filename","activity"] if c in df.columns]
        df = df.drop_duplicates(uniq_keys, keep="first")

    # lightweight bool coercion for removable-media flags if they exist
    for col in ("to_removable_media", "from_removable_media"):
        if col in df.columns:
            s = df[col].astype(str).str.strip().str.lower()
            df[col] = s.isin(["1","true","t","yes","y"])

    clean_cols = [
        "timestamp","event_month","user_key","user_raw","pc","filename","activity",
        "to_removable_media","from_removable_media","content","id","date"
    ]
    file_clean = df[[c for c in clean_cols if c in df.columns]].copy()

    # LDAP join (as-of)
    ldap_asof = load_ldap_asof(env, ldap_family_for_join)
    file_enriched = left_join_ldap_by_month(file_clean, ldap_asof)

    # Normalize / derive first_seen / last_seen and clean bounds via shared helper.
    # This keeps employment bounds consistent with LOGON and DEVICE.
    e2 = ensure_seen_bounds(file_enriched, ldap_asof)

    # Risk & convenience flags
    e2["timestamp"] = pd.to_datetime(e2["timestamp"], errors="coerce")
    e2 = add_after_hours(e2, ts_col="timestamp")

    # 'is_keylogger' if filename equals 'keylogger' (case-insensitive), with or without extension
    if "filename" in e2.columns:
        base = e2["filename"].astype(str).str.strip()
        name_only = base.str.replace(r".*[\\/]", "", regex=True)                # drop path
        stem = name_only.str.replace(r"\.[^.]+$", "", regex=True)               # drop extension
        e2["is_keylogger"] = stem.str.lower().eq("keylogger")

    # Bring in shared/unassigned flags via LOGON helpers if available
    shared_path = out_path(env, logon_family_for_pc, "shared_pcs")
    assigned_path = out_path(env, logon_family_for_pc, "assigned_pc")

    shared_set = set()
    if Path(shared_path).exists():
        sp = pd.read_parquet(shared_path)
        pc_col = "pc" if "pc" in sp.columns else sp.columns[0]
        shared_set = set(sp[pc_col].dropna().astype(str))

    if Path(assigned_path).exists():
        ap = pd.read_parquet(assigned_path)
        if {"user_key","assigned_pc"}.issubset(ap.columns):
            ap = ap.astype({"user_key": str, "assigned_pc": str})
            e2 = e2.merge(ap[["user_key","assigned_pc"]], on="user_key", how="left")
        else:
            e2["assigned_pc"] = pd.NA
    else:
        e2["assigned_pc"] = pd.NA

    if "pc" in e2.columns:
        e2["pc"] = e2["pc"].astype(str)

        e2["on_shared_pc"] = e2["pc"].isin(shared_set) if shared_set else False
        pc_cmp = e2["pc"].astype(str).fillna("")
        ap_cmp = e2["assigned_pc"].astype(str).fillna("")
        e2["on_unassigned_pc"] = e2["assigned_pc"].notna() & (pc_cmp != ap_cmp)
    else:
        e2["on_shared_pc"] = False
        e2["on_unassigned_pc"] = False

    # Employment flag from LDAP bounds (or joined_ldap fallback)
    e2 = add_active_employee_flag(
        e2,
        month_col="event_month",
        first_seen_col="first_seen",
        last_seen_col="last_seen",
        out_col="user_is_active_employee",
    )

    # Final two tables
    full_cols = [c for c in [
        "timestamp","event_month","user_key","user_raw","pc","filename","activity",
        "to_removable_media","from_removable_media","content",
        "email","role","is_admin","team","supervisor_key","first_seen","last_seen",
        "employee_name","business_unit","functional_unit","department","supervisor",
        "after_hours","on_shared_pc","on_unassigned_pc","user_is_active_employee",
        "is_keylogger","id","date",
    ] if c in e2.columns]
    file_full = e2[full_cols].copy()

    lean_cols = [c for c in [
        "timestamp","event_month","user_key","pc","filename","activity",
        "to_removable_media","from_removable_media",
        "role","is_admin","supervisor_key","team",
        "after_hours","on_shared_pc","on_unassigned_pc","user_is_active_employee",
        "is_keylogger",
    ] if c in e2.columns]
    file_lean = e2[lean_cols].copy()

    written = emit_file_final(
        env,
        df_full=file_full,
        df_lean=file_lean,
        family=family,
        overwrite=overwrite,
    )
    print("[FILE] wrote:")
    print("   full ->", written["full"])
    print("   lean ->", written["lean"])

# -----------------
# HTTP pipeline (streaming, low-memory)
# -----------------
def build_http(*, profile: str = "lean", family: str = "http_v3",
                ldap_family_for_join: str = "ldap_v3_lean",
                logon_family_for_pc: str = "logon_v3",
                overwrite: bool = False,
                reemit: bool = False):
    """Build HTTP artifacts in a memory-safe way using chunked IO.

    Writes **two final** files under out/<release>/<family>/ :
      - http_full.parquet  (rich, audit-friendly)
      - http_lean.parquet  (compact, model-ready)
    Employment flag: `user_is_active_employee` (mirrors DEVICE). No separate `active_this_month` / `user_in_ldap_month`.
    The `reemit` argument is accepted for CLI compatibility and is ignored.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq
    from urllib.parse import urlparse
    import pandas as pd

    # --- Host helpers + Scenario-2 jobsite discovery ---
    from urllib.parse import urlparse
    import re
    from collections import Counter

    _URL_RE = re.compile(r"https?://\S+", re.I)

    def _host_of(u: str):
        try:
            h = urlparse(u).hostname
            return h.lower() if h else None
        except Exception:
            return None

    def _registrable(host: str):
        parts = (host or "").split(".")
        return ".".join(parts[-2:]) if len(parts) >= 2 else host

    def _discover_jobsite_hosts(env, top_n: int = 50) -> set[str]:
        """
        Parse Scenario 2 answer files under answers/<release>-2* to extract
        frequently occurring registrable domains. Returns a set of up to top_n.
        """
        answers_dir = env.PROJECT / "answers"
        files = sorted(answers_dir.glob(f"{env.RELEASE}-2*"))
        if not files:
            return set()
        counts = Counter()
        for p in files:
            try:
                txt = p.read_text(encoding="utf-8", errors="replace")
            except Exception:
                continue
            for m in _URL_RE.finditer(txt):
                h = _host_of(m.group(0))
                if h:
                    counts[_registrable(h)] += 1
        return set([d for d, _ in counts.most_common(top_n)])

    env = bootstrap()

    # Discover job site domains once (best-effort; empty set if none found)
    JOBSITE_HOSTS = _discover_jobsite_hosts(env, top_n=50)
    # Stable sets for specific destinations
    WIKI_HOSTS = {"wikileaks.org"}
    DROPBOX_HOSTS = {"dropbox.com", "dropboxusercontent.com", "dropboxstatic.com"}

    # Resolve input CSV path (schema-aware helper)
    from notebooks.nb_paths import csv_path, iter_csv_chunks
    HTTP_PATH = csv_path(env, "http")
    chunk_iter = lambda: iter_csv_chunks(env, "http", chunksize=500_000, dtype=str)

    # Output dirs/files (final two-artifact contract)
    base_dir = out_dir(env, family)
    full_dir = base_dir / f"{family}_full"
    lean_dir = base_dir / f"{family}_lean"
    full_dir.mkdir(parents=True, exist_ok=True)
    lean_dir.mkdir(parents=True, exist_ok=True)
    FULL_P = full_dir / "http_full.parquet"
    LEAN_P = lean_dir / "http_lean.parquet"

    # Overwrite safety
    if overwrite:
        for pth in (FULL_P, LEAN_P):
            if pth.exists():
                pth.unlink()

    # Load small lookups in memory
    ldap_asof = load_ldap_asof(env, ldap_family_for_join)

    shared_path = out_path(env, logon_family_for_pc, "shared_pcs")
    assigned_path = out_path(env, logon_family_for_pc, "assigned_pc")

    shared_set = set()
    if Path(shared_path).exists():
        sp = pd.read_parquet(shared_path)
        pc_col = "pc" if "pc" in sp.columns else sp.columns[0]
        shared_set = set(sp[pc_col].dropna().astype(str))

    assigned_map = {}
    if Path(assigned_path).exists():
        ap = pd.read_parquet(assigned_path)
        if {"user_key","assigned_pc"}.issubset(ap.columns):
            assigned_map = dict(zip(ap["user_key"].astype(str), ap["assigned_pc"].astype(str)))

    # ----- Helpers: URL metrics -----
    def _url_len(u: str) -> int:
        try:
            return len(u) if isinstance(u, str) else 0
        except Exception:
            return 0

    def _url_depth(u: str) -> int:
        try:
            p = urlparse(u).path
            return len([seg for seg in p.split("/") if seg])
        except Exception:
            return 0

    full_writer = None
    lean_writer = None
    total_rows = 0

    for i, chunk in enumerate(chunk_iter(), start=1):
        # Ensure dtypes are strings for uniform cleaning
        chunk = chunk.astype({c: str for c in chunk.columns})
        # Optional activity: some releases include it, others do not.
        if "activity" in chunk.columns:
            chunk["activity"] = (
                chunk["activity"].astype(str).str.strip().str.lower()
            )

        # timestamp + event_month
        chunk = add_timestamp_and_month(chunk, "date")

        # keys
        chunk["user_key"] = normalize_user_series(chunk["user"])
        chunk["user_raw"] = chunk["user"]

        # Select bare minimum clean columns we carry forward
        base_cols = ["timestamp","event_month","user_key","user_raw","pc","url","activity","id","date"]
        c = chunk[[c for c in base_cols if c in chunk.columns]].copy()

        # Join LDAP as-of and normalize / derive first_seen / last_seen via shared helper.
        # This keeps employment bounds consistent with LOGON / DEVICE / FILE.
        enr = left_join_ldap_by_month(c.copy(), ldap_asof)
        v3 = ensure_seen_bounds(enr, ldap_asof)

        # Flags (v3)
        v3["timestamp"] = pd.to_datetime(v3["timestamp"], errors="coerce")
        v3 = add_after_hours(v3, ts_col="timestamp")

        v3["pc"] = v3["pc"].astype(str)
        v3["on_shared_pc"] = v3["pc"].isin(shared_set) if shared_set else False
        if assigned_map:
            v3["assigned_pc"] = v3["user_key"].astype(str).map(assigned_map)
            v3["on_unassigned_pc"] = v3["assigned_pc"].notna() & v3["pc"].ne(v3["assigned_pc"])
        else:
            v3["assigned_pc"] = pd.NA
            v3["on_unassigned_pc"] = False

        # URL metrics (cheap)
        v3["url_length"] = v3["url"].map(_url_len)
        v3["url_depth"]  = v3["url"].map(_url_depth)

        # Host extraction + destination flags
        v3["_host"] = v3["url"].map(_host_of)
        v3["_reg_host"] = v3["_host"].map(_registrable)
        v3["is_wikileaks"] = v3["_reg_host"].isin(WIKI_HOSTS)
        v3["is_dropbox"] = v3["_reg_host"].isin(DROPBOX_HOSTS)
        v3["is_job_site"] = v3["_reg_host"].isin(JOBSITE_HOSTS) if JOBSITE_HOSTS else False

        # Employment status (one flag, mirrors DEVICE/FILE pipeline)
        v3 = add_active_employee_flag(
            v3,
            month_col="event_month",
            first_seen_col="first_seen",
            last_seen_col="last_seen",
            out_col="user_is_active_employee",
        )

        # ---- Final two tables (locked contract) ----
        # FULL keeps broad LDAP context incl. last_seen for auditing
        full_cols = [c for c in [
            # event
            "timestamp","event_month","user_key","user_raw","pc","url","activity","id","date",
            # url features
            "url_length","url_depth",
            # destination flags
            "is_wikileaks","is_dropbox","is_job_site",
            # org context (enriched)
            "email","role","is_admin","team","supervisor_key","last_seen",
            "employee_name","business_unit","functional_unit","department","supervisor",
            # risk flags / employment
            "after_hours","on_shared_pc","on_unassigned_pc","user_is_active_employee",
            # raw join traceability
            "joined_ldap",
        ] if c in v3.columns]
        http_full_chunk = v3[full_cols].copy()

        # LEAN: compact, model-ready (no names/departments/last_seen; keep clear flags)
        lean_cols = [c for c in [
            "timestamp","event_month","user_key","pc","url","activity",
            "url_length","url_depth",
            # destination flags
            "is_wikileaks","is_dropbox","is_job_site",
            "role","is_admin","team","supervisor_key",
            "after_hours","on_shared_pc","on_unassigned_pc",
            "user_is_active_employee",
        ] if c in v3.columns]
        http_lean_chunk = v3[lean_cols].copy()

        # Stream to parquet
        t_full = pa.Table.from_pandas(http_full_chunk, preserve_index=False)
        if full_writer is None:
            full_writer = pq.ParquetWriter(str(FULL_P), t_full.schema)
        full_writer.write_table(t_full)

        t_lean = pa.Table.from_pandas(http_lean_chunk, preserve_index=False)
        if lean_writer is None:
            lean_writer = pq.ParquetWriter(str(LEAN_P), t_lean.schema)
        lean_writer.write_table(t_lean)

        # cleanup temps
        for _c in ("_host","_reg_host"):
            if _c in v3.columns:
                del v3[_c]

        total_rows += len(v3)
        if i % 10 == 0:
            print(f"Chunk {i}: wrote {len(v3):,} rows (cum {total_rows:,})")

    # Close writers
    for w in (full_writer, lean_writer):
        if w is not None:
            w.close()

    print(f"[HTTP] wrote (final):\n  full -> {FULL_P}\n  lean -> {LEAN_P}")

    # Generate QC sidecars from the written artifacts (no re-write of parquet)
    try:
        emit_http_final(env, df_full=None, df_lean=None, family=family, overwrite=False, qc_mode="final_only")
    except Exception as e:
        print(f"[HTTP][qc] warn: failed to write QC sidecars: {e}")

def build_email(
    *,
    profile: str = "lean",
    family: str = "email_v3",
    ldap_family_for_join: str = "ldap_v3_lean",
    overwrite: bool = False,
    write_edges: bool = True
):
    """
    EMAIL pipeline (release-aware).
    Outputs:
      - email_v3_full/email_full.parquet
      - email_v3_lean/email_lean.parquet
    Adds two cross-domain flags aligned with other domains:
      - after_hours = hour<07 or hour>=19 (from timestamp)
      - user_is_active_employee = event_month within [first_seen, last_seen] from LDAP
    """
    env = bootstrap()

    # Load raw and add timestamp/event_month like other domains
    df = read_csv(env, "email")
    df = add_timestamp_and_month(df, "date")

    # user_key: prefer 'user' (r4.2/r5.1/r6.1). Fallback to left side of 'from'
    if "user" in df.columns:
        df["user_key"] = normalize_user_series(df["user"])
        df["user_raw"] = df["user"]
    else:
        handle = df.get("from", pd.Series(index=df.index, dtype=object)).astype(str).str.extract(r"^([^@]+)", expand=False).fillna("")
        df["user_key"] = normalize_user_series(handle)
        df["user_raw"] = df.get("user", pd.Series(index=df.index, dtype=object))

    # Keep from_key for edges; normalize
    df["from_key"] = df.get("from", pd.Series(index=df.index, dtype=object)).astype(str).str.strip().str.lower()

    # Normalize activity if present
    if "activity" in df.columns:
        df["activity"] = df["activity"].astype(str).str.strip().str.lower()

    # Deduplicate
    if "id" in df.columns and df["id"].notna().any():
        df = df.drop_duplicates("id", keep="first")
    else:
        df = df.drop_duplicates(["timestamp","user_key","from_key","to","cc","bcc"], keep="first")

    # Attachment/recipient counts, robust to missing columns
    if "attachments" in df.columns:
        df["attachment_count"] = pd.to_numeric(df["attachments"], errors="coerce").fillna(0).astype(int)
    else:
        df["attachments"] = pd.NA
        df["attachment_count"] = 0

    def _split_count(col):
        if col not in df.columns:
            return 0
        v = df[col].astype(str).where(df[col].notna(), "")
        return v.str.replace(r"\s+", " ", regex=True).str.strip().str.split(r"[;,]", regex=True).apply(
            lambda xs: 0 if xs is None else len([x for x in xs if x and x.lower() != "nan"])
        )

    df["to_count"] = _split_count("to")
    df["cc_count"] = _split_count("cc")
    df["bcc_count"] = _split_count("bcc")

    # Direction flags default to False if absent
    for c in [
        "dir_internal_only","dir_internal_to_external","dir_external_to_internal","dir_external_only",
        "any_personal_recipient","email_to_ext_domain","internal_to_external","external_to_internal","external_to_personal"
    ]:
        if c not in df.columns:
            df[c] = False

    ldap_asof = load_ldap_asof(env, ldap_family_for_join)

    email_clean = df[[c for c in df.columns if c in (
        "timestamp","event_month","id","user_key","user_raw","pc","from_key","from","to","cc","bcc",
        "size","attachments","attachment_count","activity",
        "to_count","cc_count","bcc_count",
        "dir_internal_only","dir_internal_to_external","dir_external_to_internal","dir_external_only",
        "any_personal_recipient","email_to_ext_domain","internal_to_external","external_to_internal","external_to_personal",
        "date"
    )]].copy()

    en = left_join_ldap_by_month(email_clean, ldap_asof)

    # Standardize first_seen / last_seen and employment bounds via shared helper.
    # Keeps EMAIL aligned with LOGON / DEVICE / FILE / HTTP.
    e2 = ensure_seen_bounds(en, ldap_asof)

    e2["timestamp"] = pd.to_datetime(e2["timestamp"], errors="coerce")
    e2 = add_after_hours(e2, ts_col="timestamp")
    e2 = add_active_employee_flag(
        e2,
        month_col="event_month",
        first_seen_col="first_seen",
        last_seen_col="last_seen",
        out_col="user_is_active_employee",
    )

    # Preserve existing shapes, append the two new flags
    full_keep = [
        "timestamp","event_month","from_key","id","user_key","pc",
        "from","to","cc","bcc","size","attachments","content","activity",
        "attachment_count","to_count","cc_count","bcc_count",
        "dir_internal_only","dir_internal_to_external","dir_external_to_internal","dir_external_only",
        "any_personal_recipient","email_to_ext_domain","internal_to_external","external_to_internal","external_to_personal",
        "after_hours","user_is_active_employee","date"
    ]
    lean_keep = [
        "id","timestamp","event_month","user_key","from_key","size",
        "attachment_count","attachments","to_count","cc_count","bcc_count",
        "dir_internal_only","dir_internal_to_external","dir_external_to_internal","dir_external_only",
        "any_personal_recipient","email_to_ext_domain","internal_to_external","external_to_internal","external_to_personal",
        "after_hours","user_is_active_employee"
    ]

    df_full = e2[[c for c in full_keep if c in e2.columns]].copy()
    df_lean = e2[[c for c in lean_keep if c in e2.columns]].copy()
    written = emit_email_final(
        env,
        df_full=df_full,
        df_lean=df_lean,
        df_edges=None,
        family=family,
        overwrite=overwrite,
    )

    print("[EMAIL] wrote:")
    for k, p in written.items():
        print("  ", k, "->", p)


# -----------------
# CLI
# -----------------

def main():
    ap = argparse.ArgumentParser(prog="etl", description="CERT ETL CLI")
    sub = ap.add_subparsers(dest="cmd", required=True)

    lp = sub.add_parser("ldap", help="Build LDAP artifacts")
    lp.add_argument("--profile", default="lean", choices=["lean","full"]) 
    lp.add_argument("--family",  default="ldap_v3")
    lp.add_argument("--overwrite", action="store_true")

    lg = sub.add_parser("logon", help="Build LOGON artifacts")
    lg.add_argument("--profile", default="lean", choices=["lean","full"]) 
    lg.add_argument("--family",  default="logon_v3")
    lg.add_argument("--ldap-family-for-join", default="ldap_v3_lean")
    lg.add_argument("--overwrite", action="store_true")

    dv = sub.add_parser("device", help="Build DEVICE artifacts")
    dv.add_argument("--profile", default="lean", choices=["lean","full"]) 
    dv.add_argument("--family",  default="device_v3")
    dv.add_argument("--ldap-family-for-join", default="ldap_v3_lean")
    dv.add_argument("--logon-family-for-pc", default="logon_v3_lean")
    dv.add_argument("--overwrite", action="store_true")

    hp = sub.add_parser("http", help="Build HTTP artifacts")
    hp.add_argument("--profile", default="lean", choices=["lean","full"]) 
    hp.add_argument("--family",  default="http_v3")
    hp.add_argument("--ldap-family-for-join", default="ldap_v3_lean")
    hp.add_argument("--logon-family-for-pc", default="logon_v3_lean")
    hp.add_argument("--overwrite", action="store_true")
    hp.add_argument("--no-reemit", action="store_true")

    both = sub.add_parser("all", help="Build LDAP then LOGON")
    both.add_argument("--ldap-profile", default="lean", choices=["lean","full"]) 
    both.add_argument("--ldap-family",  default="ldap_v3_lean")
    both.add_argument("--logon-profile", default="lean", choices=["lean","full"]) 
    both.add_argument("--logon-family",  default="logon_v3_lean")
    both.add_argument("--ldap-family-for-join", default="ldap_v3_lean")
    both.add_argument("--overwrite", action="store_true")

    em = sub.add_parser("email", help="Build EMAIL artifacts (email_full / email_lean)")
    em.add_argument("--profile", default="lean", choices=["lean","full"]) 
    em.add_argument("--family",  default="email_v3")
    em.add_argument("--ldap-family-for-join", default="ldap_v3_full")
    em.add_argument("--overwrite", action="store_true")
    em.add_argument("--no-reemit", action="store_true")

    fl = sub.add_parser("file", help="Build FILE artifacts (clean/enriched/enriched_v3)")
    fl.add_argument("--profile", choices=["lean","full"], default="lean")
    fl.add_argument("--family", default="file_v3")
    fl.add_argument("--ldap-family-for-join", default="ldap_v3_lean")
    fl.add_argument("--logon-family-for-pc", default="logon_v3_lean")
    fl.add_argument("--overwrite", action="store_true")
    fl.add_argument("--no-reemit", action="store_true")

    args = ap.parse_args()

    if args.cmd == "ldap":
        build_ldap(profile=args.profile, family=args.family, overwrite=args.overwrite)
    elif args.cmd == "logon":
        build_logon(profile=args.profile, family=args.family,
                    ldap_family_for_join=args.ldap_family_for_join,
                    overwrite=args.overwrite)
    elif args.cmd == "device":
        build_device(profile=args.profile, family=args.family,
                     ldap_family_for_join=args.ldap_family_for_join,
                     logon_family_for_pc=args.logon_family_for_pc,
                     overwrite=args.overwrite)
    elif args.cmd == "http":
        build_http(profile=args.profile, family=args.family,
                   ldap_family_for_join=args.ldap_family_for_join,
                   logon_family_for_pc=args.logon_family_for_pc,
                   overwrite=args.overwrite,
                   reemit=not args.no_reemit)
    elif args.cmd == "email":
        build_email(profile=args.profile, family=args.family,
                    ldap_family_for_join=args.ldap_family_for_join,
                    overwrite=args.overwrite)
    elif args.cmd == "file":
        build_file(profile=args.profile, family=args.family,
                   ldap_family_for_join=args.ldap_family_for_join,
                   logon_family_for_pc=args.logon_family_for_pc,
                   overwrite=args.overwrite,
                   reemit=not args.no_reemit)
    else:  # all
        build_ldap(profile=args.ldap_profile, family=args.ldap_family, overwrite=args.overwrite)
        build_logon(profile=args.logon_profile, family=args.logon_family,
                    ldap_family_for_join=args.ldap_family_for_join,
                    overwrite=args.overwrite)

if __name__ == "__main__":
    main()