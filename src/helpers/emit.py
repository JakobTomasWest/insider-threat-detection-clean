"""
Shared emission + profiling helpers for CERT ETL artifacts.

Switch profiles/families via env vars without editing notebooks:
  LDAP_PROFILE=lean|full
  LDAP_FAMILY=ldap_v3|...

Usage in notebook:
    from src.helpers.emit import emit_ldap
    emit_ldap(env, ldap_all, ldap_asof, profile="lean", family=None, overwrite=False)

- LOGON, DEVICE, FILE, **HTTP** domains emit only two final artifacts via their respective two-artifact contract emitters.
- Legacy multi-table emitters (clean/enriched/enriched_v3) were removed for LOGON, DEVICE, and HTTP.
"""
from __future__ import annotations

import json
import os

# --- helper: robust row counter from parquet footer (no full scan) ---
def _parquet_rows(p: Path) -> int:
    """
    Return the number of rows recorded in the parquet file footer.
    Tries DuckDB first (fast), then falls back to PyArrow.
    Returns 0 on failure.
    """
    try:
        import duckdb  # type: ignore
        con = duckdb.connect(database=":memory:")
        r = con.execute(
            f"SELECT COALESCE(file_row_number, 0) AS n FROM parquet_metadata('{str(p)}') LIMIT 1;"
        ).fetchone()
        if r and r[0] is not None:
            return int(r[0])
    except Exception:
        pass
    try:
        import pyarrow.parquet as pq  # type: ignore
        return int(pq.ParquetFile(str(p)).metadata.num_rows)
    except Exception:
        return 0

def _parquet_columns(p: Path) -> List[str]:
    """
    Return the column names from the parquet file schema.
    Returns empty list on failure.
    """
    try:
        import pyarrow.parquet as pq  # type: ignore
        return list(pq.ParquetFile(str(p)).schema.names)
    except Exception:
        return []

#
# ---- centralized parquet write settings ----
def _pq_kwargs():
    # defaults tuned for smaller temp + good compression
    rg = int(os.getenv("CERT_PARQUET_RG", "256000"))
    comp = os.getenv("CERT_PARQUET_COMP", "zstd")
    try:
        lvl = int(os.getenv("CERT_PARQUET_LEVEL", "3"))
    except Exception:
        lvl = 3
    return dict(engine="pyarrow", index=False, compression=comp, compression_level=lvl, row_group_size=rg)
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterable, List, Mapping, MutableMapping, Optional

import pandas as pd

from src.helpers.io import out_dir as _out_dir
from src.helpers.io import out_path as _out_path


# -----------------------------
# Config dataclasses
# -----------------------------
@dataclass(frozen=True)
class ProfileSpec:
    # e.g., {"snapshots": [...], "asof": [...]}
    tables: Mapping[str, List[str]]

@dataclass(frozen=True)
class DerivedRule:
    kind: str          # e.g., "is_admin_from_role"
    src: Optional[str] = None
    dst: Optional[str] = None

@dataclass
class DomainSchema:
    family_default: str
    profiles: Mapping[str, ProfileSpec]
    derived: List[DerivedRule]


# -----------------------------
# Built-in LDAP schema
# -----------------------------
def _ldap_schema_builtin() -> DomainSchema:
    return DomainSchema(
        family_default="ldap_v3",
        profiles={
            "full": ProfileSpec(
                tables={
                    "snapshots": [
                        "user_key", "snapshot_date", "employee_name", "email", "role",
                        "business_unit", "functional_unit", "department", "team", "supervisor",
                    ],
                    "asof": [
                        "user_key", "event_month", "employee_name", "email", "role",
                        "business_unit", "functional_unit", "department", "team", "supervisor",
                        "supervisor_key", "last_seen",
                    ],
                }
            ),
            "lean": ProfileSpec(
                tables={
                    "snapshots": ["user_key", "snapshot_date", "email", "role", "is_admin", "team", "supervisor"],
                    "asof":      ["user_key", "event_month", "email", "role", "is_admin", "team", "supervisor_key", "first_seen", "last_seen"],
                }
            ),
        },
        derived=[DerivedRule(kind="is_admin_from_role", src="role", dst="is_admin")],
    )


# -----------------------------
# Derived application
# -----------------------------
def _apply_derived(df: pd.DataFrame, rules: Iterable[DerivedRule]) -> pd.DataFrame:
    if not rules:
        return df
    out = df.copy()
    for r in rules:
        if r.kind == "is_admin_from_role":
            src = r.src or "role"
            dst = r.dst or "is_admin"
            if src in out.columns:
                out[dst] = out[src].astype(str).str.lower().eq("itadmin")
    return out


# -----------------------------
# Generic emitter
# -----------------------------
def emit_generic(
    *,
    env,
    domain: str,
    frames: Mapping[str, pd.DataFrame],  # {"snapshots": df1, "asof": df2}
    schema: Optional[DomainSchema] = None,
    profile: Optional[str] = None,
    family: Optional[str] = None,
    overwrite: bool = False,
    file_names: Optional[Mapping[str, str]] = None,  # {"snapshots": "ldap_snapshots", ...}
) -> Mapping[str, Path]:
    schema = schema or (_ldap_schema_builtin() if domain == "ldap" else None)
    if schema is None:
        raise ValueError(f"No schema provided and no builtin available for domain='{domain}'")

    profile = profile or os.getenv(f"{domain.upper()}_PROFILE") or "lean"
    if profile not in schema.profiles:
        raise KeyError(f"Unknown profile '{profile}' for domain '{domain}'. Available: {list(schema.profiles)}")

    family = family or os.getenv(f"{domain.upper()}_FAMILY") or schema.family_default

    out_dir = _out_dir(env, family)
    derived = list(schema.derived) if schema.derived else []
    trimmed_frames: Dict[str, pd.DataFrame] = {}

    for table_name, df in frames.items():
        df2 = _apply_derived(df, derived)
        keep_cols = schema.profiles[profile].tables.get(table_name)
        if keep_cols is None:
            trimmed = df2
        else:
            present = [c for c in keep_cols if c in df2.columns]
            trimmed = df2[present]
        trimmed_frames[table_name] = trimmed

    default_names = {k: k for k in frames.keys()}
    file_names = {**default_names, **(file_names or {})}

    written: Dict[str, Path] = {}
    metas: Dict[str, MutableMapping[str, object]] = {}

    for table_name, df_out in trimmed_frames.items():
        base = file_names[table_name]
        path = _out_path(env, family, base)
        if path.exists() and not overwrite:
            print(f"Exists (skip): {path}")
        else:
            df_out.to_parquet(path, **_pq_kwargs())
            print(f"Wrote {path}")
        written[table_name] = path
        metas[table_name] = {
            "rows": int(len(df_out)),
            "cols": list(map(str, df_out.columns)),
            "artifact": Path(path).name,
        }

    sidecar = {
        "domain": domain,
        "family": family,
        "profile": profile,
        "tables": metas,
        "run_tag": f"{domain}_{profile}",
    }
    (out_dir / f"{domain}_{profile}_meta.json").write_text(json.dumps(sidecar, indent=2))

    return written


# -----------------------------
# LDAP wrapper
# -----------------------------
def emit_ldap(
    env,
    df_snapshots: pd.DataFrame,
    df_asof: pd.DataFrame,
    *,
    profile: Optional[str] = None,
    family: Optional[str] = None,
    overwrite: bool = False,
) -> Mapping[str, Path]:
    frames = {"snapshots": df_snapshots, "asof": df_asof}
    return emit_generic(
        env=env,
        domain="ldap",
        frames=frames,
        schema=_ldap_schema_builtin(),
        profile=profile,
        family=family,
        overwrite=overwrite,
        file_names={"snapshots": "ldap_snapshots", "asof": "ldap_asof_by_month"},
    )


#
# LOGON final emitter (two-artifact contract)
#
def emit_logon_final(
    env,
    *,
    df_full: pd.DataFrame,
    df_lean: pd.DataFrame,
    family: Optional[str] = None,
    overwrite: bool = False,
) -> Mapping[str, Path]:
    """
    Emit exactly TWO LOGON artifacts for this domain (final contract):
      - logon_full.parquet  (rich, audit-friendly)
      - logon_lean.parquet  (compact, model-ready)

    Helper artifacts:
      - shared_pcs.parquet   (pc, distinct_non_admin_users, top_user_share, shared_pc)
      - assigned_pc.parquet  (user_key, assigned_pc, days_on_pc, user_days, share)
    These helpers are written by the LOGON ETL and consumed by other domains; they are not additional "tables" for this domain.

    The LEAN artifact includes LDAP context columns (`role`, `is_admin`, `team`, `supervisor_key`) and PC flags (`after_hours_login`, `on_shared_pc`, `on_unassigned_pc`, `user_is_active_employee`) when present.
    """
    fam = family or "logon_v3"
    # 1) Compute base_dir = _out_dir(env, fam)
    base_dir = _out_dir(env, fam)
    # 2) Create subdirs for full/lean
    full_dir = base_dir / "logon_v3_full"
    lean_dir = base_dir / "logon_v3_lean"
    full_dir.mkdir(parents=True, exist_ok=True)
    lean_dir.mkdir(parents=True, exist_ok=True)
    # 3) Set artifact paths
    full_p = full_dir / "logon_full.parquet"
    lean_p = lean_dir / "logon_lean.parquet"
    # 4) Respect overwrite
    if full_p.exists() and not overwrite:
        print(f"Exists (skip): {full_p}")
    else:
        df_full.to_parquet(full_p, **_pq_kwargs())
        print(f"Wrote {full_p}")
    if lean_p.exists() and not overwrite:
        print(f"Exists (skip): {lean_p}")
    else:
        df_lean.to_parquet(lean_p, **_pq_kwargs())
        print(f"Wrote {lean_p}")
    # 5) Build meta payload
    meta = {
        "domain": "logon",
        "family": fam,
        "subdirs": ["logon_v3_full", "logon_v3_lean"],
        "artifacts": {
            "logon_full": {
                "rows": int(len(df_full)),
                "cols": list(map(str, df_full.columns)),
                "path": str(full_p.relative_to(base_dir)),
            },
            "logon_lean": {
                "rows": int(len(df_lean)),
                "cols": list(map(str, df_lean.columns)),
                "path": str(lean_p.relative_to(base_dir)),
            },
        },
    }
    # 6) Write meta JSON to qc_dir
    qc_dir = Path(env.OUT) / env.RELEASE / "qc"
    qc_dir.mkdir(parents=True, exist_ok=True)
    meta_path = qc_dir / "logon_final_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    return {"full": full_p, "lean": lean_p}

# -----------------------------
# DEVICE final emitter (two-artifact contract)
# -----------------------------
def emit_device_final(
    env,
    *,
    df_full: pd.DataFrame,
    df_lean: pd.DataFrame,
    family: Optional[str] = None,
    overwrite: bool = False,
) -> Mapping[str, Path]:
    """
    Emit exactly TWO DEVICE artifacts for this domain (final contract):
      - device_full.parquet  (rich, audit-friendly)
      - device_lean.parquet  (compact, model-ready)
    """
    fam = family or "device_v3"
    base_dir = _out_dir(env, fam)
    full_dir = base_dir / "device_v3_full"
    lean_dir = base_dir / "device_v3_lean"
    full_dir.mkdir(parents=True, exist_ok=True)
    lean_dir.mkdir(parents=True, exist_ok=True)
    full_p = full_dir / "device_full.parquet"
    lean_p = lean_dir / "device_lean.parquet"
    if full_p.exists() and not overwrite:
        print(f"Exists (skip): {full_p}")
    else:
        df_full.to_parquet(full_p, **_pq_kwargs())
        print(f"Wrote {full_p}")
    if lean_p.exists() and not overwrite:
        print(f"Exists (skip): {lean_p}")
    else:
        df_lean.to_parquet(lean_p, **_pq_kwargs())
        print(f"Wrote {lean_p}")
    meta = {
        "domain": "device",
        "family": fam,
        "subdirs": ["device_v3_full", "device_v3_lean"],
        "artifacts": {
            "device_full": {
                "rows": int(len(df_full)),
                "cols": list(map(str, df_full.columns)),
                "path": str(full_p.relative_to(base_dir)),
            },
            "device_lean": {
                "rows": int(len(df_lean)),
                "cols": list(map(str, df_lean.columns)),
                "path": str(lean_p.relative_to(base_dir)),
            },
        },
    }
    qc_dir = Path(env.OUT) / env.RELEASE / "qc"
    qc_dir.mkdir(parents=True, exist_ok=True)
    meta_path = qc_dir / "device_final_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    return {"full": full_p, "lean": lean_p}


# -----------------------------
# FILE final emitter (two-artifact contract)
# -----------------------------
def emit_file_final(
    env,
    *,
    df_full: pd.DataFrame,
    df_lean: pd.DataFrame,
    family: Optional[str] = None,
    overwrite: bool = False,
) -> Mapping[str, Path]:
    """
    Emit exactly TWO FILE artifacts for this domain (final contract):
      - file_full.parquet  (rich, audit-friendly)
      - file_lean.parquet  (compact, model-ready)
    """
    fam = family or "file_v3"
    base_dir = _out_dir(env, fam)
    full_dir = base_dir / "file_v3_full"
    lean_dir = base_dir / "file_v3_lean"
    full_dir.mkdir(parents=True, exist_ok=True)
    lean_dir.mkdir(parents=True, exist_ok=True)
    full_p = full_dir / "file_full.parquet"
    lean_p = lean_dir / "file_lean.parquet"
    if full_p.exists() and not overwrite:
        print(f"Exists (skip): {full_p}")
    else:
        df_full.to_parquet(full_p, **_pq_kwargs())
        print(f"Wrote {full_p}")
    if lean_p.exists() and not overwrite:
        print(f"Exists (skip): {lean_p}")
    else:
        df_lean.to_parquet(lean_p, **_pq_kwargs())
        print(f"Wrote {lean_p}")
    meta = {
        "domain": "file",
        "family": fam,
        "subdirs": ["file_v3_full", "file_v3_lean"],
        "artifacts": {
            "file_full": {
                "rows": int(len(df_full)),
                "cols": list(map(str, df_full.columns)),
                "path": str(full_p.relative_to(base_dir)),
            },
            "file_lean": {
                "rows": int(len(df_lean)),
                "cols": list(map(str, df_lean.columns)),
                "path": str(lean_p.relative_to(base_dir)),
            },
        },
    }
    qc_dir = Path(env.OUT) / env.RELEASE / "qc"
    qc_dir.mkdir(parents=True, exist_ok=True)
    meta_path = qc_dir / "file_final_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    return {"full": full_p, "lean": lean_p}

def emit_http_final(
    env,
    *,
    df_full: pd.DataFrame,
    df_lean: pd.DataFrame,
    family: Optional[str] = None,
    overwrite: bool = False,
    qc_mode: Optional[str] = None,  # accepted for backward compatibility
) -> Mapping[str, Path]:
    """
    Emit exactly TWO HTTP artifacts:
      - http_full.parquet
      - http_lean.parquet

    Also writes out/<REL>/qc/http_final_meta.json so qc can summarize without scanning.
    """
    fam = family or "http_v3"
    base_dir = _out_dir(env, fam)
    full_dir = base_dir / "http_v3_full"
    lean_dir = base_dir / "http_v3_lean"
    full_dir.mkdir(parents=True, exist_ok=True)
    lean_dir.mkdir(parents=True, exist_ok=True)

    full_p = full_dir / "http_full.parquet"
    lean_p = lean_dir / "http_lean.parquet"

    # Handle case where dataframes are None (qc_mode="final_only")
    if df_full is not None:
        if full_p.exists() and not overwrite:
            print(f"Exists (skip): {full_p}")
        else:
            df_full.to_parquet(full_p, **_pq_kwargs())
            print(f"Wrote {full_p}")

    if df_lean is not None:
        if lean_p.exists() and not overwrite:
            print(f"Exists (skip): {lean_p}")
        else:
            df_lean.to_parquet(lean_p, **_pq_kwargs())
            print(f"Wrote {lean_p}")

    # Pull metadata from parquet files if dataframes are None, otherwise use dataframes
    if df_full is not None and (not full_p.exists() or overwrite):
        rows_full = len(df_full)
        cols_full = list(map(str, df_full.columns))
    else:
        rows_full = _parquet_rows(full_p)
        cols_full = _parquet_columns(full_p)

    if df_lean is not None and (not lean_p.exists() or overwrite):
        rows_lean = len(df_lean)
        cols_lean = list(map(str, df_lean.columns))
    else:
        rows_lean = _parquet_rows(lean_p)
        cols_lean = _parquet_columns(lean_p)

    meta = {
        "domain": "http",
        "family": fam,
        "subdirs": ["http_v3_full", "http_v3_lean"],
        "artifacts": {
            "http_full": {
                "rows": int(rows_full),
                "cols": cols_full,
                "path": str(full_p.relative_to(base_dir)),
            },
            "http_lean": {
                "rows": int(rows_lean),
                "cols": cols_lean,
                "path": str(lean_p.relative_to(base_dir)),
            },
        },
    }
    qc_dir = Path(env.OUT) / env.RELEASE / "qc"
    qc_dir.mkdir(parents=True, exist_ok=True)
    meta_path = qc_dir / "http_final_meta.json"
    meta_path.write_text(json.dumps(meta, indent=2))
    print(f"Wrote {meta_path}")
    return {"http_full": full_p, "http_lean": lean_p}


# -----------------------------
# EMAIL final emitter (two-artifact contract)
# -----------------------------
def emit_email_final(
    env,
    *,
    df_full: Optional[pd.DataFrame] = None,
    df_lean: Optional[pd.DataFrame] = None,
    df_edges: Optional[pd.DataFrame] = None,
    family: Optional[str] = None,
    overwrite: bool = False,
) -> Mapping[str, Path]:
    """
    Emit exactly TWO EMAIL artifacts (final contract):
      - email_full.parquet  (rich, audit-friendly)
      - email_lean.parquet  (compact, model-ready)
    Optionally emit a third artifact at the FAMILY ROOT:
      - email_edges.parquet (sender→recipient edges; lean columns)

    Also writes a QC meta sidecar under out/<release>/qc/email_final_meta.json
    listing row counts and column names for full/lean/edges.
    """
    fam = family or "email_v3"
    base_dir = _out_dir(env, fam)
    full_dir = base_dir / f"{fam}_full"
    lean_dir = base_dir / f"{fam}_lean"
    full_dir.mkdir(parents=True, exist_ok=True)
    lean_dir.mkdir(parents=True, exist_ok=True)

    full_p = full_dir / "email_full.parquet"
    lean_p = lean_dir / "email_lean.parquet"
    edges_p = base_dir / "email_edges.parquet"

    # Write only if DataFrames are provided; otherwise assume files already exist
    if df_full is not None:
        if full_p.exists() and not overwrite:
            print(f"Exists (skip): {full_p}")
        else:
            df_full.to_parquet(full_p, **_pq_kwargs())
            print(f"Wrote {full_p}")
    else:
        if not full_p.exists():
            print(f"[email_final] warn: {full_p} does not exist and no df_full provided.")

    if df_lean is not None:
        if lean_p.exists() and not overwrite:
            print(f"Exists (skip): {lean_p}")
        else:
            df_lean.to_parquet(lean_p, **_pq_kwargs())
            print(f"Wrote {lean_p}")
    else:
        if not lean_p.exists():
            print(f"[email_final] warn: {lean_p} does not exist and no df_lean provided.")

    wrote_edges = False
    if df_edges is not None:
        if edges_p.exists() and not overwrite:
            print(f"Exists (skip): {edges_p}")
        else:
            df_edges.to_parquet(edges_p, **_pq_kwargs())
            print(f"Wrote {edges_p}")
        wrote_edges = True

    # QC meta sidecar
    qc_dir = Path(env.OUT) / env.RELEASE / "qc"
    qc_dir.mkdir(parents=True, exist_ok=True)
    # Helper to peek columns and row counts from existing parquet when DataFrames are None
    try:
        import pyarrow.parquet as _pq
    except Exception:
        _pq = None

    def _cols_from_parquet(path: Path) -> list[str]:
        try:
            if _pq is None or not path.exists():
                return []
            pf = _pq.ParquetFile(str(path))
            return list(pf.schema.names)
        except Exception:
            return []

    def _rows_from_parquet(path: Path) -> int:
        try:
            if _pq is None or not path.exists():
                return 0
            pf = _pq.ParquetFile(str(path))
            # metadata.num_rows is cheap and avoids full scan
            meta = getattr(pf, "metadata", None)
            return int(meta.num_rows) if meta is not None else 0
        except Exception:
            return 0
    meta = {
        "domain": "email",
        "family": fam,
        "subdirs": [f"{fam}_full", f"{fam}_lean"],
        "artifacts": {
            "email_full": {
                "rows": int(len(df_full)) if df_full is not None else _rows_from_parquet(full_p),
                "cols": list(map(str, getattr(df_full, "columns", []))) if df_full is not None else _cols_from_parquet(full_p),
                "path": str(full_p.relative_to(base_dir)),
            },
            "email_lean": {
                "rows": int(len(df_lean)) if df_lean is not None else _rows_from_parquet(lean_p),
                "cols": list(map(str, getattr(df_lean, "columns", []))) if df_lean is not None else _cols_from_parquet(lean_p),
                "path": str(lean_p.relative_to(base_dir)),
            },
        },
    }

    if wrote_edges or edges_p.exists():
        meta["artifacts"]["email_edges"] = {
            "rows": int(len(df_edges)) if df_edges is not None else _rows_from_parquet(edges_p),
            "cols": list(map(str, getattr(df_edges, "columns", []))) if df_edges is not None else _cols_from_parquet(edges_p),
            "path": str(edges_p.relative_to(base_dir)),
        }

    (qc_dir / "email_final_meta.json").write_text(json.dumps(meta, indent=2))
    return {"full": full_p, "lean": lean_p, **({"edges": edges_p} if wrote_edges else {})}

