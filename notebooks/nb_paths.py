# notebooks/nb_paths.py
"""
nb_paths.py
-----------
Minimal, read-only helpers:
- bootstrap(): resolve repo root, RELEASE, RAW and OUT
- csv_path(): canonical CSV location for a domain
- read_csv()/iter_csv_chunks(): resilient readers with header sniffing + alias mapping

Note: This module **does not** write outputs or sidecars. All writing belongs in src/helpers/io.py or emit.py.
"""
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
import pandas as pd
from typing import Optional

# =========================
# Project + env bootstrap
# =========================
@dataclass
class Env:
    PROJECT: Path
    RAW: Path
    OUT: Path
    RELEASE: str

def _project_root() -> Path:
    nb_dir = Path.cwd()
    return nb_dir.parent if nb_dir.name == "notebooks" else nb_dir

def _read_release_from_file(project: Path) -> Optional[str]:
    cfg = project / "release.txt"
    if cfg.exists():
        return cfg.read_text().strip()
    return None

def bootstrap(release: Optional[str] = None) -> Env:
    project = _project_root()
    if release is None:
        release = _read_release_from_file(project) or "r1"
    release = str(release).strip()
    data = project / "data"
    raw = data / release
    if not raw.exists():
        raise FileNotFoundError(
            f"Data folder not found for release '{release}'. "
            f"Expected at: {raw}\n"
            f"Fix: set the desired release in {project/'release.txt'} "
            f"(e.g., 'r3.1') or pass bootstrap('r3.1')."
        )
    out = project / "out"
    out.mkdir(exist_ok=True)
    return Env(PROJECT=project, RAW=raw, OUT=out, RELEASE=release)

def csv_path(env: Env, name: str) -> Path:
    return env.RAW / f"{name}.csv"



# ========= Public CSV readers =========

def read_csv(env: Env, name: str, **kwargs) -> pd.DataFrame:
    """
    Simple, explicit CSV reader.
    - No schema normalization, no header guessing.
    - Defaults are friendly for CERT (string dtypes, utf-8, warn on bad lines).
    - Use viewer notebooks to audit headers and values before ETL.
    """
    p = csv_path(env, name)
    if not p.exists():
        raise FileNotFoundError(f"Missing {p}. Is RELEASE='{env.RELEASE}' correct and data present?")

    # Sensible defaults; caller can override any of these via kwargs.
    kwargs.setdefault("dtype", str)
    kwargs.setdefault("encoding", "utf-8")
    kwargs.setdefault("on_bad_lines", "warn")
    return pd.read_csv(p, **kwargs)


def iter_csv_chunks(env: Env, name: str, chunksize: int = 500_000, **kwargs):
    """
    Chunked CSV reader with the same simple defaults.
    - No schema normalization, no header guessing.
    - Set header/names explicitly in callers when needed.
    """
    p = csv_path(env, name)
    if not p.exists():
        raise FileNotFoundError(f"Missing {p}. Is RELEASE='{env.RELEASE}' correct and data present?")

    kwargs.setdefault("dtype", str)
    kwargs.setdefault("encoding", "utf-8")
    kwargs.setdefault("on_bad_lines", "warn")
    kwargs.setdefault("chunksize", chunksize)

    for chunk in pd.read_csv(p, **kwargs):
        yield chunk


# Convenience thin wrappers matching your prior API (kept for compatibility)
def read_csv_raw(env: Env, name: str, **kwargs) -> pd.DataFrame:
    """Direct pass-through to pandas (no schema normalization)."""
    p = csv_path(env, name)
    if not p.exists():
        raise FileNotFoundError(f"Missing {p}. Is RELEASE='{env.RELEASE}' correct and data present?")
    return pd.read_csv(p, **kwargs)

# Public API: Env, bootstrap, csv_path, read_csv, iter_csv_chunks, read_csv_raw