# src/helpers/io.py
from __future__ import annotations
from pathlib import Path
import pandas as pd

def out_dir(env, family: str) -> Path:
    """Return output directory for a given family, e.g. out/r5.1/ldap_v2."""
    d = env.OUT / env.RELEASE / family
    d.mkdir(parents=True, exist_ok=True)
    return d

def out_path(env, family: str, base: str) -> Path:
    """Return full path for a given artifact, without writing anything."""
    return out_dir(env, family) / f"{base}.parquet"