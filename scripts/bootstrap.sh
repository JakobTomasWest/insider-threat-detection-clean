#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

echo "[bootstrap] repo: $ROOT"

# 1) Python env (pin to Python 3.11 by default)
PYTHON_BIN="${PYTHON_BIN:-python3.11}"

# Detect a broken/moved interpreter inside .venv (common after OS or Python updates).
if [[ -d .venv ]]; then
  if ! .venv/bin/python3 -V >/dev/null 2>&1; then
    echo "[bootstrap] detected broken .venv (python path moved). Recreating…"
    rm -rf .venv
  else
    # Check major.minor of existing venv vs desired interpreter
    VENV_VER="$(
      .venv/bin/python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))'
    )"
    if command -v "$PYTHON_BIN" >/dev/null 2>&1; then
      EXPECT_VER="$(
        "$PYTHON_BIN" -c 'import sys; print(".".join(map(str, sys.version_info[:2])))'
      )"
    else
      EXPECT_VER=""
    fi

    if [[ -n "$EXPECT_VER" && "$VENV_VER" != "$EXPECT_VER" ]]; then
      echo "[bootstrap] .venv Python is $VENV_VER but expected $EXPECT_VER; recreating with $PYTHON_BIN"
      rm -rf .venv
    fi
  fi
fi

if [[ ! -d .venv ]]; then
  echo "[bootstrap] creating .venv with $PYTHON_BIN"
  if ! command -v "$PYTHON_BIN" >/dev/null 2>&1; then
    echo "[bootstrap] ERROR: $PYTHON_BIN not found on PATH." >&2
    echo "[bootstrap] Install it (e.g. 'brew install python@3.11') or set PYTHON_BIN to a valid interpreter."
    exit 1
  fi
  "$PYTHON_BIN" -m venv .venv
fi

# shellcheck disable=SC1091
source .venv/bin/activate
python3 -V || { echo "[bootstrap] ERROR: failed to run venv python"; exit 1; }
pip install --upgrade pip >/dev/null
pip install -r requirements.txt

# 2) Make helper scripts executable
chmod +x scripts/dev_cheats.sh scripts/qc_checks.sh scripts/rebuild.sh scripts/daily_check.py || true

# 3) DuckDB CLI (used by qc_checks.sh)
if ! command -v duckdb >/dev/null 2>&1; then
  echo "[bootstrap] DuckDB CLI not found."
  if [[ "$(uname -s)" == "Darwin" ]] && command -v brew >/dev/null 2>&1; then
    echo "[bootstrap] installing duckdb via Homebrew"
    brew install duckdb
  else
    echo "[bootstrap] please install DuckDB CLI from https://duckdb.org/#quickstart"
    echo "[bootstrap] continuing (qc will be limited)…"
  fi
fi

# 4) release.txt hygiene — do NOT force a specific release.
if [[ ! -f release.txt ]]; then
  # Try to infer if there’s exactly one data/r*/ present
  mapfile -t CAND < <(find data -maxdepth 1 -mindepth 1 -type d -name 'r*' -printf '%f\n' 2>/dev/null | sort)
  if [[ "${#CAND[@]}" -eq 1 ]]; then
    echo "${CAND[0]}" > release.txt
    echo "[bootstrap] wrote release.txt -> ${CAND[0]}"
  else
    echo "[bootstrap] No release.txt found."
    echo "  Create one with a tag you actually have in data/:"
    echo "    echo r5.1 > release.txt     # example"
  fi
else
  echo "[bootstrap] using existing release.txt -> $(cat release.txt)"
fi

# 5) Gentle sanity: check CSVs exist for chosen release (if set)
if [[ -f release.txt ]]; then
  REL="$(cat release.txt)"
  if ! ls "data/$REL/"*.csv >/dev/null 2>&1; then
    echo "[bootstrap] WARNING: data/$REL/*.csv not found."
    echo "  Either update release.txt to match your local data folder"
    echo "  or symlink your CSVs into data/<release>/."
  else
    echo "[bootstrap] found CSVs under data/$REL/"
  fi
fi

echo
echo "[bootstrap] done."
echo "Next steps:"
echo "  make build    # ldap → logon → device → file → http → email"
echo "  make qc       # quick sanity checks"
echo "Optional:"
echo "  make daily    # build daily aggregates + qc"