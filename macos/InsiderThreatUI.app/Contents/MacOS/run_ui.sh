#!/usr/bin/env bash
set -euo pipefail

# Log file for Finder launches
LOG="$HOME/insider_ui_launcher.log"

{
  echo
  echo "=== Launcher started at $(date) ==="
  echo "Original PWD: $(pwd)"
  echo "Script path:  $0"

  # ------------------------------------------------------
  # Locate repo root
  # ------------------------------------------------------
  CANDIDATES=(
    "$HOME/Desktop/code/insider-threat-capstone"
    "$HOME/code/insider-threat-capstone"
    "$(cd "$(dirname "$0")/../../.." && pwd)"
  )

  REPO_ROOT=""
  for cand in "${CANDIDATES[@]}"; do
    if [ -d "$cand" ] && [ -f "$cand/Makefile" ] && [ -f "$cand/src/ui/app.py" ]; then
      REPO_ROOT="$cand"
      break
    fi
  done

  if [ -z "$REPO_ROOT" ]; then
    echo "❌ Could not locate repo root."
    exit 1
  fi

  echo "Repo root: $REPO_ROOT"
  cd "$REPO_ROOT"

  # ------------------------------------------------------
  # Activate virtualenv (PATH‑based)
  # ------------------------------------------------------
  VENV_DIR="$REPO_ROOT/.venv"
  VENV_BIN="$VENV_DIR/bin"

  if [[ -x "$VENV_BIN/python" ]]; then
    export VIRTUAL_ENV="$VENV_DIR"
    export PATH="$VENV_BIN:$PATH"
    echo "Using virtualenv: $VENV_DIR"
  else
    echo "❌ .venv missing. Run: python3 -m venv .venv && source .venv/bin/activate && make setup"
    exit 1
  fi

  echo "python: $(which python)"
  echo "make:   $(which make)"

  # ------------------------------------------------------
  # Kill old UI server on port 8000
  # ------------------------------------------------------
  if command -v lsof >/dev/null; then
    PIDS="$(lsof -ti tcp:8000 || true)"
    if [[ -n "$PIDS" ]]; then
      echo "🔪 Killing old UI server(s): $PIDS"
      kill $PIDS || true
      sleep 2
    fi
  fi

  # ------------------------------------------------------
  # Start UI server
  # ------------------------------------------------------
  echo "Starting make ui..."
  make ui > /tmp/insider_ui.log 2>&1 &

  sleep 2

  echo "Opening browser..."
  open "http://127.0.0.1:8000"

  echo "=== Launcher finished at $(date) ==="
} >> "$LOG" 2>&1
