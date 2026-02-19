#!/usr/bin/env bash
# scripts/dev_cheats.sh — developer shortcuts (bash)
set -o pipefail

# -------- basics --------
alias rel='cat release.txt'
alias here='pwd'
alias disk_fs='df -h .'
alias disk_here='du -sh .'
alias qc='./scripts/qc_checks.sh'
alias rb='./scripts/rebuild.sh'
alias build_daily='python3 scripts/build_daily.py'
alias daily_check_v1='python3 scripts/daily_check.py'   # legacy v1 checker
unalias daily_check 2>/dev/null || true   # ensure we can define a function named daily_check
# daily_check (no args) now validates features_v2/daily_user

# v2-aware daily checker: mirrors the v1 script’s output format
# Usage: daily_check
# Checks: out/<REL>/features_v2/daily_user/daily_user.parquet
daily_check() {
  local R; R="$(rel)" || return 1
  local p="out/$R/features_v2/daily_user/daily_user.parquet"
  if [[ ! -f "$p" ]]; then
    echo "[daily_check] missing v2 daily: $p" >&2
    echo "Hint: run: make daily-v1 && make daily-v2" >&2
    return 2
  fi
  python3 - <<'PY'
import duckdb, pathlib, sys
rel = pathlib.Path('release.txt').read_text().strip()
p = pathlib.Path('out')/rel/'features_v2'/'daily_user'/'daily_user.parquet'
con = duckdb.connect(database=':memory:')
con.execute(f"CREATE OR REPLACE TABLE t AS SELECT * FROM read_parquet('{p.as_posix()}')")
rows = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
users = con.execute("SELECT COUNT(DISTINCT user_key) FROM t").fetchone()[0]
ndist = con.execute("SELECT COUNT(*) FROM (SELECT DISTINCT user_key, day FROM t)").fetchone()[0]
dups = rows - ndist
dmin, dmax = con.execute("SELECT MIN(day), MAX(day) FROM t").fetchone()
cols = [r[1] for r in con.execute("PRAGMA table_info('t')").fetchall()]
print(f"== Daily checks (release {rel}) ==\n")
print("-- Summary --")
print("table                    rows      users     dups                          range   path")
rng = f"[{str(dmin)[:10]} .. {str(dmax)[:10]}]"
print(f"{'daily_user':<24}{rows:>10} {users:>10} {dups:>9}     {rng:<28}   {p}")
print("\n-- Findings --")
if dups == 0:
    print("No problems detected.")
else:
    print("Duplicate (user_key, day) rows present → investigate v2 builder.")
print("\n-- Columns --")
print("[daily_user] " + ", ".join(cols))
PY
}

# -------- venv on/off --------
venv_on() {
  local v="${1:-.venv}"
  if [[ -f "$v/bin/activate" ]]; then
    # shellcheck disable=SC1090
    source "$v/bin/activate"
    python3 -V 2>/dev/null || true
    which python3 2>/dev/null || true
    echo "[venv] activated: $v"
  else
    echo "[venv] missing: $v/bin/activate"
    return 1
  fi
}
venv_off() {
  type deactivate >/dev/null 2>&1 && deactivate || echo "[venv] none active"
}

# -------- duckdb one-liner --------
dq() { duckdb -c "$*"; }

# -------- resolver: token -> parquet path --------
_pq_for() {
  local tok="$1"; local R="$(rel)"
  if [[ "$tok" == *".parquet"* || "$tok" == */* ]]; then echo "$tok"; return 0; fi
  local prof="lean"
  [[ "$tok" == *":full" ]] && prof="full" && tok="${tok%:full}"
  [[ "$tok" == *":lean" ]] && prof="lean" && tok="${tok%:lean}"

  case "$tok" in
    logon)  [[ "$prof" == "full" ]] && echo "out/$R/logon_v3/logon_v3_full/logon_full.parquet" \
                                 || echo "out/$R/logon_v3/logon_v3_lean/logon_lean.parquet" ;;
    device) [[ "$prof" == "full" ]] && echo "out/$R/device_v3/device_v3_full/device_full.parquet" \
                                 || echo "out/$R/device_v3/device_v3_lean/device_lean.parquet" ;;
    file)   [[ "$prof" == "full" ]] && echo "out/$R/file_v3/file_v3_full/file_full.parquet" \
                                 || echo "out/$R/file_v3/file_v3_lean/file_lean.parquet" ;;
    http)   [[ "$prof" == "full" ]] && echo "out/$R/http_v3/http_v3_full/http_full.parquet" \
                                 || echo "out/$R/http_v3/http_v3_lean/http_lean.parquet" ;;
    email|mail)
            [[ "$prof" == "full" ]] && echo "out/$R/email_v3/email_v3_full/email_full.parquet" \
                                 || echo "out/$R/email_v3/email_v3_lean/email_lean.parquet" ;;
    ldap)   echo "out/$R/ldap_v3_lean/ldap_asof_by_month.parquet" ;;
    daily_v1) echo "out/$R/features_v1/daily_user/daily_user.parquet" ;;
    daily_v2|daily) echo "out/$R/features_v2/daily_user/daily_user.parquet" ;;
    *)
      local hit
      hit="$(find "out/$R" -type f -name "*.parquet" -ipath "*$tok*" 2>/dev/null | head -n 1)"
      [[ -n "$hit" ]] && { echo "$hit"; return 0; }
      echo "ERR: unknown token '$tok'" >&2; return 2 ;;
  esac
}

# -------- quick schema/rows/range/pq_head using tokens --------
schema() {
  local p="$(_pq_for "$1")" || return $?
  duckdb -csv -header -c "CREATE OR REPLACE TABLE _t AS SELECT * FROM read_parquet('$p') LIMIT 0; SELECT name, type FROM pragma_table_info('_t');" \
    | tail -n +2 \
    | awk -F',' '{printf "%-40s %s\n", $1, $2}'
}

schema_json() { schema "$@"; }
rows() {
  local p="$(_pq_for "$1")" || return $?
  dq "SELECT COALESCE(file_row_number,0) AS approx_rows FROM parquet_metadata('$p') LIMIT 1;"
}
range() {
  local p="$(_pq_for "$1")" || return $?
  local q_day="WITH t AS (SELECT * FROM read_parquet('$p')) SELECT MIN(day) AS tmin, MAX(day) AS tmax, COUNT(DISTINCT lower(COALESCE(user_key,''))) AS users FROM t;"
  local q_ts="WITH t AS (SELECT * FROM read_parquet('$p')) SELECT MIN(timestamp) AS tmin, MAX(timestamp) AS tmax, COUNT(DISTINCT lower(COALESCE(user_key,''))) AS users FROM t;"
  dq "$q_day" 2>/dev/null || dq "$q_ts"
}
pq_head() {
  local p="$(_pq_for "$1")" || return $?
  local n="${2:-20}"
  dq "SELECT * FROM read_parquet('$p') LIMIT $n;"
}

# -------- daily convenience (v2 default) --------

daily_head() {
  local n="${1:-20}"
  local p="$(_pq_for daily)" || return $?
  echo "[daily] $p"
  dq "SELECT * FROM read_parquet('$p') LIMIT $n;"
}

# -------- UI / loop helpers --------
run_ui() {
  local port="${1:-8000}"
  PYTHONPATH=. python -m uvicorn src.interface.api_stub:app --reload --port "$port"
}

run_loop() {
  if [[ -z "$1" || -z "$2" ]]; then
    echo "usage: run_loop <START:YYYY-MM-DD> <END:YYYY-MM-DD> [--dry-run]" >&2
    return 2
  fi
  local start="$1"; shift
  local end="$1"; shift
  PYTHONPATH=. python -m src.run_loop --start "$start" --end "$end" "$@"
}

# -------- daily_user.parquet shape/key QC --------
qc_daily() {
  local R; R="$(rel)" || return 1
  local base_v2="out/$R/features_v2/daily_user"
  local p="$base_v2/daily_user.parquet"
  if [[ ! -f "$p" ]]; then
    echo "[qc_daily] missing v2 daily: $p" >&2
    return 2
  fi
  python3 - <<'PY'
import sys, duckdb, pathlib
base_v2 = pathlib.Path('out')/open('release.txt').read().strip()/'features_v2'/'daily_user'
p = base_v2/'daily_user.parquet'
con = duckdb.connect(database=':memory:')
con.execute(f"CREATE OR REPLACE TABLE t AS SELECT * FROM read_parquet('{p.as_posix()}')")
cols = [r[1] for r in con.execute("PRAGMA table_info('t')").fetchall()]
coltypes = {r[1]: r[2] for r in con.execute("PRAGMA table_info('t')").fetchall()}
fail = False
for req in ('user_key','day'):
    if req not in cols:
        print(f"[FAIL] missing required column: {req}"); fail = True
if 'day' in coltypes and 'TIMESTAMP' not in coltypes['day'].upper():
    print(f"[FAIL] day has type {coltypes['day']} (expected TIMESTAMP*)"); fail = True
rows = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
ndist = con.execute("SELECT COUNT(*) FROM (SELECT DISTINCT user_key, day FROM t)").fetchone()[0]
dups = rows - ndist
u, dmin, dmax = con.execute("SELECT COUNT(DISTINCT user_key), MIN(day), MAX(day) FROM t").fetchone()
print(f"[qc_daily] (v2) rows={rows} distinct_users={u} range=[{dmin}..{dmax}] dup_keys={dups}")
sys.exit(1 if fail else 0)
PY
}

qc_daily_v1() {
  local R; R="$(rel)" || return 1
  local base="out/$R/features_v1/daily_user"
  local p="$base/daily_user.parquet"
  if [[ ! -f "$p" ]]; then
    echo "[qc_daily_v1] missing: $p" >&2
    return 2
  fi
  python3 - <<'PY'
import sys, duckdb, pathlib
base = pathlib.Path('out')/open('release.txt').read().strip()/'features_v1'/'daily_user'
p = base/'daily_user.parquet'
con = duckdb.connect(database=':memory:')
con.execute(f"CREATE OR REPLACE TABLE t AS SELECT * FROM read_parquet('{p.as_posix()}')")
cols = [r[1] for r in con.execute("PRAGMA table_info('t')").fetchall()]
coltypes = {r[1]: r[2] for r in con.execute("PRAGMA table_info('t')").fetchall()}
fail = False
for req in ('user_key','day'):
    if req not in cols:
        print(f"[FAIL] missing required column: {req}")
        fail = True
if 'day' in coltypes and coltypes['day'].upper() != 'DATE':
    print(f"[FAIL] day has type {coltypes['day']} (expected DATE)")
    fail = True
rows = con.execute("SELECT COUNT(*) FROM t").fetchone()[0]
ndist = con.execute("SELECT COUNT(*) FROM (SELECT DISTINCT user_key, day FROM t)").fetchone()[0]
dups = rows - ndist
u, dmin, dmax = con.execute("SELECT COUNT(DISTINCT user_key), MIN(day), MAX(day) FROM t").fetchone()
print(f"[qc_daily_v1] rows={rows} distinct_users={u} range=[{dmin}..{dmax}] dup_keys={dups}")
if dups>0:
    print("[FAIL] duplicate (user_key,day) rows present → fix build_union() merge logic")
    fail = True
sys.exit(1 if fail else 0)
PY
}

# -------- inventory helpers --------
big_rel() { local R="${1:-$(rel)}"; du -sh "out/$R"/* 2>/dev/null | sort -h; }
big_parquets() {
  find out -type f -name '*.parquet' -exec du -k {} + 2>/dev/null \
    | sort -n | tail -40 | awk '{printf "%8.2f MiB  %s\n", $1/1024, $2}'
}

# -------- space cleanup --------
purge_old_releases() {
  local mode="${1:-check}"
  local keep="${2:-$(rel)}"
  [[ ! -d out ]] && { echo "[purge] no out/ directory"; return 0; }

  mapfile -t all < <(find out -maxdepth 1 -mindepth 1 -type d -name 'r*' | sort)
  mapfile -t cand < <(printf "%s\n" "${all[@]}" | grep -v "/$keep$" || true)

  if [[ ${#cand[@]} -eq 0 ]]; then
    echo "[purge] nothing to purge; only $keep present"
    return 0
  fi

  echo "[purge] candidates (keeping $keep):"
  local total_k=0
  for d in "${cand[@]}"; do
    local sz
    sz="$(du -sh "$d" 2>/dev/null | awk '{print $1}')"
    local k; k="$(du -sk "$d" 2>/dev/null | awk '{print $1}')"
    total_k=$(( total_k + k ))
    printf "  %8s  %s\n" "$sz" "$d"
  done
  printf "  ---------\n  %8s  %s\n" "$(awk "BEGIN{printf \"%.1fG\", $total_k/1024/1024}")" "(total)"

  if [[ "$mode" == "check" ]]; then
    echo "[purge] dry-run only. To delete: purge_old_releases doit ${keep}"
    return 0
  fi

  if [[ "$mode" == "doit" ]]; then
    echo "[purge] deleting..."
    rm -rf "${cand[@]}"
    echo "[purge] done."
    return 0
  fi

  echo "[purge] usage: purge_old_releases check|doit [KEEP_REL]"
  return 2
}

help_cheats() {
cat <<'EOF'
  schema <tok>        # print columns/types
  rows <tok>          # approximate row count
  range <tok>         # min/max timestamp + distinct users
  pq_head <tok> [N]   # first N rows (default 20)
Tokens:
  logon[:lean|:full]  device[:lean|:full]  file[:lean|:full]
  http[:lean|:full]   email[:lean|:full]   ldap   daily  daily_v1  daily_v2

UI / Loop:
  run_ui [PORT]       # start FastAPI heartbeat/UI (default 8000)
  run_loop S E [opts] # run loop from start S to end E (e.g., --dry-run)

Daily helpers:
  daily_head [N]         # preview daily_user.parquet (v2)
  qc_daily               # validate shape & keys of daily_user.parquet (v2)
  daily_check            # v2 daily sanity summary (rows/users/dups/range + columns)
  daily_check_v1 [verbose]  # legacy v1 checker (same format)

Inventory:
  big_rel [REL]       # sizes under out/<REL>
  big_parquets        # largest parquet files

Space cleanup:
  purge_old_releases check|doit [KEEP_REL]

Venv:
  venv_on [path]      # default .venv
  venv_off            # deactivate current venv

Usage:
  source scripts/dev_cheats.sh
  help_cheats
EOF
}