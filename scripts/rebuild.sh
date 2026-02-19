#!/usr/bin/env bash
set -euo pipefail

# Rebuild v3 artifacts + QC for selected domains.
# Order matters because downstream joins depend on upstream outputs.
# Usage:
#   scripts/rebuild.sh                 # same as "all"
#   scripts/rebuild.sh all             # ldap → logon → device → file → http → email
#   scripts/rebuild.sh ldap http       # just those domains (ldap first is safest)
#   scripts/rebuild.sh logon device    # etc.


ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

# Prefer the project virtualenv; fall back to system python3/python.
if [[ -x ".venv/bin/python" ]]; then
  PY=".venv/bin/python"
elif command -v python3 >/dev/null 2>&1; then
  PY="$(command -v python3)"
elif command -v python >/dev/null 2>&1; then
  PY="$(command -v python)"
else
  echo "❌ No suitable Python interpreter found (.venv/bin/python, python3, or python)." >&2
  exit 1
fi

RELEASE="$(cat release.txt)"
DATA_DIR="data/$RELEASE"


bold() { printf "\033[1m%s\033[0m\n" "$*"; }

# require N GB free on the current filesystem
need_space_gb() {
  local need="$1"
  local free
  free="$(df -g . | tail -1 | awk '{print $4}')"
  free="${free:-0}"
  if [[ "$free" -lt "$need" ]]; then
    echo "Not enough free space: have ${free} GB, need ${need} GB. Aborting." >&2
    exit 1
  fi
}

# Estimate email sizes using DuckDB CLI if available, else fall back to Python DuckDB API.
estimate_email_sizes() {
  local rel="$(cat release.txt)"
  local csv="data/$rel/email.csv"
  local tmp="out/$rel/tmp_email_est"
  mkdir -p "$tmp"
  rm -f "$tmp"/*.parquet

  # Prefer DuckDB CLI if present; otherwise fall back to Python duckdb.
  if command -v duckdb >/dev/null 2>&1; then
    # Write lean-ish and full-ish samplings to parquet. Log errors for inspection.
    local log="$tmp/estimator.log"
    : >"$log"
    duckdb -c "
      COPY (
        SELECT \"date\",\"id\",\"user\",\"pc\",\"activity\",\"from\",\"to\",\"cc\",\"bcc\",\"attachments\",\"size\"
        FROM read_csv_auto('$csv', SAMPLE_SIZE=200000, IGNORE_ERRORS=TRUE)
        USING SAMPLE 0.5 PERCENT
      ) TO '$tmp/email_leanish.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 512000);
    " >>"$log" 2>&1 || true
    duckdb -c "
      COPY (
        SELECT \"date\",\"id\",\"user\",\"pc\",\"activity\",\"from\",\"to\",\"cc\",\"bcc\",\"attachments\",\"size\",\"content\"
        FROM read_csv_auto('$csv', SAMPLE_SIZE=200000, IGNORE_ERRORS=TRUE)
        USING SAMPLE 0.5 PERCENT
      ) TO '$tmp/email_fullish.parquet'
        (FORMAT PARQUET, COMPRESSION ZSTD, ROW_GROUP_SIZE 512000);
    " >>"$log" 2>&1 || true
  else
    # Python fallback uses the duckdb module
    "${PY:-python}" - <<'PY' || true
import duckdb, pathlib
rel = open('release.txt').read().strip()
csv = f"data/{rel}/email.csv"
tmp = pathlib.Path(f"out/{rel}/tmp_email_est")
tmp.mkdir(parents=True, exist_ok=True)
con = duckdb.connect()
def copy(q, outp):
    try:
        con.execute(f"COPY ({q}) TO '{outp}' (FORMAT PARQUET, COMPRESSION 'ZSTD', ROW_GROUP_SIZE 512000);")
    except Exception:
        pass
lean_q = f"""
SELECT "date","id","user","pc","activity","from","to","cc","bcc","attachments","size"
FROM read_csv_auto('{csv}', SAMPLE_SIZE=200000, IGNORE_ERRORS=TRUE)
USING SAMPLE 0.5 PERCENT
"""
full_q = f"""
SELECT "date","id","user","pc","activity","from","to","cc","bcc","attachments","size","content"
FROM read_csv_auto('{csv}', SAMPLE_SIZE=200000, IGNORE_ERRORS=TRUE)
USING SAMPLE 0.5 PERCENT
"""
copy(lean_q, str(tmp / "email_leanish.parquet"))
copy(full_q, str(tmp / "email_fullish.parquet"))
PY
  fi

  # Size the temporary outputs (0 if missing)
  local slean=$(stat -f%z "$tmp/email_leanish.parquet" 2>/dev/null || echo 0)
  local sfull=$(stat -f%z "$tmp/email_fullish.parquet" 2>/dev/null || echo 0)

  # Scale from 0.5% sample: x200, then add +20% headroom ( * 12 / 10 ).
  local estlean=$(( slean * 200 * 12 / 10 ))
  local estfull=$(( sfull * 200 * 12 / 10 ))
  echo "$estlean $estfull"
}

need_file() {
  local path="$1"
  [[ -e "$path" ]] || { echo "Missing required input: $path" >&2; exit 1; }
}

check_inputs() {
  # Only validate required inputs for the domains being run.
  for d in "$@"; do
    case "$d" in
      ldap)  need_file "$DATA_DIR/LDAP" ;;
      logon) need_file "$DATA_DIR/logon.csv" ;;
      device) need_file "$DATA_DIR/device.csv" ;;
      file)  need_file "$DATA_DIR/file.csv" ;;
      http)  need_file "$DATA_DIR/http.csv" ;;
      email) need_file "$DATA_DIR/email.csv" ;;
    esac
  done
}

run_ldap() {
  bold "[1/6] LDAP (lean + full)"
  # Write lean + full so downstream domains have everything they might need
  $PY -m scripts.etl ldap --profile lean --family ldap_v3_lean --overwrite
  $PY -m scripts.etl ldap --profile full --family ldap_v3_full --overwrite
}

run_logon() {
  bold "[2/6] LOGON"
  # Emits logon_full/lean + helper parquets (shared_pcs, assigned_pc) + QC
  $PY -m scripts.etl logon --overwrite
}

run_device() {
  bold "[3/6] DEVICE"
  $PY -m scripts.etl device --overwrite
}

run_file() {
  bold "[4/6] FILE"
  $PY -m scripts.etl file --overwrite
}

run_http() {
  bold "[5/6] HTTP"
  # Streams http.csv → http_full/lean + writes http_final_meta.json
  $PY -m scripts.etl http --overwrite
}

run_email() {
  bold "[6/6] EMAIL"
  # direct modest guard; full build may include content
  export TMPDIR="$(pwd)/out/$RELEASE/tmp_pyarrow"; mkdir -p "$TMPDIR"
  need_space_gb 5
  # Build email final artifacts (full + lean) and optional edges; emitter writes QC JSON.
  $PY -m scripts.etl email --overwrite --ldap-family-for-join ldap_v3_lean

  # Surface the QC meta so humans don't go spelunking.
  local qc="out/$RELEASE/qc/email_final_meta.json"
  if [[ -f "$qc" ]]; then
    echo "QC meta: $qc"
  else
    echo "Warning: missing $qc (emitter should have written it)."
  fi
}

run_email_lean() {
  bold "[6/6] EMAIL (lean only)"
  export TMPDIR="$(pwd)/out/$RELEASE/tmp_pyarrow"; mkdir -p "$TMPDIR"

  # If a lean parquet already exists, skip unless FORCE=1
  if [[ -f "out/$RELEASE/email_v3/email_v3_lean/email_lean.parquet" && "${FORCE:-0}" != "1" ]]; then
    echo "Email lean already exists → out/$RELEASE/email_v3/email_v3_lean/email_lean.parquet"
    echo "Skip rebuild (set FORCE=1 to rebuild)."
    return
  fi

  # Estimate sizes using a 0.5% DuckDB sample and scale up (+50% safety).
  read estlean estfull < <(estimate_email_sizes)  # bytes
  # If estimator failed to produce sizes, emit a clear hint.
  if [[ "${estlean:-0}" -le 0 ]]; then
    echo "Estimator note: either DuckDB isn't installed (no 'duckdb' CLI found)"
    echo "or the sample parquet artifacts weren't created under out/$RELEASE/tmp_email_est."
    echo "Check: out/$RELEASE/tmp_email_est/estimator.log for CLI errors (if present)."
  fi
  # If estimator failed (0 bytes), fall back to a safe minimum
  local est_mib=$(( estlean / 1024 / 1024 ))
  local need=$(( ( (estlean*3) / 2 ) / 1024 / 1024 / 1024 ))  # 1.5x headroom
  if [[ "$estlean" -le 0 ]]; then
    est_mib=0
    need=2  # conservative floor
    echo "Preflight: estimator unavailable (≈ 0 MiB). Requiring ≥ ${need} GiB free."
  else
    (( need < 2 )) && need=2
    echo "Preflight: estimated lean size ≈ ${est_mib} MiB; requiring ≥ ${need} GiB free."
  fi
  need_space_gb "$need"

  # Clean stale temp so we don’t death-spiral on quota
  rm -rf "out/$RELEASE/tmp_email_est" || true
  rm -rf "$TMPDIR"/* || true

  CERT_EMAIL_BUILD="lean" $PY -m scripts.etl email --overwrite

  # Best-effort temp cleanup
  rm -rf "$TMPDIR" || true
}

run_email_full() {
  bold "[6/6] EMAIL (full)"
  export TMPDIR="$(pwd)/out/$RELEASE/tmp_pyarrow"; mkdir -p "$TMPDIR"

  # If a full parquet already exists, skip unless FORCE=1
  if [[ -f "out/$RELEASE/email_v3/email_v3_full/email_full.parquet" && "${FORCE:-0}" != "1" ]]; then
    echo "Email full already exists → out/$RELEASE/email_v3/email_v3_full/email_full.parquet"
    echo "Skip rebuild (set FORCE=1 to rebuild)."
    return
  fi

  # estimate and require ~2x the predicted full size (min 8 GB)
  read estlean estfull < <(estimate_email_sizes)
  local need=$(( (estfull/1024/1024/1024)*2 ))
  (( need < 8 )) && need=8
  need_space_gb "$need"
  CERT_EMAIL_BUILD="full" $PY -m scripts.etl email --overwrite
}

run_email_edges() {
  bold "[6/6] EMAIL (edges only)"
  export TMPDIR="$(pwd)/out/$RELEASE/tmp_pyarrow"; mkdir -p "$TMPDIR"
  need_space_gb 2
  CERT_EMAIL_BUILD="edges" $PY -m scripts.etl email --overwrite
}

main() {
  # Default: all domains in dependency order
  if [[ $# -eq 0 || "$1" == "all" ]]; then
    domains=(ldap logon device file http email)
  else
    # Keep the user’s selection but enforce safe ordering for anything listed
    want=("$@")
    ordered=(ldap logon device file http email email-lean email-full email-edges)
    domains=()
    for d in "${ordered[@]}"; do
      for w in "${want[@]}"; do
        [[ "$d" == "$w" ]] && domains+=("$d")
      done
    done
  fi

  echo "Release: $RELEASE"
  # global safety rails for all domains
  export CERT_PARQUET_RG="${CERT_PARQUET_RG:-512000}"
  export CERT_PARQUET_COMP="${CERT_PARQUET_COMP:-zstd}"
  export CERT_PARQUET_LEVEL="${CERT_PARQUET_LEVEL:-3}"
  export MALLOC_ARENA_MAX="${MALLOC_ARENA_MAX:-1}"
  export TMPDIR="$(pwd)/out/$RELEASE/tmp_pyarrow"
  mkdir -p "$TMPDIR"
  echo "Will rebuild: ${domains[*]}"
  [[ "${FORCE:-0}" == "1" ]] && echo "FORCE=1 → will overwrite existing email artifacts."
  echo

  check_inputs "${domains[@]}"

  # Run selected steps
  for d in "${domains[@]}"; do
    case "$d" in
      ldap)  run_ldap ;;
      logon) run_logon ;;
      device) run_device ;;
      file)  run_file ;;
      http)  run_http ;;
      email) run_email ;;
      email-lean)  run_email_lean ;;
      email-full)  run_email_full ;;
      email-edges) run_email_edges ;;
      *) echo "Unknown domain: $d" >&2; exit 1 ;;
    esac
    echo
  done

  bold "Done. Outputs under: out/$RELEASE/"
  echo "QC sidecars under:   out/$RELEASE/qc/"
}

main "$@"
