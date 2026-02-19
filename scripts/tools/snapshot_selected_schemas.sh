#!/usr/bin/env bash
set -euo pipefail

# ====== CONFIG ======
RELEASES=("r3.1" "r5.1")     # which releases to scan
EXCLUDE_DIRS=("LDAP")        # skip these domain dirs under data/<release>/
CSV_BASE="data"
PARQUET_BASE="out"
OUT_DIR="schemas_snapshot"
MASTER_TXT="$OUT_DIR/COLUMNS.txt"
TMP_DIR="$(mktemp -d 2>/dev/null || mktemp -d -t schemas_tmp)"
CSV_INDEX="$TMP_DIR/csv_index.tsv"   # columns staged as: <release>\t<basename>\t<path>\t<columns>
EXCLUDE_PATTERNS=("ldap" "LDAP")
# ====================

# Safety: never write into a script or any path under scripts/
case "$MASTER_TXT" in
  *scripts/*|*.sh)
    echo "Refusing to write to suspicious MASTER_TXT: $MASTER_TXT" >&2
    exit 1
    ;;
esac

# Ensure output dir exists and temp is cleaned up on exit
mkdir -p "$OUT_DIR"
trap 'rm -rf "$TMP_DIR" 2>/dev/null || true' EXIT
: > "$MASTER_TXT"

# Get a |-prune expression to skip EXCLUDE_DIRS in find
build_prune_expr() {
  local root="$1"
  local expr=""
  for d in "${EXCLUDE_DIRS[@]}"; do
    local p="$root/$d"
    if [[ -z "$expr" ]]; then
      expr="-path '$p' -prune"
    else
      expr="$expr -o -path '$p' -prune"
    fi
  done
  echo "$expr"
}

snapshot_parquet() {
  local f="$1"
  # normalize path so duckdb doesn't care where the script is run from
  local absf
  absf="$(realpath "$f" 2>/dev/null || python3 - "$f" <<'PY'
import os,sys
print(__import__("os").path.abspath(sys.argv[1]))
PY
)"

  # one-shot duckdb call: csv, no header, create view -> read schema (SELECT last), strip CSV quotes
  local cols
  cols="$(duckdb -csv -noheader -c "
    PRAGMA enable_progress_bar = false;
    CREATE OR REPLACE VIEW _p AS SELECT * FROM read_parquet('${absf}') LIMIT 0;
    SELECT string_agg(name, ', ' ORDER BY cid) FROM PRAGMA table_info('_p');
  " 2>/dev/null | sed 's/^\"//; s/\"$//' || true)"

  {
    echo "$f"
    if [[ -n "$cols" ]]; then
      echo "  ${cols}"
    else
      echo "  [schema unreadable]"
    fi
    echo
  } >> "$MASTER_TXT"
}

snapshot_csv() {
  local f="$1"
  local rel="$2"
  local cols="" duckdb_ok=true
  cols="$(duckdb -c "
    PRAGMA enable_progress_bar = false;
    SET output_format = csv;
    CREATE OR REPLACE VIEW _c AS
      SELECT * FROM read_csv_auto(
        '$f',
        SAMPLE_SIZE=16384,
        IGNORE_ERRORS=true,
        NULL_PADDING=true,
        MAX_LINE_SIZE=10000000
      ) LIMIT 0;
    SELECT string_agg(name, ', ' ORDER BY cid) AS columns FROM PRAGMA table_info('_c');
    DROP VIEW _c;
  " 2>/dev/null | tail -n +2 | sed 's/^ *//; s/ *$//' )" || duckdb_ok=false

  if ! $duckdb_ok || [[ -z "$cols" ]]; then
    cols="$(python3 - "$f" <<'PY'
import sys, csv
path = sys.argv[1]
with open(path, "rb") as fh:
    raw = fh.read(65536)
txt = raw.decode("utf-8", errors="replace").lstrip("\ufeff")
first = next((ln for ln in txt.splitlines() if ln.strip()), "")
def parse_with(d):
    class D(csv.Dialect):
        delimiter=d; quotechar='"'; escapechar=None
        doublequote=True; skipinitialspace=False
        lineterminator='\n'; quoting=csv.QUOTE_MINIMAL
    try:
        return next(csv.reader([first], D))
    except Exception:
        return []
row=[]
try:
    row = next(csv.reader([first], csv.Sniffer().sniff(txt)))
except Exception:
    for d in [",","|",";","\t","  "]:
        row = parse_with(d)
        if row: break
if not row:
    row = [first] if first else []
cols = [ (c or "").strip() for c in row if (c or "").strip() ]
print(", ".join(cols))
PY
)"
  fi

  printf "%s\t%s\t%s\t%s\n" "$rel" "$(basename "$f")" "$f" "$cols" >> "$CSV_INDEX"
}

# CSVs (by release, skipping excluded dirs)
for rel in "${RELEASES[@]}"; do
  csv_root="$CSV_BASE/$rel"
  [[ -d "$csv_root" ]] || continue
  PRUNE_EXPR=$(build_prune_expr "$csv_root")
  eval "
    find '$csv_root' \
      \( $PRUNE_EXPR \) -o \
      -type f -name '*.csv' -print0
  " | while IFS= read -r -d '' f; do
        snapshot_csv "$f" "$rel"
      done
done

{
  echo "# CSV columns by file (releases grouped)"
  echo "# Format: <release> <filename> - <comma-separated columns>"
  echo
} >> "$MASTER_TXT"

if [[ -s "$CSV_INDEX" ]]; then
  # Get unique basenames
  while IFS= read -r name; do
    # For each requested release, emit line if present
    for rel in "${RELEASES[@]}"; do
      cols="$(awk -F '\t' -v r="$rel" -v n="$name" '$1==r && $2==n {print $4}' "$CSV_INDEX")"
      if [[ -n "$cols" ]]; then
        echo "$rel $name - $cols" >> "$MASTER_TXT"
      fi
    done
    echo >> "$MASTER_TXT"
  done < <(cut -f2 "$CSV_INDEX" | sort -u)
fi

echo >> "$MASTER_TXT"
echo "# Parquet columns" >> "$MASTER_TXT"
echo >> "$MASTER_TXT"

# Parquets (by release)
for rel in "${RELEASES[@]}"; do
  parq_root="$PARQUET_BASE/$rel"
  [[ -d "$parq_root" ]] || continue
  while IFS= read -r -d '' f; do
    snapshot_parquet "$f"
  done < <(find "$parq_root" -type f -name '*.parquet' -print0)
done

echo "Wrote $MASTER_TXT"