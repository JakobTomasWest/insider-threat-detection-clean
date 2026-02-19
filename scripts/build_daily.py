


#!/usr/bin/env python3
"""
Build per-domain daily aggregates and a joined daily_user table (v1).

Inputs (per release, LEAN artifacts):
  out/<REL>/logon_v3/logon_v3_lean/logon_lean.parquet
  out/<REL>/device_v3/device_v3_lean/device_lean.parquet
  out/<REL>/file_v3/file_v3_lean/file_lean.parquet
  out/<REL>/http_v3/http_v3_lean/http_lean.parquet
  out/<REL>/email_v3/email_v3_lean/email_lean.parquet

Outputs:
  out/<REL>/features_v1/daily_user/logon_daily.parquet
  out/<REL>/features_v1/daily_user/device_daily.parquet
  out/<REL>/features_v1/daily_user/file_daily.parquet
  out/<REL>/features_v1/daily_user/http_daily.parquet
  out/<REL>/features_v1/daily_user/email_daily.parquet
  out/<REL>/features_v1/daily_user/daily_user.parquet  (union across domains)

Notes:
- Idempotent: running again overwrites outputs.
- Missing domains are skipped with a warning.
- All tables keyed by (user_key, day) where day = date_trunc('day', timestamp).
- Common fields added where possible:
    * is_active_employee_day: MAX(user_is_active_employee) for the day (bool-as-int 0/1)
    * n_events_post_departure: COUNT of events where user_is_active_employee = FALSE
"""

# --- path bootstrap so `from src...` works when run as a script ---
import sys as _sys
from pathlib import Path as _Path
_repo_root = _Path(__file__).resolve().parents[1]
if str(_repo_root) not in _sys.path:
    _sys.path.insert(0, str(_repo_root))
# -----------------------------------------------------------------
from notebooks.nb_paths import bootstrap
from pathlib import Path
import sys
import duckdb

# -----------------------------
# helpers
# -----------------------------
def _p(*parts) -> Path:
    return Path(*parts)

def _exists(p: Path) -> bool:
    return p.exists() and p.is_file()

def _mk_out_dir(rel: str) -> Path:
    d = _p("out", rel, "features_v1", "daily_user")
    d.mkdir(parents=True, exist_ok=True)
    return d

def _print_qc(con: duckdb.DuckDBPyConnection, table: str) -> None:
    try:
        rows = con.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        users, dmin, dmax = con.execute(
            f"SELECT COUNT(DISTINCT user_key), MIN(day), MAX(day) FROM {table}"
        ).fetchone()
        print(f"[ok] {table}: rows={rows} users={users} range=[{dmin}..{dmax}]")
    except Exception as e:
        print(f"[warn] QC failed for {table}: {e}")

def _copy(con: duckdb.DuckDBPyConnection, table: str, out_path: Path) -> None:
    con.execute(f"COPY {table} TO '{str(out_path)}' (FORMAT PARQUET)")
    print(f"[wrote] {out_path}")

def _bool_int(expr: str) -> str:
    """Cast a boolean-ish expression to INTEGER in DuckDB, safely."""
    return f"CAST(COALESCE({expr}, FALSE) AS INTEGER)"

# Helper to check if a column exists in a DuckDB view/table
def _has_column(con: duckdb.DuckDBPyConnection, view_name: str, col: str) -> bool:
    try:
        return bool(con.execute(
            f"SELECT COUNT(*) FROM pragma_table_info('{view_name}') WHERE name='{col}'"
        ).fetchone()[0])
    except Exception:
        return False

def _table_exists(con: duckdb.DuckDBPyConnection, name: str) -> bool:
    try:
        con.execute(f"SELECT 1 FROM {name} LIMIT 1")
        return True
    except Exception:
        return False

# TODO: add code to introduce features_v2 columns for daily_user enrichments and anomaly-aligned metrics

# -----------------------------
# domain builders
# -----------------------------
def build_logon(con: duckdb.DuckDBPyConnection, src: Path, out: Path):
    if not _exists(src):
        print(f"[skip] LOGON missing: {src}")
        return False
    con.execute(f"CREATE OR REPLACE VIEW logon AS SELECT * FROM read_parquet('{str(src)}')")
    con.execute(f"""
        CREATE OR REPLACE TABLE logon_daily AS
        SELECT
          lower(user_key)                                        AS user_key,
          date_trunc('day', timestamp)                           AS day,
          COUNT(*)                                               AS n_logon,
          COUNT(DISTINCT pc)                                     AS n_pc_used,
          AVG({_bool_int('after_hours')})                        AS after_hours_rate,
          AVG({_bool_int('on_shared_pc')})                       AS on_shared_pc_rate,
          AVG({_bool_int('on_unassigned_pc')})                   AS on_unassigned_pc_rate,
          MAX({_bool_int('user_is_active_employee')})            AS is_active_employee_day,
          SUM(CASE WHEN COALESCE(user_is_active_employee, TRUE) = FALSE THEN 1 ELSE 0 END)
                                                                AS n_events_post_departure
        FROM logon
        GROUP BY 1,2
        ORDER BY 1,2
    """)
    _print_qc(con, "logon_daily")
    _copy(con, "logon_daily", out / "logon_daily.parquet")
    return True

def build_device(con: duckdb.DuckDBPyConnection, src: Path, out: Path):
    if not _exists(src):
        print(f"[skip] DEVICE missing: {src}")
        return False
    con.execute(f"CREATE OR REPLACE VIEW device AS SELECT * FROM read_parquet('{str(src)}')")
    has_activity = _has_column(con, "device", "activity")
    activity_select = (
        "            lower(COALESCE(activity, '')) AS activity_lc,\n"
        if has_activity
        else
        "            '' AS activity_lc,\n"
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE device_daily AS
        WITH flags AS (
          SELECT
            lower(user_key) AS user_key,
            date_trunc('day', timestamp) AS day,
            pc,
{activity_select}            after_hours,
            on_shared_pc,
            user_is_active_employee
          FROM device
        )
        SELECT
          user_key,
          day,
          COUNT(*)                                         AS n_device_events,
          COUNT(DISTINCT pc)                               AS n_unique_pcs,
          SUM(CASE WHEN activity_lc = 'connect'    THEN 1 ELSE 0 END) AS n_usb_connects,
          SUM(CASE WHEN activity_lc = 'disconnect' THEN 1 ELSE 0 END) AS n_usb_disconnects,
          AVG({_bool_int('after_hours')})                  AS after_hours_rate,
          AVG({_bool_int('on_shared_pc')})                 AS usb_on_shared_pc_rate,
          MAX({_bool_int('user_is_active_employee')})      AS is_active_employee_day,
          SUM(CASE WHEN COALESCE(user_is_active_employee, TRUE) = FALSE THEN 1 ELSE 0 END)
                                                      AS n_events_post_departure
        FROM flags
        GROUP BY 1,2
        ORDER BY 1,2
    """)
    _print_qc(con, "device_daily")
    _copy(con, "device_daily", out / "device_daily.parquet")
    return True

def build_file(con: duckdb.DuckDBPyConnection, src: Path, out: Path):
    if not _exists(src):
        print(f"[skip] FILE missing: {src}")
        return False
    con.execute(f"CREATE OR REPLACE VIEW file AS SELECT * FROM read_parquet('{str(src)}')")
    has_activity = _has_column(con, "file", "activity")
    activity_select = (
        "            lower(COALESCE(activity, '')) AS activity_lc,\n"
        if has_activity
        else
        "            '' AS activity_lc,\n"
    )
    con.execute(f"""
        CREATE OR REPLACE TABLE file_daily AS
        WITH f AS (
          SELECT
            lower(user_key) AS user_key,
            date_trunc('day', timestamp) AS day,
            pc,
{activity_select}            to_removable_media,
            from_removable_media,
            is_keylogger,
            after_hours,
            user_is_active_employee
          FROM file
        )
        SELECT
          user_key,
          day,
          COUNT(*)                                         AS n_file_events,
          SUM(CASE WHEN activity_lc = 'file open'  THEN 1 ELSE 0 END) AS n_file_open,
          SUM(CASE WHEN activity_lc = 'file write' THEN 1 ELSE 0 END) AS n_file_write,
          SUM({_bool_int('to_removable_media')})           AS n_to_removable,
          SUM({_bool_int('from_removable_media')})         AS n_from_removable,
          SUM({_bool_int('is_keylogger')})                 AS n_is_keylogger,
          COUNT(DISTINCT pc)                               AS n_unique_pcs,
          AVG({_bool_int('after_hours')})                  AS after_hours_rate,
          MAX({_bool_int('user_is_active_employee')})      AS is_active_employee_day,
          SUM(CASE WHEN COALESCE(user_is_active_employee, TRUE) = FALSE THEN 1 ELSE 0 END)
                                                      AS n_events_post_departure
        FROM f
        GROUP BY 1,2
        ORDER BY 1,2
    """)
    _print_qc(con, "file_daily")
    _copy(con, "file_daily", out / "file_daily.parquet")
    return True

def build_http(con: duckdb.DuckDBPyConnection, src: Path, out: Path):
    if not _exists(src):
        print(f"[skip] HTTP missing: {src}")
        return False
    con.execute(f"CREATE OR REPLACE VIEW http AS SELECT * FROM read_parquet('{str(src)}')")
    con.execute(f"""
        CREATE OR REPLACE TABLE http_daily AS
        SELECT
          lower(user_key)                               AS user_key,
          date_trunc('day', timestamp)                  AS day,
          COUNT(*)                                      AS n_http,
          AVG(COALESCE(url_length, length(url)))        AS avg_url_length,
          AVG(COALESCE(url_depth, 0))                   AS avg_url_depth,
          SUM({_bool_int('is_wikileaks')})              AS n_wikileaks,
          SUM({_bool_int('is_dropbox')})                AS n_dropbox,
          SUM({_bool_int('is_job_site')})               AS n_job_sites,
          AVG({_bool_int('after_hours')})               AS after_hours_rate,
          MAX({_bool_int('user_is_active_employee')})   AS is_active_employee_day,
          SUM(CASE WHEN COALESCE(user_is_active_employee, TRUE) = FALSE THEN 1 ELSE 0 END)
                                                      AS n_events_post_departure
        FROM http
        GROUP BY 1,2
        ORDER BY 1,2
    """)
    _print_qc(con, "http_daily")
    _copy(con, "http_daily", out / "http_daily.parquet")
    return True

def build_email(con: duckdb.DuckDBPyConnection, src: Path, out: Path):
    if not _exists(src):
        print(f"[skip] EMAIL missing: {src}")
        return False
    con.execute(f"CREATE OR REPLACE VIEW email AS SELECT * FROM read_parquet('{str(src)}')")

    # Support both legacy booleans and the new v3.1 direction flags
    has_dir_internal_only        = _has_column(con, "email", "dir_internal_only")
    has_dir_internal_to_external = _has_column(con, "email", "dir_internal_to_external")
    has_dir_external_to_internal = _has_column(con, "email", "dir_external_to_internal")
    has_dir_external_only        = _has_column(con, "email", "dir_external_only")
    has_any_personal             = _has_column(con, "email", "any_personal_recipient")

    # Fallbacks to legacy fields if new ones are absent
    n_internal_only_expr = (
        f"SUM({_bool_int('dir_internal_only')})"
        if has_dir_internal_only else
        # Approximation under legacy: internal_to_external=0 AND external_to_internal=0 AND email_to_ext_domain=0
        "SUM(CASE WHEN COALESCE(internal_to_external, FALSE)=FALSE AND COALESCE(external_to_internal, FALSE)=FALSE AND COALESCE(email_to_ext_domain, FALSE)=FALSE THEN 1 ELSE 0 END)"
    )
    n_internal_to_external_expr = (
        f"SUM({_bool_int('dir_internal_to_external')})"
        if has_dir_internal_to_external else
        f"SUM({_bool_int('internal_to_external')})"
    )
    n_external_to_internal_expr = (
        f"SUM({_bool_int('dir_external_to_internal')})"
        if has_dir_external_to_internal else
        f"SUM({_bool_int('external_to_internal')})"
    )
    n_external_only_expr = (
        f"SUM({_bool_int('dir_external_only')})"
        if has_dir_external_only else
        # Legacy has no clean external-only; we approximate as emails where sender is external and no internal recipients → not available here, so default 0
        "0"
    )
    n_any_personal_expr = (
        f"SUM({_bool_int('any_personal_recipient')})"
        if has_any_personal else
        f"SUM({_bool_int('external_to_personal')})"
    )

    # Some leans may lack user_is_active_employee; guard it
    has_active = _has_column(con, "email", "user_is_active_employee")
    iae_expr = f"MAX({_bool_int('user_is_active_employee')})" if has_active else "0"
    ndepart_expr = (
        "SUM(CASE WHEN COALESCE(user_is_active_employee, TRUE) = FALSE THEN 1 ELSE 0 END)"
        if has_active else "0"
    )

    con.execute(f"""
        CREATE OR REPLACE TABLE email_daily AS
        WITH e AS (
          SELECT
            lower(user_key)                               AS user_key,
            date_trunc('day', timestamp)                  AS day,
            timestamp,
            COALESCE(attachment_count, 0)                 AS attachment_count,
            /* derive after-hours: before 07:00 or at/after 19:00 */
            (EXTRACT(hour FROM timestamp) < 7 OR EXTRACT(hour FROM timestamp) >= 19) AS is_after_hours,
            /* bring through directional flags as-is for daily aggregation */
            dir_internal_only,
            dir_internal_to_external,
            dir_external_to_internal,
            dir_external_only,
            any_personal_recipient,
            email_to_ext_domain,
            internal_to_external,
            external_to_internal,
            external_to_personal,
            /* user activity flag may be missing in lean; handle outside aggregation via COALESCE */
            user_is_active_employee
          FROM email
        )
        SELECT
          user_key,
          day,
          COUNT(*)                                      AS n_email_sent,
          AVG(attachment_count)                         AS avg_attachment_count,
          SUM({ _bool_int('dir_internal_only') })               AS n_internal_only,
          SUM({ _bool_int('dir_internal_to_external') })        AS n_internal_to_external,
          SUM({ _bool_int('dir_external_to_internal') })        AS n_external_to_internal,
          SUM({ _bool_int('dir_external_only') })               AS n_external_only,
          SUM({ _bool_int('any_personal_recipient') })          AS n_any_personal_recipient,
          /* after-hours metrics derived from timestamp */
          SUM({ _bool_int('is_after_hours') })                  AS n_after_hours,
          AVG({ _bool_int('is_after_hours') })                  AS after_hours_rate,
          /* active-employee day and post-departure counts when available */
          MAX({ _bool_int('user_is_active_employee') })         AS is_active_employee_day,
          SUM(CASE WHEN COALESCE(user_is_active_employee, TRUE) = FALSE THEN 1 ELSE 0 END)
                                                            AS n_events_post_departure
        FROM e
        GROUP BY 1,2
        ORDER BY 1,2
    """)
    _print_qc(con, "email_daily")
    _copy(con, "email_daily", out / "email_daily.parquet")
    return True

def build_union(con: duckdb.DuckDBPyConnection, out: Path) -> None:
    """
    Create features_v1/daily_user/daily_user.parquet by outer-joining all
    per-domain daily tables that exist. Always projects at least (user_key, day),
    and prefixes domain columns to avoid name collisions.

    Output schema (present columns depend on which per-domain tables exist):
      user_key, day,
      logon_*, device_*, file_*, http_*, email_*
    """
    present = []
    for dom in ["logon", "device", "file", "http", "email"]:
        if _table_exists(con, f"{dom}_daily"):
            present.append(dom)

    if not present:
        print("[skip] UNION: no per-domain daily tables found")
        return

    # Build a keyset of (user_key, day) by UNION of all present domain tables
    unions = " UNION ".join([f"SELECT LOWER(user_key) AS user_key, day FROM {d}_daily" for d in present])
    con.execute("CREATE OR REPLACE TEMP VIEW _keys AS " + unions)

    # Build SELECT list: always include keys, then each domain's columns with a domain_ prefix.
    select_parts = ["k.user_key AS user_key", "k.day AS day"]

    def _cols(view: str) -> list[str]:
        rows = con.execute(f"SELECT name FROM pragma_table_info('{view}') ORDER BY cid").fetchall()
        return [r[0] for r in rows]

    for dom in present:
        cols = _cols(f"{dom}_daily")
        for c in cols:
            if c in ("user_key", "day"):
                continue
            select_parts.append(f"{dom}_daily.{c} AS {dom}_{c}")

    select_sql = ",\n          ".join(select_parts)

    # LEFT JOIN each domain table onto the keyset. Since _keys is the union of all keys,
    # LEFT JOIN is sufficient and avoids FULL OUTER corner cases.
    from_sql = "FROM _keys k\n"
    for dom in present:
        from_sql += f"LEFT JOIN {dom}_daily ON {dom}_daily.user_key = k.user_key AND {dom}_daily.day = k.day\n"

    full_sql = f"""
        CREATE OR REPLACE TABLE daily_user AS
        SELECT
            {select_sql}
        {from_sql}
        ORDER BY 1,2
    """
    con.execute(full_sql)
    _print_qc(con, "daily_user")
    _copy(con, "daily_user", out / "daily_user.parquet")

# -----------------------------
# main
# -----------------------------
def main():
    env = bootstrap()
    rel = env.RELEASE
    out_dir = _mk_out_dir(rel)

    logon_src  = _p("out", rel, "logon_v3",  "logon_v3_lean",  "logon_lean.parquet")
    device_src = _p("out", rel, "device_v3", "device_v3_lean", "device_lean.parquet")
    file_src   = _p("out", rel, "file_v3",   "file_v3_lean",   "file_lean.parquet")
    http_src   = _p("out", rel, "http_v3",   "http_v3_lean",   "http_lean.parquet")
    email_src  = _p("out", rel, "email_v3",  "email_v3_lean",  "email_lean.parquet")

    con = duckdb.connect(database=":memory:")

    built_any = False
    built_any |= build_logon(con, logon_src, out_dir)
    built_any |= build_device(con, device_src, out_dir)
    built_any |= build_file(con,   file_src,   out_dir)
    built_any |= build_http(con,   http_src,   out_dir)
    built_any |= build_email(con,  email_src,  out_dir)

    if built_any:
        build_union(con, out_dir)
    else:
        print("[warn] nothing built; no source LEAN artifacts were found for this release.")

if __name__ == "__main__":
    sys.exit(main() or 0)