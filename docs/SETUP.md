# Setup & Run Guide (Current Pipeline)

This repo now uses a **Makefile + scripts** pipeline to build all Parquet artifacts.  
The old notebooks are **view-only** and no longer the primary way to run ETL.

Use this guide as the single source of truth for:

- Setting up your Python environment  
- Pointing the repo at a CERT release  
- Building ETL v3 Parquets  
- Building daily features (v1 + v2)  
- Running quick QC checks  
- Running the detector loop and UI

---

## 0. Expected repo layout

```text
capstone_root/
├─ docs/                  # design docs, detector guides, UI handshake
├─ notebooks/             # legacy / view-only ETL notebooks
│  └─ ml/                 # notebook comparing detector and forecast models (s1)
├─ src/                   # ETL, feature, detector, and interface code
├─ scripts/               # shell + Python helpers (bootstrap, rebuild, qc, etc.)
├─ data/                  # raw CERT CSVs (NOT committed)
│  └─ <release>/...       # e.g., r5.2/logon.csv, device.csv, ...
├─ out/                   # all derived Parquet + QC outputs (NOT committed)
│  └─ <release>/<domain>_v3
├─ answers/               # answer keys / labels (local-only; gitignored)
├─ release.txt            # current release name, e.g. r5.2
├─ requirements.txt
└─ Makefile
```

---

## 1. One-time environment bootstrap

The project uses a local virtual environment under `.venv` and expects Python 3.11 by default.

From the repo root:

```bash
make setup
```

This will:

- Create or refresh `.venv` using `${PYTHON_BIN:-python3.11}`  
- Install all Python dependencies from `requirements.txt`  
- Make key helper scripts executable (`scripts/dev_cheats.sh`, `scripts/rebuild.sh`, etc.)  
- Try to infer or validate `release.txt` based on what exists under `data/`  
- Check that `data/<release>/*.csv` exists for the chosen release

You only need to re-run `make setup` when:

- `requirements.txt` changes, **or**  
- `.venv` breaks (for example, after a Python / OS upgrade)

Do **not** create a separate `.venv` manually; let `make setup` own it.

---

## 2. Activating the environment in a new shell

Every time you open a new terminal, you need to:

1. Load the helper shortcuts  
2. Activate the project virtual environment

From the repo root:

```bash
source scripts/dev_cheats.sh
venv_on
```

This will:

- Activate `.venv` so `python3` and `pip` point at the project env  
- Register helper commands such as `schema`, `range`, `run_loop`, `run_ui`, `daily_head`, `qc_daily`, etc.  

You can sanity-check:

```bash
python3 -V
which python3
help_cheats
```

---

## 3. Pointing to a dataset (release)

Raw CERT CSVs live under `data/<release>/`:

```text
data/
└─ r5.2/
   ├─ logon.csv
   ├─ device.csv
   ├─ file.csv
   ├─ http.csv
   ├─ email.csv
   └─ ldap.csv
```

To choose a release, create or update `release.txt`:

```bash
echo r5.2 > release.txt
```

The ETL and feature scripts read `release.txt` and write outputs under:

```text
out/<release>/...
```

For example, with `release.txt` set to `r5.2`, all outputs go under `out/r5.2/`.

---

## 4. Building ETL v3 Parquets

The ETL v3 pipeline reads `data/<release>/*.csv` and produces per-domain Parquets plus QC sidecars.

### 4.1 Build all domains

With the venv active:

```bash
make build
```

This runs `scripts/rebuild.sh`, which in turn calls the domain ETL (`etl.py` → `emit.py`) for:

- `ldap`
- `logon`
- `device`
- `file`
- `http`
- `email`

Each run expects CSVs under `data/<release>/` and writes outputs to `out/<release>/...`.

### 4.2 Output layout

For most domains, the pattern is:

```text
out/<release>/
  ├─ <domain>_v3/
  │  ├─ <domain>_v3_full/
  │  │  └─ <domain>_full.parquet
  │  └─ <domain>_v3_lean/
  │     └─ <domain>_lean.parquet
  └─ qc/
     └─ <domain>_v3_*.json   # schema / row-count / range summaries
```

Examples (for release `r5.2`):

```text
out/r5.2/logon_v3/logon_v3_full/logon_full.parquet
out/r5.2/logon_v3/logon_v3_lean/logon_lean.parquet

out/r5.2/device_v3/device_v3_full/device_full.parquet
out/r5.2/device_v3/device_v3_lean/device_lean.parquet

out/r5.2/http_v3/http_v3_full/http_full.parquet
out/r5.2/http_v3/http_v3_lean/http_lean.parquet

out/r5.2/email_v3/email_v3_full/email_full.parquet
out/r5.2/email_v3/email_v3_lean/email_lean.parquet
```

LDAP is slightly different:

```text
out/r5.2/ldap_v3_full/ldap_asof_by_month.parquet
out/r5.2/ldap_v3_lean/ldap_asof_by_month.parquet
out/r5.2/ldap_v3_full/ldap_full_meta.json
out/r5.2/ldap_v3_lean/ldap_lean_meta.json
```

All domains also emit QC JSON files under:

```text
out/<release>/qc/
```

These describe row counts, column names, and basic sanity checks for each artifact.

### 4.3 Inspecting ETL outputs

With `dev_cheats` loaded, you can quickly inspect any Parquet via tokens:

```bash
schema logon:lean       # columns + types for logon_lean.parquet
range device:full       # timerange + distinct users for device_full.parquet
pq_head http:lean 10    # first 10 rows of http_lean.parquet
rows email:lean         # approximate row count for email_lean.parquet
```

For more details, run:

```bash
help_cheats
```

---

## 5. Building daily features (v1 and v2)

Once ETL v3 has run, you can build per-user-per-day aggregates.

### 5.1 features_v1 (daily_v1)

```bash
make daily-v1
```

This runs `scripts/build_daily.py` and writes:

```text
out/<release>/features_v1/daily_user/daily_user.parquet
```

This table is keyed by `(user_key, day)` and is the base input for v2 features.

### 5.2 features_v2 (daily_v2)

```bash
make daily-v2
```

This runs `scripts/build_features_v2.py` and writes:

```text
out/<release>/features_v2/daily_user/daily_user.parquet
```

`features_v2` keeps the same `(user_key, day)` keys as v1 and adds:

- After-hours rates and baselines  
- USB counts and baselines  
- Novelty flags  
- Other derived features consumed by detectors

See `docs/features_v2.md` for exact columns and window definitions.

### 5.3 Convenience target: build v1 + v2 + QC

```bash
make daily
```

This is equivalent to:

```bash
make daily-v1
make daily-v2
make qc
```

---

## 6. Quick QC and inspection

There are two layers of QC:

1. **Batch QC script**

   ```bash
   make qc
   ```

   Runs `scripts/qc_checks.sh` to summarize ETL and feature artifacts under `out/<release>/`.

2. **Daily-user-specific helpers**

   With `dev_cheats` loaded:

   ```bash
   daily_check         # summary of features_v2/daily_user (rows, users, dups, range, columns)
   qc_daily            # strict v2 daily shape/type checks
   qc_daily_v1         # legacy v1 daily checks
   daily_head 20       # peek at the first 20 rows of v2 daily_user
   ```

These helpers are the fastest way to confirm that:

- `(user_key, day)` keys are unique  
- Day ranges look reasonable  
- Expected columns are present  

---

## 7. Running the detector loop and UI

Once ETL and features are built, you can run the simulated daily loop and the web UI.

### 7.1 Detector loop

From the repo root, with venv active:

```bash
# Makefile wrapper
make run-loop START=2010-12-01 END=2010-12-10

# Or using cheats directly
source scripts/dev_cheats.sh
venv_on
run_loop 2010-12-01 2010-12-10
```

This:

- Reads `features_v2/daily_user.parquet` for the chosen release  
- Builds rolling 14-day windows per user  
- Calls all configured detectors (rules, anomaly, ml)  
- Appends alerts to:

```text
out/mvp0/alerts_ndjson/alerts.ndjson
```

### 7.2 Web UI

To start the FastAPI heartbeat UI:

```bash
# Makefile wrapper
make ui PORT=8000

# Or via cheats
source scripts/dev_cheats.sh
venv_on
run_ui 8000
```

Then open:

- `http://localhost:8000`

You’ll see:

- A heartbeat chart of alerts per day per detector  
- A table of the most recent alerts from `alerts.ndjson`  

For more detail on the UI contract and API endpoints, see `docs/ui_handshake.md`.

---

## 8. Detector and feature details

For detector developers:

- `docs/detector_dev_guide.md` describes:
  - The input window shape from `features_v2/daily_user`  
  - The contract for detector modules (`check(window_df, day, user_key)`)  
  - How to structure `evidence` fields in alerts  

For feature definitions:

- `docs/features_v2.md` explains:
  - The 14-day detector window  
  - Trend and baseline definitions  
  - All added columns and how they are computed  

These docs are the reference when adding or modifying detectors that consume `features_v2`.

---

## 9. Legacy notebooks (view-only)

The notebooks under `notebooks/` are now **legacy**:

- They mirror the ETL logic at a higher level  
- They are useful for exploration, debugging, and teaching  
- They are **not** the pipeline of record

All official builds should go through:

- `make setup`  
- `make build`  
- `make daily`  
- `make run-loop` / `make ui`

If notebook behavior and script behavior ever diverge, the **scripts + Makefile** version is authoritative.

---