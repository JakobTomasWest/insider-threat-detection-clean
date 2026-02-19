# Insider Threat Capstone

This repo contains notebooks and helpers to clean and join CERT-style datasets:
LDAP (monthly people snapshots), Logon, Device (USB), HTTP, Email, and File,
plus a per-user “windows” viewer.

## Repo layout
- `notebooks/`
  - `01_ldap_basics.ipynb` → builds `out/ldap_v1/*` lookups (per-month LDAP)
  - `02_logon_clean.ipynb` → `out/logon_v1/*` (clean + enriched + PC lookups)
  - `03_device_clean.ipynb` → `out/device_v1/*`
  - `04_http_clean.ipynb`   → `out/http_v1/*`
  - `05_email_clean.ipynb`  → `out/email_v1/*`
  - `06_file_clean.ipynb`   → `out/file_v1/*`
  - `create_user_windows.ipynb` → `out/windows_v1/*` (per-user timelines)
  - `nb_paths.py` → tiny helper for paths (`data/<release>`, `out/<tag>`)
- `release.txt` → which dataset release to use (e.g., `r1`, `r3`)
- `data/` → **not in git** (large raw inputs)
- `out/`  → **not in git** (derived artifacts)

## Quick start
1. Create/activate a virtualenv (Python 3.10+ recommended).
2. `pip install -r requirements.txt`
3. Put raw data under `data/<release>/` (e.g., `data/r1/LDAP/*.csv`, `data/r1/logon.csv`, etc.).
4. Set the release in `release.txt` (e.g., `r1` or `r3`).
5. Open `notebooks/01_ldap_basics.ipynb` and run top-to-bottom. Then `02_…`, etc.

## Notes
- We keep raw `data/` and derived `out/` out of git to avoid huge repos.
- All event tables join to LDAP on `(user_key, event_month)` (month start).
- Usernames are normalized to `user_key` (lowercase, no domain/email).


⸻

macOS Application Launcher

This repository includes a macOS application bundle that allows you to launch the UI dashboard without using the terminal.

Running the UI from your Desktop
1.Copy the app bundle to your Desktop:
	cp -R macos/InsiderThreatUI.app ~/Desktop/
2.Double-click InsiderThreatUI.app.
This will:
	•	activate .venv
	•	run make ui
	•	open the dashboard at http://127.0.0.1:8000

If .venv/ does not exist, create it:
python3 -m venv .venv
source .venv/bin/activate
make setup # installs all Python requirements into the venv
