SHELL := /bin/bash

.PHONY: setup build ldap logon device file http email all qc daily daily-v1 daily-v2 daily-check clean ui run-loop

# 0) Environment bootstrap (venv, deps, duckdb cli hint)
setup:
	@bash scripts/bootstrap.sh

# 1) ETL v3 (ldap → logon → device → file → http → email)
build:
	@bash scripts/rebuild.sh $(filter-out $@,$(MAKECMDGOALS))

# 2a) Build daily aggregates (features_v1)
daily-v1:
	@python3 scripts/build_daily.py

# 2b) Append v2 features from v1 daily_user.parquet → features_v2
daily-v2:
	@python3 scripts/build_features_v2.py

# 2c) Convenience: build v1 then v2 in order, then run quick qc
daily: daily-v1 daily-v2 qc

# 3) QC rollup (your existing script)
qc:
	@bash scripts/qc_checks.sh || true

# Legacy features_v1 sanity (optionally verbose)
# Usage: make daily-check            # summary
#        make daily-check VERBOSE=1  # includes column listings
daily-check:
	@if [ -n "$(VERBOSE)" ]; then \
		python3 scripts/daily_check.py verbose; \
	else \
		python3 scripts/daily_check.py; \
	fi

# Space check helper; sources cheats so function exists in same shell
clean:
	@bash -lc 'source scripts/dev_cheats.sh; purge_old_releases check'

# Start the UI server (src.ui.app)
ui:
	@PYTHONPATH=. python3 -m src.ui.app

# Convenience wrapper for the simulation loop.
# Usage:
#   make run-loop START=2010-12-01 END=2010-12-10
#   make run-loop START=2010-12-01 END=2010-12-03 DRY=1
run-loop:
	@START="$(START)"; END="$(END)"; DRY="$(DRY)"; \
	opts=""; \
	[ -n "$$START" ] && opts="$$opts --start $$START"; \
	[ -n "$$END" ] && opts="$$opts --end $$END"; \
	[ -n "$$DRY" ] && opts="$$opts --dry-run"; \
	PYTHONPATH=. python3 -m src.run_loop $$opts