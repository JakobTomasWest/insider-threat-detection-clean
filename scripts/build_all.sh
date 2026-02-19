#!/usr/bin/env bash
set -euo pipefail

# Activate project venv if it exists so "python" in sub-scripts uses the right interpreter
if [ -f ".venv/bin/activate" ]; then
  # shellcheck disable=SC1091
  source ".venv/bin/activate"
fi

# Prefer the project venv's Python if it exists, otherwise fall back to system python3/python
if [ -x ".venv/bin/python" ]; then
  PYTHON=".venv/bin/python"
else
  PYTHON="$(command -v python3 || command -v python)"
fi

FORCE="${1:-0}"

echo "=== 1) make setup ==="
make setup

echo "=== 2) make build (ETL v3) ==="
HTTP_SENTINEL="out/r5.2/http_v3/http_v3_full/http_full.parquet"
LOGON_SENTINEL="out/r5.2/logon_v3/logon_v3_full/logon_full.parquet"

if [ "$FORCE" -eq 1 ] || [ ! -f "$HTTP_SENTINEL" ] || [ ! -f "$LOGON_SENTINEL" ]; then
  echo "  -> Forcing or missing ETL outputs, running make build..."
  make build
else
  echo "  -> ETL outputs already exist:"
  echo "     $HTTP_SENTINEL"
  echo "     $LOGON_SENTINEL"
  echo "     Skipping make build."
fi

echo "=== 3) make daily (features_v2) ==="
DAILY_SENTINEL="out/r5.2/features_v2/daily_user/daily_user.parquet"
if [ "$FORCE" -eq 1 ] || [ ! -f "$DAILY_SENTINEL" ]; then
  echo "  -> Forcing or missing $DAILY_SENTINEL, running make daily..."
  make daily
else
  echo "  -> Found $DAILY_SENTINEL, skipping make daily."
fi

echo "=== 4) S1 supervised detection model ==="
S1_MODEL="out/r5.2/ml/supervised_model_xgb.pkl"
if [ "$FORCE" -eq 1 ] || [ ! -f "$S1_MODEL" ]; then
  echo "  -> No supervised model found at $S1_MODEL, training..."
  $PYTHON -m src.ml.detect_s1.build_exfil_ranges
  $PYTHON -m src.ml.detect_s1.build_daily_labels
  $PYTHON -m src.ml.detect_s1.make_train_val_test
  $PYTHON -m src.ml.detect_s1.make_windows
  $PYTHON -m src.ml.detect_s1.preprocess
  $PYTHON -m src.ml.detect_s1.train_supervised
  $PYTHON -m src.ml.detect_s1.eval_supervised
else
  echo "  -> Found existing model at $S1_MODEL, skipping supervised training."
fi

echo "=== 5) S1 forecast model ==="
S1_FCAST_MODEL="out/r5.2/ml_forecast/supervised_model_xgb_forecast.pkl"
if [ "$FORCE" -eq 1 ] || [ ! -f "$S1_FCAST_MODEL" ]; then
  echo "  -> No forecast model found at $S1_FCAST_MODEL, training..."
  $PYTHON -m src.ml.forecast_s1.build_daily_labels_forecast
  $PYTHON -m src.ml.forecast_s1.make_windows_forecast
  $PYTHON -m src.ml.forecast_s1.preprocess_forecast
  $PYTHON -m src.ml.forecast_s1.train_supervised_forecast
  $PYTHON -m src.ml.forecast_s1.eval_supervised_forecast
else
  echo "  -> Found existing forecast model at $S1_FCAST_MODEL, skipping forecast training."
fi

echo "=== 6) Anomaly models ==="
ANOM_IFOREST="out/r5.2/anomaly/isolation_forest.pkl"
if [ "$FORCE" -eq 1 ] || [ ! -f "$ANOM_IFOREST" ]; then
  echo "  -> Training anomaly models..."
  $PYTHON -m src.anomaly.build_user_roles
  $PYTHON -m src.anomaly.build_user_org_structure
  $PYTHON -m src.anomaly.train_isolation_forest
else
  echo "  -> Found $ANOM_IFOREST, skipping anomaly model training."
fi

ANOM_WINS="out/r5.2/anomaly/window_scores.parquet"
if [ "$FORCE" -eq 1 ] || [ ! -f "$ANOM_WINS" ]; then
  $PYTHON -m src.anomaly.build_window_scores
else
  echo "  -> Found $ANOM_WINS, skipping window scoring."
fi

ANOM_ZSCORES="out/r5.2/anomaly/window_zscores.parquet"
if [ "$FORCE" -eq 1 ] || [ ! -f "$ANOM_ZSCORES" ]; then
  $PYTHON -m src.anomaly.compute_baselines
else
  echo "  -> Found $ANOM_ZSCORES, skipping baselines."
fi

$PYTHON -m src.anomaly.analyze_window_scores || true

echo "=== 7) Full run_loop simulation (r5.2, 2010-01-01 → 2011-12-31) ==="
$PYTHON -m src.run_loop --start 2010-01-01 --end 2011-12-31

echo "=== DONE: ETL + ML + forecast + anomaly + run_loop ==="
echo ""
echo "Next step:"
echo "  Run: make ui"
echo "  Then open: http://127.0.0.1:8000 in your browser to view the dashboard."
