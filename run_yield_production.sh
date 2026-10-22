#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-status}"
PROJECT_DIR="${PROJECT_DIR:-/opt/project}"
PYTHON_BIN="${PYTHON_BIN:-/root/.pyenv/versions/yield_estimate/bin/python}"
RUN_ID="${RUN_ID:-wapor-yield-all-fields-v1}"
RUN_DIR="${RUN_DIR:-${PROJECT_DIR}/yield_runs/${RUN_ID}}"
GPKG="${GPKG:-${PROJECT_DIR}/final2.gpkg}"
PSQL_COMMAND="${PSQL_COMMAND:-sudo -u postgres psql -d kenya_monitoring}"
WORKERS="${WORKERS:-2}"
CHUNK_SIZE="${CHUNK_SIZE:-20000}"
MINIMUM_CROP_CONFIDENCE="${MINIMUM_CROP_CONFIDENCE:-0.0}"
MINIMUM_NPP_COVERAGE="${MINIMUM_NPP_COVERAGE:-0.8}"
MINIMUM_NPP_SPATIAL_COVERAGE="${MINIMUM_NPP_SPATIAL_COVERAGE:-0.8}"
DASHBOARD_PYTHON="${DASHBOARD_PYTHON:-/opt/kenya-dashboard/venv/bin/python}"
DASHBOARD_SCRATCH="${DASHBOARD_SCRATCH:-/opt/kenya-dashboard/load-tmp}"

mkdir -p "${RUN_DIR}" "${PROJECT_DIR}/yield_runs/logs"

common=(--run-dir "${RUN_DIR}")
calculate_common=(
  --psql-command "${PSQL_COMMAND}"
  --minimum-crop-confidence "${MINIMUM_CROP_CONFIDENCE}"
  --minimum-npp-coverage "${MINIMUM_NPP_COVERAGE}"
  --minimum-npp-spatial-coverage "${MINIMUM_NPP_SPATIAL_COVERAGE}"
)

case "${ACTION}" in
  prepare)
    "${PYTHON_BIN}" -m yield_estimation.production prepare "${common[@]}" \
      --gpkg "${GPKG}" --psql-command "${PSQL_COMMAND}"
    ;;
  inventory)
    "${PYTHON_BIN}" -m yield_estimation.production inventory "${common[@]}"
    ;;
  clip)
    "${PYTHON_BIN}" -m yield_estimation.production clip "${common[@]}" --workers "${WORKERS}"
    ;;
  build-weights)
    "${PYTHON_BIN}" -m yield_estimation.production build-weights "${common[@]}" \
      --chunk-size "${CHUNK_SIZE}"
    ;;
  extract-npp)
    "${PYTHON_BIN}" -m yield_estimation.production extract-npp "${common[@]}" --workers "${WORKERS}"
    ;;
  calculate-season)
    if [[ $# -lt 2 ]]; then
      echo "Usage: $0 calculate-season SEASON_CODE" >&2
      exit 2
    fi
    "${PYTHON_BIN}" -m yield_estimation.production calculate-season "${common[@]}" \
      --season-code "$2" "${calculate_common[@]}"
    ;;
  calculate-all)
    "${PYTHON_BIN}" -m yield_estimation.production calculate-all "${common[@]}" \
      "${calculate_common[@]}"
    ;;
  validate)
    "${PYTHON_BIN}" -m yield_estimation.production validate "${common[@]}"
    ;;
  load-dashboard)
    sudo -u postgres env \
      DATABASE_URL='postgresql:///kenya_monitoring?host=/var/run/postgresql' \
      "${DASHBOARD_PYTHON}" "${PROJECT_DIR}/dashboard/scripts/load_yield.py" \
      --run-dir "${RUN_DIR}" --scratch-dir "${DASHBOARD_SCRATCH}"
    ;;
  status)
    "${PYTHON_BIN}" -m yield_estimation.production status "${common[@]}"
    ;;
  *)
    echo "Usage: $0 {prepare|inventory|clip|build-weights|extract-npp|calculate-season SEASON|calculate-all|validate|load-dashboard|status}" >&2
    exit 2
    ;;
esac
