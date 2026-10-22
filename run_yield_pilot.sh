#!/usr/bin/env bash
set -euo pipefail

ACTION="${1:-status}"
PROJECT_DIR="${PROJECT_DIR:-/opt/project}"
PYTHON_BIN="${PYTHON_BIN:-/root/.pyenv/versions/yield_estimate/bin/python}"
RUN_ID="${RUN_ID:-wapor-yield-2023lr-pilot-v1}"
RUN_DIR="${RUN_DIR:-${PROJECT_DIR}/yield_runs/${RUN_ID}}"
SEASON_CODE="${SEASON_CODE:-2023_lr}"
FIELD_LIST="${FIELD_LIST:-${PROJECT_DIR}/field_id_list.txt}"
FIELD_GPKG="${FIELD_GPKG:-${PROJECT_DIR}/final2.gpkg}"
DATABASE_URL="${DATABASE_URL:-postgresql:///kenya_monitoring?host=/var/run/postgresql}"
MINIMUM_CROP_CONFIDENCE="${MINIMUM_CROP_CONFIDENCE:-0.0}"
MINIMUM_NPP_COVERAGE="${MINIMUM_NPP_COVERAGE:-0.8}"
MINIMUM_NPP_SPATIAL_COVERAGE="${MINIMUM_NPP_SPATIAL_COVERAGE:-0.8}"

mkdir -p "${RUN_DIR}" "${PROJECT_DIR}/yield_runs/logs"

pipeline() {
  "${PYTHON_BIN}" -m yield_estimation.pipeline "$@" \
    --run-dir "${RUN_DIR}" --season-code "${SEASON_CODE}"
}

case "${ACTION}" in
  prepare)
    "${PYTHON_BIN}" -m yield_estimation.pipeline prepare \
      --run-dir "${RUN_DIR}" --season-code "${SEASON_CODE}" \
      --field-list "${FIELD_LIST}" --gpkg "${FIELD_GPKG}" \
      --psql-command "sudo -u postgres psql -d kenya_monitoring"
    ;;
  inventory)
    pipeline inventory
    ;;
  clip)
    pipeline clip
    ;;
  extract-npp)
    pipeline extract-npp
    ;;
  calculate)
    pipeline calculate \
      --minimum-crop-confidence "${MINIMUM_CROP_CONFIDENCE}" \
      --minimum-npp-coverage "${MINIMUM_NPP_COVERAGE}" \
      --minimum-npp-spatial-coverage "${MINIMUM_NPP_SPATIAL_COVERAGE}"
    ;;
  validate)
    pipeline validate
    ;;
  status)
    pipeline status
    ;;
  *)
    echo "Usage: $0 {prepare|inventory|clip|extract-npp|calculate|validate|status}" >&2
    exit 2
    ;;
esac
