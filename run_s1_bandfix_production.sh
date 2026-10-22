#!/usr/bin/env bash
set -Eeuo pipefail

MODE="${1:-}"
if [[ -z "${MODE}" ]]; then
    echo "Usage: $0 {pilot-prepare|pilot-extract|pilot-validate|prepare|prepare-rebuild|extract|validate|database-prepare|database-load|database-finalize|database-validate|database-swap|status}"
    exit 2
fi

PROJECT_DIR="${PROJECT_DIR:-/opt/project}"
PYTHON_BIN="${PYTHON_BIN:-/root/.pyenv/versions/yield_estimate/bin/python}"
RUN_ID="${RUN_ID:-s1-bandfix-20260928-v1}"
RUN_ROOT="${RUN_ROOT:-/opt/project/zonal_runs}"
RUN_DIR="${RUN_ROOT}/${RUN_ID}"
PILOT_RUN_DIR="${RUN_ROOT}/${RUN_ID}-pilot"
GPKG_PATH="${GPKG_PATH:-/opt/project/final2.gpkg}"
S1_WEEKLY_DIR="${S1_WEEKLY_DIR:-/opt/project/kenya_s1_ops/final_target/weekly}"
S2_INPUT_DIR="${S2_INPUT_DIR:-/data/indices}"
PILOT_FIELD_IDS="${PILOT_FIELD_IDS:-/opt/project/s1_bandfix/pilot_field_ids.txt}"
BASE_RUN_DIR="${BASE_RUN_DIR:-/opt/project/zonal_runs/adaptive-center-20260926-v1}"
WORKERS="${WORKERS:-2}"
DB_WORKERS="${DB_WORKERS:-4}"
DB_TEMP_DIR="${DB_TEMP_DIR:-/data/zonal-db-load-s1-bandfix}"
DUCKDB_MEMORY_LIMIT="${DUCKDB_MEMORY_LIMIT:-6GB}"
DB_NAME="${DB_NAME:-kenya_monitoring}"
PSQL_COMMAND="${PSQL_COMMAND:-sudo -u postgres psql}"

export OMP_NUM_THREADS="${OMP_NUM_THREADS:-1}"
export OPENBLAS_NUM_THREADS="${OPENBLAS_NUM_THREADS:-1}"
export MKL_NUM_THREADS="${MKL_NUM_THREADS:-1}"

cd "${PROJECT_DIR}"

prepare_run() {
    local target="$1"
    shift
    if [[ -f "${target}/run_manifest.json" ]]; then
        echo "Run already prepared: ${target}"
        return
    fi
    "${PYTHON_BIN}" zonal_center_pipeline.py prepare \
        --run-dir "${target}" \
        --gpkg-path "${GPKG_PATH}" \
        --uid-field field_id \
        --machakos-layer machakos_fin \
        --nakuru-layer nakuru_fin \
        --s1-input-dir "${S1_WEEKLY_DIR}" \
        --s1-input-kind weekly-linear \
        --s2-input-dir "${S2_INPUT_DIR}" \
        --min-pixels 5 \
        --tile-size 1024 \
        "$@"
}

extract_run() {
    local target="$1"
    shift
    "${PYTHON_BIN}" zonal_center_pipeline.py extract \
        --run-dir "${target}" \
        --sensor s1 \
        --workers "${WORKERS}" \
        --batch-rows 500000 \
        --duckdb-memory-limit "${DUCKDB_MEMORY_LIMIT}" \
        "$@"
}

case "${MODE}" in
    pilot-prepare)
        prepare_run "${PILOT_RUN_DIR}" \
            --field-id-file "${PILOT_FIELD_IDS}" \
            --raster-start-date 2023-05-08 \
            --raster-end-date 2023-08-28
        ;;
    pilot-extract)
        extract_run "${PILOT_RUN_DIR}"
        ;;
    pilot-validate)
        "${PYTHON_BIN}" zonal_center_pipeline.py validate \
            --run-dir "${PILOT_RUN_DIR}" --sensor s1 --full
        ;;
    prepare)
        "${PYTHON_BIN}" s1_bandfix/prepare_from_validated_run.py \
            --base-run "${BASE_RUN_DIR}" \
            --run-dir "${RUN_DIR}" \
            --s1-weekly-dir "${S1_WEEKLY_DIR}"
        ;;
    prepare-rebuild)
        prepare_run "${RUN_DIR}"
        ;;
    extract)
        extract_run "${RUN_DIR}"
        ;;
    validate)
        "${PYTHON_BIN}" zonal_center_pipeline.py validate \
            --run-dir "${RUN_DIR}" --sensor s1 --full
        ;;
    database-prepare)
        "${PYTHON_BIN}" load_zonal_v2.py prepare \
            --run-dir "${RUN_DIR}" --database "${DB_NAME}" \
            --psql-command "${PSQL_COMMAND}" --sensor s1
        ;;
    database-load)
        "${PYTHON_BIN}" load_zonal_v2.py load \
            --run-dir "${RUN_DIR}" --database "${DB_NAME}" \
            --psql-command "${PSQL_COMMAND}" --sensor s1 \
            --workers "${DB_WORKERS}" --temp-dir "${DB_TEMP_DIR}"
        ;;
    database-finalize)
        "${PYTHON_BIN}" load_zonal_v2.py finalize \
            --run-dir "${RUN_DIR}" --database "${DB_NAME}" \
            --psql-command "${PSQL_COMMAND}" --sensor s1
        ;;
    database-validate)
        "${PYTHON_BIN}" load_zonal_v2.py validate \
            --run-dir "${RUN_DIR}" --database "${DB_NAME}" \
            --psql-command "${PSQL_COMMAND}" --sensor s1
        ;;
    database-swap)
        "${PYTHON_BIN}" load_zonal_v2.py swap \
            --run-dir "${RUN_DIR}" --database "${DB_NAME}" \
            --psql-command "${PSQL_COMMAND}" --sensor s1 \
            --legacy-suffix pre_s1_bandfix_20260928
        ;;
    status)
        echo "Run directory: ${RUN_DIR}"
        [[ -f "${RUN_DIR}/run_manifest.json" ]] && cat "${RUN_DIR}/run_manifest.json"
        find "${RUN_DIR}/parts/s1" -type f -name '*.parquet' 2>/dev/null | wc -l || true
        [[ -f "${RUN_DIR}/validation_report.json" ]] && cat "${RUN_DIR}/validation_report.json"
        [[ -f "${RUN_DIR}/database_validation_report.json" ]] && cat "${RUN_DIR}/database_validation_report.json"
        ;;
    *)
        echo "Unknown mode: ${MODE}"
        exit 2
        ;;
esac
